package tests

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	storageApi "k8s.io/api/storage/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/backup/portworx"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	v1 "k8s.io/api/core/v1"

	semver "github.com/blang/semver"
)

// BasicSelectiveRestore selects random backed-up apps and restores them
var _ = Describe("{BasicSelectiveRestore}", func() {
	var (
		backupName        string
		contexts          []*scheduler.Context
		appContexts       []*scheduler.Context
		bkpNamespaces     []string
		clusterUid        string
		clusterStatus     api.ClusterInfo_StatusInfo_Status
		restoreName       string
		cloudCredName     string
		cloudCredUID      string
		backupLocationUID string
		bkpLocationName   string
		numDeployments    int
		providers         []string
		backupLocationMap map[string]string
		labelSelectors    map[string]string
	)
	JustBeforeEach(func() {
		backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
		bkpNamespaces = make([]string, 0)
		restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
		backupLocationMap = make(map[string]string)
		labelSelectors = make(map[string]string)

		numDeployments = 6 // For this test case to have relevance, it is necessary to raise the number of deployments.
		providers = getProviders()

		StartTorpedoTest("BasicSelectiveRestore", "All namespace backup and restore selective namespaces", nil, 83717)
		log.InfoD(fmt.Sprintf("App list %v", Inst().AppList))
		contexts = make([]*scheduler.Context, 0)
		log.InfoD("Starting to deploy applications")
		for i := 0; i < numDeployments; i++ {
			log.InfoD(fmt.Sprintf("Iteration %v of deploying applications", i))
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})
	It("Selective Restore From a Basic Backup", func() {

		Step("Validating deployed applications", func() {
			log.InfoD("Validating deployed applications")
			ValidateApplications(contexts)
		})
		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-bl", provider, getGlobalBucketName(provider))
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})
		Step("Registering cluster for backup", func() {
			log.InfoD("Registering cluster for backup")
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
		Step("Taking backup of multiple namespaces", func() {
			log.InfoD(fmt.Sprintf("Taking backup of multiple namespaces [%v]", bkpNamespaces))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateBackup(backupName, SourceClusterName, bkpLocationName, backupLocationUID, bkpNamespaces,
				labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying [%s] backup creation", backupName))
		})
		Step("Selecting random backed-up apps and restoring them", func() {
			log.InfoD("Selecting random backed-up apps and restoring them")
			selectedBkpNamespaces, err := GetSubsetOfSlice(bkpNamespaces, len(bkpNamespaces)/2)
			log.FailOnError(err, "Getting a subset of backed-up namespaces")
			selectedBkpNamespaceMapping := make(map[string]string)
			for _, namespace := range selectedBkpNamespaces {
				selectedBkpNamespaceMapping[namespace] = namespace
			}
			log.InfoD("Selected application namespaces to restore: [%v]", selectedBkpNamespaces)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateRestore(restoreName, backupName, selectedBkpNamespaceMapping, destinationClusterName, orgID, ctx, make(map[string]string))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed applications")
		ValidateAndDestroy(contexts, opts)
		backupDriver := Inst().Backup
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
		log.FailOnError(err, "Failed while trying to get backup UID for - [%s]", backupName)
		log.InfoD("Deleting backup")
		_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting backup [%s]", backupName))
		log.InfoD("Deleting restore")
		log.InfoD(fmt.Sprintf("Backup name [%s]", restoreName))
		err = DeleteRestore(restoreName, orgID, ctx)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// This test does custom resource backup and restore.
var _ = Describe("{CustomResourceBackupAndRestore}", func() {
	namespaceMapping := make(map[string]string)
	var contexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	var appContexts []*scheduler.Context
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
	var backupNames []string
	var restoreNames []string
	bkpNamespaces = make([]string, 0)

	JustBeforeEach(func() {
		StartTorpedoTest("CustomResourceBackupAndRestore", "Create custom resource backup and restore", nil, 58043)
		log.InfoD("Deploy applications")

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})
	It("Create custom resource backup and restore", func() {
		Step("Validate applications", func() {
			ValidateApplications(contexts)
		})

		Step("Creating cloud credentials", func() {
			log.InfoD("Creating cloud credentials")
			providers := getProviders()
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				cloudCredUID = uuid.New()
				CloudCredUIDMap[cloudCredUID] = cloudCredName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
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

		Step("Creating backup location", func() {
			log.InfoD("Creating backup location")
			providers := getProviders()
			for _, provider := range providers {
				backupLocation = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocation
				err := CreateBackupLocation(provider, backupLocation, backupLocationUID, cloudCredName, cloudCredUID,
					getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
			}
		})

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				backupNames = append(backupNames, backupName)
				err = CreateBackupWithCustomResourceType(backupName, SourceClusterName, backupLocation, backupLocationUID, []string{namespace}, nil, orgID, clusterUid, "", "", "", "", []string{"PersistentVolumeClaim"}, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup %s creation with custom resources", backupName))
			}
		})

		Step("Restoring the backed up application", func() {
			log.InfoD("Restoring the backed up application")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				restoreName = fmt.Sprintf("%s-%s-%v", restoreNamePrefix, backupName, time.Now().Unix())
				restoreNames = append(restoreNames, restoreName)
				restoredNameSpace := fmt.Sprintf("%s-%s", namespace, "restored")
				namespaceMapping[namespace] = restoredNameSpace
				err = CreateRestore(restoreName, backupName, namespaceMapping, SourceClusterName, orgID, ctx, make(map[string]string))
				log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)
			}
		})

		Step("Compare PVCs on both namespaces", func() {
			log.InfoD("Compare PVCs on both namespaces")
			for _, namespace := range bkpNamespaces {
				pvcs, _ := core.Instance().GetPersistentVolumeClaims(namespace, labelSelectors)
				restoreNamespace := fmt.Sprintf("%s-%s", namespace, "restored")
				restoredPvcs, _ := core.Instance().GetPersistentVolumeClaims(restoreNamespace, labelSelectors)
				dash.VerifyFatal(len(pvcs.Items), len(restoredPvcs.Items), "Compare number of PVCs")
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)

		for _, restore := range restoreNames {
			err := DeleteRestore(restore, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restore))
		}
		for _, backupName := range backupNames {
			backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Getting backup UID for backup %s", backupName))
			_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting backup - %s", backupName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// DeleteAllBackupObjects deletes all backed up objects
var _ = Describe("{DeleteAllBackupObjects}", func() {
	var (
		appList           = Inst().AppList
		backupName        string
		contexts          []*scheduler.Context
		preRuleNameList   []string
		postRuleNameList  []string
		appContexts       []*scheduler.Context
		bkpNamespaces     []string
		clusterUid        string
		clusterStatus     api.ClusterInfo_StatusInfo_Status
		restoreName       string
		cloudCredName     string
		cloudCredUID      string
		backupLocationUID string
		bkpLocationName   string
		preRuleName       string
		postRuleName      string
		preRuleUid        string
		postRuleUid       string
	)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	var namespaceMapping map[string]string
	namespaceMapping = make(map[string]string)
	intervalName := fmt.Sprintf("%s-%v", "interval", time.Now().Unix())
	JustBeforeEach(func() {
		StartTorpedoTest("DeleteAllBackupObjects", "Create the backup Objects and Delete", nil, 58088)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the AppParameters or not ")
		for i := 0; i < len(appList); i++ {
			if Contains(postRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["post"]; ok {
					dash.VerifyFatal(ok, true, "Post Rule details mentioned for the apps")
				}
			}
			if Contains(preRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["pre"]; ok {
					dash.VerifyFatal(ok, true, "Pre Rule details mentioned for the apps")
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
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})
	It("Create backup objects and delete", func() {
		providers := getProviders()

		Step("Validate applications", func() {
			ValidateApplications(contexts)
		})
		Step("Creating rules for backup", func() {
			log.InfoD("Creating pre rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "pre")
				log.FailOnError(err, "Creating pre rule %s for deployed apps failed", ruleName)
				dash.VerifyFatal(preRuleStatus, true, "Verifying pre rule for backup")

				if ruleName != "" {
					preRuleNameList = append(preRuleNameList, ruleName)
				}
			}
			log.InfoD("Creating post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "post")
				log.FailOnError(err, "Creating post %s rule for deployed apps failed", ruleName)
				dash.VerifyFatal(postRuleStatus, true, "Verifying Post rule for backup")
				if ruleName != "" {
					postRuleNameList = append(postRuleNameList, ruleName)
				}
			}
		})
		Step("Creating cloud account and backup location", func() {
			log.InfoD("Creating cloud account and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-%v", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				log.FailOnError(err, "Creating backup location %s failed", bkpLocationName)
			}
		})
		Step("Creating backup schedule policy", func() {
			log.InfoD("Creating a backup schedule policy")
			intervalSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 2)
			intervalPolicyStatus := Inst().Backup.BackupSchedulePolicy(intervalName, uuid.New(), orgID, intervalSchedulePolicyInfo)
			dash.VerifyFatal(intervalPolicyStatus, nil, fmt.Sprintf("Creating interval schedule policy %s", intervalName))
		})
		Step("Register cluster for backup", func() {
			log.InfoD("Register cluster for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			log.FailOnError(err, "Creation of source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			if len(preRuleNameList) > 0 {
				preRuleUid, err = Inst().Backup.GetRuleUid(orgID, ctx, preRuleNameList[0])
				log.FailOnError(err, "Failed to get UID for rule %s", preRuleNameList[0])
				preRuleName = preRuleNameList[0]
			} else {
				preRuleUid = ""
				preRuleName = ""
			}
			if len(postRuleNameList) > 0 {
				postRuleUid, err = Inst().Backup.GetRuleUid(orgID, ctx, postRuleNameList[0])
				log.FailOnError(err, "Failed to get UID for rule %s", postRuleNameList[0])
				postRuleName = postRuleNameList[0]
			} else {
				postRuleUid = ""
				postRuleName = ""
			}
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				err = CreateBackup(backupName, SourceClusterName, bkpLocationName, backupLocationUID, []string{namespace},
					labelSelectors, orgID, clusterUid, preRuleName, preRuleUid, postRuleName, postRuleUid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying %s backup creation", backupName))
			}
		})
		Step("Restoring the backed up applications", func() {
			log.InfoD("Restoring the backed up applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-%v", "test-restore", time.Now().Unix())
			err = CreateRestore(restoreName, backupName, namespaceMapping, destinationClusterName, orgID, ctx, make(map[string]string))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying %s backup's restore %s creation", backupName, restoreName))
		})

		Step("Delete the restores", func() {
			log.InfoD("Delete the restores")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restore %s deletion", restoreName))
		})
		Step("Delete the backups", func() {
			log.Infof("Delete the backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
			_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup %s deletion", backupName))

		})
		Step("Delete backup schedule policy", func() {
			log.InfoD("Delete backup schedule policy")
			policyList := []string{intervalName}
			err := Inst().Backup.DeleteBackupSchedulePolicy(orgID, policyList)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", policyList))
		})
		Step("Delete the pre and post rules", func() {
			log.InfoD("Delete the pre rule")
			if len(preRuleNameList) > 0 {
				for _, ruleName := range preRuleNameList {
					err := Inst().Backup.DeleteRuleForBackup(orgID, ruleName)
					dash.VerifySafely(err, nil, fmt.Sprintf("Deleting  backup pre rules %s", ruleName))
				}
			}
			log.InfoD("Delete the post rules")
			if len(postRuleNameList) > 0 {
				for _, ruleName := range postRuleNameList {
					err := Inst().Backup.DeleteRuleForBackup(orgID, ruleName)
					dash.VerifySafely(err, nil, fmt.Sprintf("Deleting  backup post rules %s", ruleName))
				}
			}
		})
		Step("Delete the backup location and cloud account", func() {
			log.InfoD("Delete the backup location %s and cloud account %s", bkpLocationName, cloudCredName)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.Infof(" Deleting deployed applications")
		ValidateAndDestroy(contexts, opts)
	})
})

// This testcase verifies schedule backup creation with a single namespace.
var _ = Describe("{ScheduleBackupCreationSingleNS}", func() {
	var (
		contexts           []*scheduler.Context
		appContexts        []*scheduler.Context
		backupLocationName string
		backupLocationUID  string
		cloudCredUID       string
		bkpNamespaces      []string
		scheduleNames      []string
		cloudAccountName   string
		backupName         string
		schBackupName      string
		schPolicyUid       string
		restoreName        string
		clusterStatus      api.ClusterInfo_StatusInfo_Status
	)
	var testrailID = 58014 // testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58014
	namespaceMapping := make(map[string]string)
	labelSelectors := make(map[string]string)
	cloudCredUIDMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	timeStamp := strconv.Itoa(int(time.Now().Unix()))
	periodicPolicyName := fmt.Sprintf("%s-%s", "periodic", timeStamp)

	JustBeforeEach(func() {
		StartTorpedoTest("ScheduleBackupCreationSingleNS", "Create schedule backup creation with a single namespace", nil, testrailID)
		log.Infof("Application installation")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})

	It("Schedule Backup Creation with single namespace", func() {
		Step("Validate deployed applications", func() {
			ValidateApplications(contexts)
		})
		providers := getProviders()
		Step("Adding Cloud Account", func() {
			log.InfoD("Adding cloud account")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudAccountName = fmt.Sprintf("%s-%v", provider, timeStamp)
				cloudCredUID = uuid.New()
				cloudCredUIDMap[cloudCredUID] = cloudAccountName
				err := CreateCloudCredential(provider, cloudAccountName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudAccountName, orgID, provider))
			}
		})

		Step("Adding Backup Location", func() {
			log.InfoD("Adding Backup Location")
			for _, provider := range providers {
				cloudAccountName = fmt.Sprintf("%s-%v", provider, timeStamp)
				backupLocationName = fmt.Sprintf("auto-bl-%v", time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudAccountName, cloudCredUID,
					getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of adding backup location - %s", backupLocationName))
			}
		})

		Step("Creating Schedule Policies", func() {
			log.InfoD("Creating Schedule Policies")
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
			periodicPolicyStatus := Inst().Backup.BackupSchedulePolicy(periodicPolicyName, uuid.New(), orgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(periodicPolicyStatus, nil, fmt.Sprintf("Verification of creating periodic schedule policy - %s", periodicPolicyName))
		})

		Step("Adding Clusters for backup", func() {
			log.InfoD("Adding application clusters")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of creating source - %s and destination - %s clusters", SourceClusterName, destinationClusterName))
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
		})

		Step("Creating schedule backups", func() {
			log.InfoD("Creating schedule backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, periodicPolicyName)
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				err = CreateScheduleBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, []string{namespace},
					labelSelectors, orgID, "", "", "", "", periodicPolicyName, schPolicyUid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of creating schedule backup with schedule name - %s", backupName))
				schBackupName, err = GetFirstScheduleBackupName(ctx, backupName, orgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the first schedule backup - %s", schBackupName))
				scheduleNames = append(scheduleNames, backupName)
			}
		})

		Step("Restoring scheduled backups", func() {
			log.InfoD("Restoring scheduled backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-%s", restoreNamePrefix, schBackupName)
			err = CreateRestore(restoreName, schBackupName, namespaceMapping, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of restoring scheduled backups - %s", restoreName))
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Clean up objects after test execution")
		log.Infof("Deleting backup schedules")
		for _, scheduleName := range scheduleNames {
			scheduleUid, err := GetScheduleUID(scheduleName, orgID, ctx)
			log.FailOnError(err, "Error while getting schedule uid %v", scheduleName)
			err = DeleteSchedule(scheduleName, scheduleUid, orgID)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		log.Infof("Deleting backup schedule policy")
		policyList := []string{periodicPolicyName}
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, policyList)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", policyList))
		log.Infof("Deleting restores")
		err = DeleteRestore(restoreName, orgID, ctx)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of deleting restores - %s", restoreName))
		log.Infof("Deleting the deployed apps after test execution")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)

		CleanupCloudSettingsAndClusters(backupLocationMap, cloudAccountName, cloudCredUID, ctx)
	})
})

// This testcase verifies schedule backup creation with all namespaces.
var _ = Describe("{ScheduleBackupCreationAllNS}", func() {
	var (
		contexts           []*scheduler.Context
		appContexts        []*scheduler.Context
		backupLocationName string
		backupLocationUID  string
		cloudCredUID       string
		bkpNamespaces      []string
		scheduleNames      []string
		cloudAccountName   string
		backupName         string
		schBackupName      string
		schPolicyUid       string
		restoreName        string
		clusterStatus      api.ClusterInfo_StatusInfo_Status
	)
	var testrailID = 58015 // testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58015
	namespaceMapping := make(map[string]string)
	labelSelectors := make(map[string]string)
	cloudCredUIDMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	timeStamp := strconv.Itoa(int(time.Now().Unix()))
	periodicPolicyName := fmt.Sprintf("%s-%s", "periodic", timeStamp)

	JustBeforeEach(func() {
		StartTorpedoTest("ScheduleBackupCreationAllNS", "Create schedule backup creation with all namespaces", nil, testrailID)
		log.Infof("Application installation")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})

	It("Schedule Backup Creation with all namespaces", func() {
		Step("Validate deployed applications", func() {
			ValidateApplications(contexts)
		})
		providers := getProviders()
		Step("Adding Cloud Account", func() {
			log.InfoD("Adding cloud account")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudAccountName = fmt.Sprintf("%s-%v", provider, timeStamp)
				cloudCredUID = uuid.New()
				cloudCredUIDMap[cloudCredUID] = cloudAccountName
				err := CreateCloudCredential(provider, cloudAccountName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudAccountName, orgID, provider))
			}
		})

		Step("Adding Backup Location", func() {
			for _, provider := range providers {
				cloudAccountName = fmt.Sprintf("%s-%v", provider, timeStamp)
				backupLocationName = fmt.Sprintf("auto-bl-%v", time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudAccountName, cloudCredUID,
					getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Adding Backup Location - %s", backupLocationName))
			}
		})

		Step("Creating Schedule Policies", func() {
			log.InfoD("Adding application clusters")
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
			periodicPolicyStatus := Inst().Backup.BackupSchedulePolicy(periodicPolicyName, uuid.New(), orgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(periodicPolicyStatus, nil, fmt.Sprintf("Verification of creating periodic schedule policy - %s", periodicPolicyName))
		})

		Step("Adding Clusters for backup", func() {
			log.InfoD("Adding application clusters")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of creating source - %s and destination - %s clusters", SourceClusterName, destinationClusterName))
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
		})

		Step("Creating schedule backups", func() {
			log.InfoD("Creating schedule backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, periodicPolicyName)
			backupName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, timeStamp)
			err = CreateScheduleBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, bkpNamespaces,
				labelSelectors, orgID, "", "", "", "", periodicPolicyName, schPolicyUid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of creating schedule backup with schedule name - %s", backupName))
			schBackupName, err = GetFirstScheduleBackupName(ctx, backupName, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the first schedule backup - %s", schBackupName))
			scheduleNames = append(scheduleNames, backupName)
		})

		Step("Restoring scheduled backups", func() {
			log.InfoD("Restoring scheduled backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-%s", restoreNamePrefix, schBackupName)
			err = CreateRestore(restoreName, schBackupName, namespaceMapping, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of restoring scheduled backups - %s", restoreName))
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Clean up objects after test execution")
		log.Infof("Deleting backup schedules")
		for _, scheduleName := range scheduleNames {
			scheduleUid, err := GetScheduleUID(scheduleName, orgID, ctx)
			log.FailOnError(err, "Error while getting schedule uid %v", scheduleName)
			err = DeleteSchedule(scheduleName, scheduleUid, orgID)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		log.Infof("Deleting backup schedule policy")
		policyList := []string{periodicPolicyName}
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, policyList)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", policyList))
		log.Infof("Deleting restores")
		err = DeleteRestore(restoreName, orgID, ctx)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of deleting restores - %s", restoreName))
		log.Infof("Deleting the deployed applications after test execution")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudAccountName, cloudCredUID, ctx)
	})
})

var _ = Describe("{CustomResourceRestore}", func() {
	var (
		contexts           []*scheduler.Context
		appContexts        []*scheduler.Context
		backupLocationUID  string
		cloudCredUID       string
		bkpNamespaces      []string
		clusterUid         string
		clusterStatus      api.ClusterInfo_StatusInfo_Status
		backupName         string
		credName           string
		cloudCredUidList   []string
		backupLocationName string
		deploymentName     string
		restoreName        string
		backupNames        []string
		restoreNames       []string
	)
	labelSelectors := make(map[string]string)
	namespaceMapping := make(map[string]string)
	newBackupLocationMap := make(map[string]string)
	backupNamespaceMap := make(map[string]string)
	deploymentBackupMap := make(map[string]string)
	bkpNamespaces = make([]string, 0)

	JustBeforeEach(func() {
		StartTorpedoTest("CustomResourceRestore", "Create custom resource restore", nil, 58041)
		log.InfoD("Deploy applications")

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})
	It("Create custom resource restore", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})

		Step("Creating credentials and backup location", func() {
			log.InfoD("Creating cloud credentials and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err := CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				backupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				newBackupLocationMap[backupLocationUID] = backupLocationName
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				log.FailOnError(err, "Creating Backup location [%v] failed", backupLocationName)
				log.InfoD("Created Backup Location with name - %s", backupLocationName)
			}
		})
		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Register source and destination cluster for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			log.FailOnError(err, "Creation of Source and destination cluster failed")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				backupNamespaceMap[namespace] = backupName
				err = CreateBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, []string{namespace}, labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup creation %s", backupName))
				backupNames = append(backupNames, backupName)
			}
		})
		Step("Restoring the backed up application", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.InfoD("Restoring backed up applications")
			for _, namespace := range bkpNamespaces {
				backupName := backupNamespaceMap[namespace]
				restoreName = fmt.Sprintf("%s-%s-%v", restoreNamePrefix, backupName, time.Now().Unix())
				restoreNames = append(restoreNames, restoreName)
				restoredNameSpace := fmt.Sprintf("%s-%s", namespace, "restored")
				namespaceMapping[namespace] = restoredNameSpace
				deploymentName, err = CreateCustomRestoreWithPVCs(restoreName, backupName, namespaceMapping, SourceClusterName, orgID, ctx, make(map[string]string), namespace)
				deploymentBackupMap[backupName] = deploymentName
				log.FailOnError(err, "Restoring of backup [%s] has failed with name [%s] in namespace [%s]", backupName, restoreName, restoredNameSpace)
			}
		})

		Step("Validating restored resources", func() {
			log.InfoD("Validating restored resources")
			for _, namespace := range bkpNamespaces {
				restoreNamespace := fmt.Sprintf("%s-%s", namespace, "restored")
				backupName := backupNamespaceMap[namespace]
				deploymentName = deploymentBackupMap[backupName]
				deploymentStatus, err := apps.Instance().DescribeDeployment(deploymentName, restoreNamespace)
				log.FailOnError(err, "unable to fetch deployment status for %v", deploymentName)
				status := deploymentStatus.Conditions[1].Status
				dash.VerifyFatal(status, v1.ConditionTrue, fmt.Sprintf("checking the deployment status for %v in namespace %v", deploymentName, restoreNamespace))
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		//Delete Backup
		log.InfoD("Deleting backup")
		backupDriver := Inst().Backup
		for _, backupName := range backupNames {
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			dash.VerifySafely(err, nil, fmt.Sprintf("trying to get backup UID for backup %s", backupName))
			log.Infof("About to delete backup - %s", backupName)
			_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup %s deletion is successful", backupName))
		}
		//Delete Restore
		log.InfoD("Deleting restore")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting user restore %s", restoreName))
		}
		log.Infof("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)
		CleanupCloudSettingsAndClusters(newBackupLocationMap, credName, cloudCredUID, ctx)

	})
})

var _ = Describe("{AllNSBackupWithIncludeNewNSOption}", func() {
	var (
		contexts                   []*scheduler.Context
		cloudCredUID               string
		cloudCredName              string
		backupLocationName         string
		backupLocationUID          string
		backupLocationMap          map[string]string
		periodicSchedulePolicyName string
		periodicSchedulePolicyUid  string
		scheduleName               string
		appNamespaces              []string
		scheduleNames              []string
		appClusterName             string
		restoreName                string
		nextScheduleBackupName     interface{}
	)

	JustBeforeEach(func() {
		StartTorpedoTest("AllNSBackupWithIncludeNewNSOption", "Verification of schedule backups created with include new namespaces option", nil, 84760)
	})

	It("Validates schedule backups created with include new namespaces option includes newly created namespaces", func() {
		Step("Create cloud credentials and backup locations", func() {
			log.InfoD("Creating cloud credentials and backup locations")
			providers := getProviders()
			backupLocationMap = make(map[string]string)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				log.InfoD("Creating cloud credential named [%s] and uid [%s] using [%s] as provider", cloudCredUID, cloudCredName, provider)
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				backupLocationName = fmt.Sprintf("%s-%s-bl", provider, getGlobalBucketName(provider))
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				bucketName := getGlobalBucketName(provider)
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, bucketName, orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location named [%s] with uid [%s] of [%s] as provider", backupLocationName, backupLocationUID, provider))
			}
		})
		Step("Configure source and destination clusters with px-central-admin ctx", func() {
			log.InfoD("Configuring source and destination clusters with px-central-admin ctx")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))
			appClusterName = destinationClusterName
			clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, appClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", appClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", appClusterName))
			clusterUid, err := Inst().Backup.GetClusterUID(ctx, orgID, appClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", appClusterName))
			log.InfoD("Uid of [%s] cluster is %s", appClusterName, clusterUid)
		})
		Step("Create schedule policy", func() {
			log.InfoD("Creating a schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
			periodicSchedulePolicyUid = uuid.New()
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
			err = Inst().Backup.BackupSchedulePolicy(periodicSchedulePolicyName, periodicSchedulePolicyUid, orgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval 15 minutes named [%s]", periodicSchedulePolicyName))
			periodicSchedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, periodicSchedulePolicyName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching uid of periodic schedule policy named [%s]", periodicSchedulePolicyName))
		})
		Step("Create schedule backups", func() {
			log.InfoD("Creating a schedule backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
			namespaces := []string{"*"}
			labelSelectors := make(map[string]string)
			err = CreateScheduleBackup(scheduleName, appClusterName, backupLocationName, backupLocationUID, namespaces,
				labelSelectors, orgID, "", "", "", "", periodicSchedulePolicyName, periodicSchedulePolicyUid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule backup with schedule name [%s]", scheduleName))
			firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the first schedule backup [%s]", firstScheduleBackupName))
			scheduleNames = append(scheduleNames, scheduleName)
		})
		// To ensure applications are deployed after a schedule backup is created
		Step("Schedule applications to create new namespaces", func() {
			log.InfoD("Scheduling applications to create new namespaces")
			contexts = make([]*scheduler.Context, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
				for _, ctx := range appContexts {
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					log.InfoD("Scheduled application with namespace [%s]", namespace)
					// appNamespaces in this scenario is of newly created namespaces
					appNamespaces = append(appNamespaces, namespace)
				}
			}
		})
		Step("Validate new namespaces", func() {
			log.InfoD("Validating new namespaces")
			ValidateApplications(contexts)
		})
		Step("Verify new application namespaces inclusion in next schedule backup", func() {
			log.InfoD("Verifying new application namespaces inclusion in next schedule backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(ctx, scheduleName, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching names of all schedule backups of schedule named [%s]", scheduleName))
			currentScheduleBackupCount := len(allScheduleBackupNames)
			log.InfoD("Current number of schedule backups is [%v]", currentScheduleBackupCount)
			nextScheduleBackupOrdinal := currentScheduleBackupCount + 1
			log.InfoD("Ordinal of the next schedule backup is [%v]", nextScheduleBackupOrdinal)
			checkOrdinalScheduleBackupCreation := func() (interface{}, bool, error) {
				ordinalScheduleBackupName, err := GetOrdinalScheduleBackupName(ctx, scheduleName, nextScheduleBackupOrdinal, orgID)
				if err != nil {
					return "", true, err
				}
				return ordinalScheduleBackupName, false, nil
			}
			log.InfoD("Waiting for 15 minutes for the next schedule backup to be triggered")
			time.Sleep(15 * time.Minute)
			nextScheduleBackupName, err = DoRetryWithTimeoutWithGinkgoRecover(checkOrdinalScheduleBackupCreation, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching next schedule backup name of ordinal [%v] of schedule named [%s]", nextScheduleBackupOrdinal, scheduleName))
			log.InfoD("Next schedule backup name [%s]", nextScheduleBackupName.(string))
			err = backupSuccessCheck(nextScheduleBackupName.(string), orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying success of next schedule backup named [%s] of schedule named [%s]", nextScheduleBackupName.(string), scheduleName))
			nextScheduleBackupUid, err := Inst().Backup.GetBackupUID(ctx, nextScheduleBackupName.(string), orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching uid of of next schedule backup named [%s] of schedule named [%s]", nextScheduleBackupName, scheduleName))
			backupInspectRequest := &api.BackupInspectRequest{
				Name:  nextScheduleBackupName.(string),
				Uid:   nextScheduleBackupUid,
				OrgId: orgID,
			}
			resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Inspecting next schedule backup named [%s] and uid [%s]", nextScheduleBackupName, nextScheduleBackupUid))
			backedUpNamespaces := resp.GetBackup().GetNamespaces()
			log.InfoD("Namespaces in next schedule backup named [%s] and uid [%s] are [%v]", nextScheduleBackupName, nextScheduleBackupUid, backedUpNamespaces)
			for _, namespace := range appNamespaces {
				dash.VerifyFatal(strings.Contains(strings.Join(backedUpNamespaces, ","), namespace), true, fmt.Sprintf("Checking the new application namespace [%s] against the next scheduled backup named [%s]", namespace, nextScheduleBackupName))
			}
		})
		Step("Restore new application namespaces from next schedule backup", func() {
			log.InfoD("Restoring new application namespaces from next schedule backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			namespaceMapping := make(map[string]string)
			// Modifying namespaceMapping to restore only new namespaces
			for _, namespace := range appNamespaces {
				namespaceMapping[namespace] = namespace
			}
			log.InfoD("Namespace mapping used for restoring - %v", namespaceMapping)
			restoreName = fmt.Sprintf("%s-%s", "test-restore", RandomString(4))
			err = CreateRestore(restoreName, nextScheduleBackupName.(string), namespaceMapping, appClusterName, orgID, ctx, make(map[string]string))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, scheduleName := range scheduleNames {
			scheduleUid, err := GetScheduleUID(scheduleName, orgID, ctx)
			log.FailOnError(err, "Error while getting schedule uid %v", scheduleName)
			err = DeleteSchedule(scheduleName, scheduleUid, orgID)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		log.Infof("Deleting backup schedule policy")
		policyList := []string{periodicSchedulePolicyName}
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, policyList)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", policyList))
		err = DeleteRestore(restoreName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))

		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed namespaces - %v", appNamespaces)
		ValidateAndDestroy(contexts, opts)

		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// BackupSyncBasicTest take a good number of backups check if backup sync is working
var _ = Describe("{BackupSyncBasicTest}", func() {
	numberOfBackups, _ := strconv.Atoi(getEnv(maxBackupsToBeCreated, "10"))
	timeBetweenConsecutiveBackups := 10 * time.Second
	backupNames := make([]string, 0)
	numberOfSimultaneousBackups := 20
	var contexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	var backupLocationUID string
	var cloudCredUID string
	var backupName string
	var cloudCredUidList []string
	var appContexts []*scheduler.Context
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var customBackupLocationName string
	var credName string
	bkpNamespaces = make([]string, 0)
	backupNamespaceMap := make(map[string]string)
	backupLocationMap := make(map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("BackupSyncBasicTest",
			"Validate that the backup sync syncs all the backups present in bucket", nil, 58040)
		log.InfoD("Deploy applications")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})
	It("Validate that the backup sync syncs all the backups present in bucket", func() {
		providers := getProviders()
		Step("Validate applications and get their labels", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})

		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Using pre-provisioned bucket. Creating cloud credentials and backup location.")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err := CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
				log.InfoD("Created Backup Location with name - %s", customBackupLocationName)
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
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

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			var sem = make(chan struct{}, numberOfSimultaneousBackups)
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.InfoD("Taking %d backups", numberOfBackups)
			for _, namespace := range bkpNamespaces {
				for i := 0; i < numberOfBackups; i++ {
					sem <- struct{}{}
					time.Sleep(timeBetweenConsecutiveBackups)
					backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
					backupNames = append(backupNames, backupName)
					wg.Add(1)
					go func(backupName string) {
						defer GinkgoRecover()
						defer wg.Done()
						defer func() { <-sem }()
						err = CreateBackup(backupName, SourceClusterName, customBackupLocationName, backupLocationUID, []string{namespace},
							labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup creation: %s", backupName))
					}(backupName)
				}
				wg.Wait()
			}
			log.Infof("List of backups - %v", backupNames)
		})

		Step("Remove the backup location where backups were taken", func() {
			log.InfoD("Remove backup location where backups were taken")
			// Issue a remove backup location call
			err := DeleteBackupLocation(customBackupLocationName, backupLocationUID, orgID, false)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup location %s", customBackupLocationName))

			// Wait until backup location is removed
			backupLocationDeleteStatusCheck := func() (interface{}, bool, error) {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				status, err := IsBackupLocationPresent(customBackupLocationName, ctx, orgID)
				if err != nil {
					return "", true, fmt.Errorf("backup location %s still present with error %v", customBackupLocationName, err)
				}
				if status == true {
					return "", true, fmt.Errorf("backup location %s is not deleted yet", customBackupLocationName)
				}
				return "", false, nil
			}
			_, err = DoRetryWithTimeoutWithGinkgoRecover(backupLocationDeleteStatusCheck, 3*time.Minute, 30*time.Second)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup location %s", customBackupLocationName))
		})

		Step("Add the backup location again which had backups", func() {
			log.InfoD("Add the backup location with backups back")
			for _, provider := range providers {
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = customBackupLocationName
				err := CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
			}
		})
		Step("Taking backup of applications to trigger BackupSync goroutine", func() {
			log.InfoD("Taking backup of applications to trigger BackupSync goroutine")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				backupNamespaceMap[namespace] = backupName
				err = CreateBackup(backupName, SourceClusterName, customBackupLocationName, backupLocationUID, []string{namespace}, labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup creation %s", backupName))
				backupNames = append(backupNames, backupName)
			}
		})

		Step("Check if all backups are synced or not", func() {
			log.InfoD("Check if backups created before are synced or not")

			// Wait for backups to get synced
			checkBackupSync := func() (interface{}, bool, error) {
				fetchedBackupNames, err := GetAllBackupsAdmin()
				// Debug lines tobe removed in the next patch with the fix
				log.InfoD(fmt.Sprintf("The list of backups fetched %s", fetchedBackupNames))
				if err != nil {
					return "", true, fmt.Errorf("unable to fetch backups. Error: %s", err.Error())
				}
				if len(fetchedBackupNames) == len(backupNames) {
					return "", false, nil
				}
				return "", true, fmt.Errorf("expected: %d and actual: %d", len(backupNames), len(fetchedBackupNames))
			}
			_, err := DoRetryWithTimeoutWithGinkgoRecover(checkBackupSync, 100*time.Minute, 30*time.Second)
			log.FailOnError(err, "Wait for BackupSync to complete")
			fetchedBackupNames, err := GetAllBackupsAdmin()
			log.FailOnError(err, "Getting a list of all backups")
			dash.VerifyFatal(len(fetchedBackupNames), len(backupNames), "Comparing the expected and actual number of backups")
			var bkp *api.BackupObject
			backupDriver := Inst().Backup
			bkpEnumerateReq := &api.BackupEnumerateRequest{
				OrgId: orgID}
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			curBackups, err := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
			for _, bkp = range curBackups.GetBackups() {
				backupInspectRequest := &api.BackupInspectRequest{
					Name:  bkp.Name,
					Uid:   bkp.Uid,
					OrgId: orgID,
				}
				resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
				log.FailOnError(err, "Inspect each backup from list")
				actual := resp.GetBackup().GetStatus().Status
				expected := api.BackupInfo_StatusInfo_Success
				dash.VerifyFatal(actual, expected, fmt.Sprintf("Check each backup for success status %s", bkp.Name))
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// BackupMultipleNsWithSameLabel takes backup and restores multiple namespace having same labels
var _ = Describe("{BackupMultipleNsWithSameLabel}", func() {
	var (
		err                         error
		backupLocationUID           string
		cloudCredUID                string
		clusterUid                  string
		credName                    string
		restoreName                 string
		backupLocationName          string
		multipleNamespaceBackupName string
		nsLabelString               string
		restoreNames                []string
		bkpNamespaces               []string
		cloudCredUidList            []string
		nsLabelsMap                 map[string]string
		contexts                    []*scheduler.Context
		appContexts                 []*scheduler.Context
	)
	backupLocationMap := make(map[string]string)
	namespaceMapping := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	JustBeforeEach(func() {
		StartTorpedoTest("BackupMultipleNsWithSameLabel", "Taking backup and restoring multiple namespace having same labels", nil, 84851)
		log.InfoD("Deploy applications")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < 10; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
		log.InfoD("Created namespaces %v", bkpNamespaces)
	})
	It("Taking backup and restoring multiple namespace having same labels", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})
		Step("Adding labels to all namespaces", func() {
			log.InfoD("Adding labels to all namespaces")
			nsLabelsMap = GenerateRandomLabels(20)
			err = AddLabelsToMultipleNamespaces(nsLabelsMap, bkpNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding labels [%v] to namespaces [%v]", nsLabelsMap, bkpNamespaces))
		})
		Step("Generating namespace label string from label map for multiple namespace", func() {
			log.InfoD("Generating namespace label string from label map for multiple namespace")
			nsLabelString = MapToKeyValueString(nsLabelsMap)
			log.Infof("labels for multiple namespace %s", nsLabelString)
		})
		Step("Creating cloud credentials and registering backup location", func() {
			log.InfoD("Creating cloud credentials and registering backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err = CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating cloud credential named %v", credName))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				backupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateBackupLocation(provider, backupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %v", backupLocationName))
			}
		})
		Step("Configure source and destination clusters with px-central-admin ctx", func() {
			log.InfoD("Configuring source and destination clusters with px-central-admin ctx")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))
			appClusterName := destinationClusterName
			clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, appClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", appClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", appClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, appClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", appClusterName))
			log.InfoD("Uid of [%s] cluster is %s", appClusterName, clusterUid)
		})
		Step("Taking a backup of multiple applications with namespace label filter", func() {
			log.InfoD("Taking a backup of multiple applications with namespace label filter")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			multipleNamespaceBackupName = fmt.Sprintf("%s-%v", "multiple-namespace-backup", time.Now().Unix())
			err = CreateBackupWithNamespaceLabel(multipleNamespaceBackupName, SourceClusterName, backupLocationName, backupLocationUID,
				nil, orgID, clusterUid, "", "", "", "", nsLabelString, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup [%s] creation with label [%s]", multipleNamespaceBackupName, nsLabelString))
			err = NamespaceLabelBackupSuccessCheck(multipleNamespaceBackupName, ctx, bkpNamespaces, nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Reverifying labels added to backup [%s]", multipleNamespaceBackupName))
		})
		Step("Restoring multiple applications backup", func() {
			log.InfoD("Restoring multiple applications backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			dash.VerifyFatal(err, nil, "Fetching px-admin context")
			restoreName = fmt.Sprintf("%s-%v", restoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, multipleNamespaceBackupName, namespaceMapping, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying multiple backups [%s] restore", restoreName))
			restoreNames = append(restoreNames, restoreName)
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		dash.VerifySafely(err, nil, "Fetching px-central-admin ctx")
		for _, restoreName := range restoreNames {
			err := DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the restore named [%s]", restoreName))
		}
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed namespaces - %v", bkpNamespaces)
		ValidateAndDestroy(contexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// MultipleCustomRestoreSameTimeDiffStorageClassMapping issues multiple custom restores at the same time using different storage class mapping
var _ = Describe("{MultipleCustomRestoreSameTimeDiffStorageClassMapping}", func() {
	var (
		contexts          []*scheduler.Context
		appContexts       []*scheduler.Context
		bkpNamespaces     []string
		clusterUid        string
		clusterStatus     api.ClusterInfo_StatusInfo_Status
		backupLocationUID string
		cloudCredName     string
		cloudCredUID      string
		bkpLocationName   string
		backupName        string
		restoreList       []string
		sourceScName      *storageApi.StorageClass
		scNames           []string
		scCount           int
	)
	namespaceMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	storageClassMapping := make(map[string]string)
	k8sStorage := storage.Instance()
	params := make(map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("MultipleCustomRestoreSameTimeDiffStorageClassMapping",
			"Issue multiple custom restores at the same time using different storage class mapping", nil, 58052)
		log.InfoD("Deploy applications needed for backup")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})
	It("Issue multiple custom restores at the same time using different storage class mapping", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})
		Step("Register cluster for backup", func() {
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})
		Step("Create new storage class on source cluster for storage class mapping for restore", func() {
			log.InfoD("Create new storage class on source cluster for storage class mapping for restore")
			scCount = 10
			for i := 0; i < scCount; i++ {
				scName := fmt.Sprintf("replica-sc-%d-%v", time.Now().Unix(), i)
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
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating new storage class %v on source cluster %s", scName, SourceClusterName))
				scNames = append(scNames, scName)
			}
		})
		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})
		Step("Taking backup of application for different combination of restores", func() {
			log.InfoD("Taking backup of application for different combination of restores")
			backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, bkpNamespaces[0], time.Now().Unix())
			err = CreateBackup(backupName, SourceClusterName, bkpLocationName, backupLocationUID, []string{bkpNamespaces[0]},
				labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup: %s", backupName))
		})
		Step("Multiple restore for same backup in different storage class in same cluster at the same time", func() {
			log.InfoD(fmt.Sprintf("Multiple restore for same backup into %d different storage class in same cluster at the same time", scCount))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			pvcs, err := core.Instance().GetPersistentVolumeClaims(bkpNamespaces[0], labelSelectors)
			singlePvc := pvcs.Items[0]
			sourceScName, err = core.Instance().GetStorageClassForPVC(&singlePvc)
			var wg sync.WaitGroup
			for _, scName := range scNames {
				storageClassMapping[sourceScName.Name] = scName
				time.Sleep(2)
				namespaceMap[bkpNamespaces[0]] = fmt.Sprintf("new-namespace-%v", time.Now().Unix())
				restoreName := fmt.Sprintf("restore-new-storage-class-%s-%s", scName, RestoreNamePrefix)
				restoreList = append(restoreList, restoreName)
				wg.Add(1)
				go func(scName string) {
					defer GinkgoRecover()
					defer wg.Done()
					err = CreateRestore(restoreName, backupName, namespaceMap, SourceClusterName, orgID, ctx, storageClassMapping)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Restoring backup %v using storage class %v", backupName, scName))
				}(scName)
			}
			wg.Wait()
		})
	})
	JustAfterEach(func() {
		var wg sync.WaitGroup
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)
		log.InfoD("Deleting created restores")
		for _, restoreName := range restoreList {
			wg.Add(1)
			go func(restoreName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err = DeleteRestore(restoreName, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))
			}(restoreName)
		}
		wg.Wait()
		log.InfoD("Deleting the newly created storage class")
		for _, scName := range scNames {
			err = k8sStorage.DeleteStorageClass(scName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting storage class %s from source cluster", scName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// AddMultipleNamespaceLabel add labels to multiple namespace, perform manual backup, schedule backup using namespace label selector and restore
var _ = Describe("{AddMultipleNamespaceLabels}", func() {
	var (
		batchSize                  int
		desiredNumLabels           int
		backupLocationUID          string
		cloudCredUID               string
		clusterUid                 string
		backupName                 string
		credName                   string
		backupLocationName         string
		restoreName                string
		periodicSchedulePolicyName string
		periodicSchedulePolicyUid  string
		scheduleName               string
		firstScheduleBackupName    string
		namespaceLabel             string
		restoreNames               []string
		bkpNamespaces              []string
		cloudCredUidList           []string
		contexts                   []*scheduler.Context
		appContexts                []*scheduler.Context
		scheduleRestoreMapping     map[string]string
		fetchedLabelMap            map[string]string
		labelMap                   map[string]string
		err                        error
	)
	bkpNamespaces = make([]string, 0)
	backupLocationMap := make(map[string]string)
	scheduleRestoreMapping = make(map[string]string)
	JustBeforeEach(func() {
		StartTorpedoTest("AddMultipleNamespaceLabels", "Add multiple labels to namespaces, perform manual backup, schedule backup using namespace labels and restore", nil, 85583)
		log.InfoD("Deploy applications")
		batchSize = 10
		desiredNumLabels = 1000
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})
	It("Add multiple labels to namespaces, perform manual backup, schedule backup using namespace label and restore", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})
		Step("Adding labels to namespaces in multiple of 10 until 1000", func() {
			log.InfoD("Adding labels to namespaces in multiple of 10 until 1000")
			for i := 0; i < desiredNumLabels/batchSize; i++ {
				labelMap = GenerateRandomLabels(batchSize)
				err := AddLabelsToMultipleNamespaces(labelMap, []string{bkpNamespaces[0]})
				dash.VerifyFatal(err, nil, fmt.Sprintf("Adding [%v] labels to namespaces [%v]", batchSize, bkpNamespaces[0]))
			}
		})
		Step("Verifying labels added to namespace", func() {
			log.InfoD("Verifying number of labels added to namespace")
			fetchedLabelMap, err = Inst().S.GetNamespaceLabel(bkpNamespaces[0])
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the labels added to namespace %s", bkpNamespaces[0]))
			length := len(fetchedLabelMap)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching labels %v for namespace %v", length-3, bkpNamespaces[0]))
			dash.VerifyFatal(length-3, desiredNumLabels, fmt.Sprintf("Verifying number of added labels to desired labels for namespace %v", bkpNamespaces[0]))
		})

		Step("Creating cloud credentials and registering backup location", func() {
			log.InfoD("Creating cloud credentials and registering backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())

				err = CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating cloud credential named %s", credName))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				backupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateBackupLocation(provider, backupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocationName))
			}
		})
		Step("Configure source and destination clusters with px-central-admin ctx", func() {
			log.InfoD("Configuring source and destination clusters with px-central-admin ctx")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))
			appClusterName := destinationClusterName
			clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, appClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", appClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", appClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, appClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", appClusterName))
			log.InfoD("Uid of [%s] cluster is %s", appClusterName, clusterUid)
		})
		Step("Taking a manual backup of application using namespace labels", func() {
			log.InfoD("Taking a manual backup of application using namespace label")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			namespaceLabel = MapToKeyValueString(fetchedLabelMap)
			backupName = fmt.Sprintf("%s-%v", "backup", time.Now().Unix())
			err = CreateBackupWithNamespaceLabel(backupName, SourceClusterName, backupLocationName, backupLocationUID,
				nil, orgID, clusterUid, "", "", "", "", namespaceLabel, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup [%s] creation with label [%s]", backupName, namespaceLabel))
			err = NamespaceLabelBackupSuccessCheck(backupName, ctx, bkpNamespaces, namespaceLabel)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", bkpNamespaces, namespaceLabel, backupName))
		})
		Step("Create schedule policy", func() {
			log.InfoD("Creating a schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
			periodicSchedulePolicyUid = uuid.New()
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 2, 5)
			err = Inst().Backup.BackupSchedulePolicy(periodicSchedulePolicyName, periodicSchedulePolicyUid, orgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval 15 minutes named [%s]", periodicSchedulePolicyName))
			periodicSchedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, periodicSchedulePolicyName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching uid of periodic schedule policy named [%s]", periodicSchedulePolicyName))
		})
		Step("Creating a schedule backup with namespace label filter", func() {
			log.InfoD("Creating a schedule backup with namespace label filter")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
			err = CreateScheduleBackupWithNamespaceLabel(scheduleName, SourceClusterName, backupLocationName, backupLocationUID,
				nil, orgID, "", "", "", "", namespaceLabel, periodicSchedulePolicyName, periodicSchedulePolicyUid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule backup [%s] with labels [%v]", scheduleName, namespaceLabel))
			firstScheduleBackupName, err = GetFirstScheduleBackupName(ctx, scheduleName, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the first schedule backup [%s]", firstScheduleBackupName))
			err = NamespaceLabelBackupSuccessCheck(firstScheduleBackupName, ctx, bkpNamespaces, namespaceLabel)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", bkpNamespaces, namespaceLabel, firstScheduleBackupName))
		})
		Step("Restoring manual backup of application", func() {
			log.InfoD("Restoring manual backup of application")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-%v", backupName, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, nil, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup restore with name [%s] in default namespace", restoreName))
			restoreNames = append(restoreNames, restoreName)
		})
		Step("Restoring first schedule backup of application", func() {
			log.InfoD("Restoring first schedule backup of application")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			scheduleRestoreMapping = make(map[string]string)
			backupScheduleNamespace, err := FetchNamespacesFromBackup(ctx, firstScheduleBackupName, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %s from schedule backup %s", backupScheduleNamespace, firstScheduleBackupName))
			restoredNameSpace := fmt.Sprintf("%s-%v", backupScheduleNamespace[0], time.Now().Unix())
			scheduleRestoreMapping[backupScheduleNamespace[0]] = restoredNameSpace
			customRestoreName := fmt.Sprintf("%s-%v", scheduleName, time.Now().Unix())
			err = CreateRestore(customRestoreName, firstScheduleBackupName, scheduleRestoreMapping, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of restoring scheduled backups %s in custom namespace %v", customRestoreName, scheduleRestoreMapping))
			restoreNames = append(restoreNames, customRestoreName)
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Unable to px-central-admin ctx")
		scheduleUid, err := GetScheduleUID(scheduleName, orgID, ctx)
		log.FailOnError(err, "Error while getting schedule uid %s", scheduleName)
		err = DeleteSchedule(scheduleName, scheduleUid, orgID)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, []string{periodicSchedulePolicyName})
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", []string{periodicSchedulePolicyName}))
		for _, restoreName := range restoreNames {
			err := DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the restore named [%s]", restoreName))
		}
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed namespaces - %v", bkpNamespaces)
		ValidateAndDestroy(contexts, opts)
	})
})

// ManualAndScheduleBackupUsingNamespaceLabel Perform Namespace labeled manual and schedule backup of single and multiple namespaces along with default and custom restore
var _ = Describe("{ManualAndScheduleBackupUsingNamespaceLabel}", func() {
	var (
		err                               error
		backupLocationUID                 string
		cloudCredUID                      string
		clusterUid                        string
		manualBkpSingleNS                 string
		credName                          string
		backupLocationName                string
		restoreName                       string
		periodicSchedulePolicyName        string
		periodicSchedulePolicyUid         string
		labelForSingleNamespace           string
		labelForMultipleNamespace         string
		scheduleBkpSingleNs               string
		firstScheduleBackupName           string
		manualBkpMultipleNS               string
		scheduleBkpMultipleNs             string
		firstScheduleBackupForMultipleNs  string
		secondScheduleBackupForMultipleNs string
		secondScheduleBackupName          string
		singleNamespace                   []string
		multipleNamespace                 []string
		cloudCredUidList                  []string
		restoreNames                      []string
		bkpNamespaces                     []string
		scheduleNames                     []string
		nsLabelsGroup1                    map[string]string
		nsLabelsGroup2                    map[string]string
		namespaceMapping                  map[string]string
		multipleRestoreMapping            map[string]string
		scheduleRestoreMapping            map[string]string
		scheduleMultipleRestoreMapping    map[string]string
		contexts                          []*scheduler.Context
		appContexts                       []*scheduler.Context
	)
	backupLocationMap := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	JustBeforeEach(func() {
		StartTorpedoTest("ManualAndScheduleBackupUsingNamespaceLabel", "Namespace labeled manual and schedule backup of single and multiple namespaces along with default and custom restore", nil, 84842)
		log.InfoD("Deploy applications")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < 3; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
		log.InfoD("Created namespaces %v", bkpNamespaces)
	})
	It("Namespace labeled manual and schedule backup of single and multiple namespaces along with default and custom restore", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})
		Step("Adding labels to namespaces", func() {
			log.InfoD("Adding labels to namespaces")
			singleNamespace = []string{bkpNamespaces[0]}
			multipleNamespace = bkpNamespaces[1:]
			nsLabelsGroup1 = GenerateRandomLabels(10)
			err = AddLabelsToMultipleNamespaces(nsLabelsGroup1, multipleNamespace)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding labels [%v] to multiple namespaces [%v]", nsLabelsGroup1, multipleNamespace))
			nsLabelsGroup2 = GenerateRandomLabels(10)
			err = AddLabelsToMultipleNamespaces(nsLabelsGroup2, singleNamespace)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding labels [%v] to single namespaces [%v]", nsLabelsGroup2, singleNamespace))
		})
		Step("Generating namespace label string from label map for single and multiple namespace", func() {
			log.InfoD("Generating namespace label string from label map for single and multiple namespace")
			labelForSingleNamespace = MapToKeyValueString(nsLabelsGroup2)
			log.Infof("labels for single namespace [%s]", labelForSingleNamespace)
			labelForMultipleNamespace = MapToKeyValueString(nsLabelsGroup1)
			log.Infof("labels for multiple namespace [%s]", labelForMultipleNamespace)
		})
		Step("Creating cloud credentials and registering backup location", func() {
			log.InfoD("Creating cloud credentials and registering backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err = CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating cloud credential named %s", credName))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				backupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateBackupLocation(provider, backupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocationName))
			}
		})
		Step("Configure source and destination clusters with px-central-admin ctx", func() {
			log.InfoD("Configuring source and destination clusters with px-central-admin ctx")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))
			appClusterName := destinationClusterName
			clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, appClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", appClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", appClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, appClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", appClusterName))
			log.InfoD("Uid of [%s] cluster is %s", appClusterName, clusterUid)
		})
		Step("Taking a manual backup of single application with namespace label filter", func() {
			log.InfoD("Taking a manual backup of single application with namespace label filter")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			manualBkpSingleNS = fmt.Sprintf("%s-%v", "backup", time.Now().Unix())
			err = CreateBackupWithNamespaceLabel(manualBkpSingleNS, SourceClusterName, backupLocationName, backupLocationUID,
				nil, orgID, clusterUid, "", "", "", "", labelForSingleNamespace, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying status of backup [%s] creation with label [%s]", manualBkpSingleNS, labelForSingleNamespace))
			err = NamespaceLabelBackupSuccessCheck(manualBkpSingleNS, ctx, singleNamespace, labelForSingleNamespace)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", singleNamespace, labelForSingleNamespace, manualBkpSingleNS))
		})
		Step("Taking a manual backup of multiple applications with namespace label filter", func() {
			log.InfoD("Taking a manual backup of multiple applications with namespace label filter")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			manualBkpMultipleNS = fmt.Sprintf("%s-%v", "multiple-namespace-backup", time.Now().Unix())
			err = CreateBackupWithNamespaceLabel(manualBkpMultipleNS, SourceClusterName, backupLocationName, backupLocationUID,
				nil, orgID, clusterUid, "", "", "", "", labelForMultipleNamespace, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup [%s] creation with labels [%v]", manualBkpMultipleNS, labelForMultipleNamespace))
			err = NamespaceLabelBackupSuccessCheck(manualBkpMultipleNS, ctx, multipleNamespace, labelForMultipleNamespace)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespaces [%v] are backed up and check for labels [%s] applied to backups [%s]", multipleNamespace, labelForMultipleNamespace, manualBkpMultipleNS))
		})
		Step("Create schedule policy", func() {
			log.InfoD("Creating a schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
			periodicSchedulePolicyUid = uuid.New()
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 2, 5)
			err = Inst().Backup.BackupSchedulePolicy(periodicSchedulePolicyName, periodicSchedulePolicyUid, orgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval 15 minutes named [%s]", periodicSchedulePolicyName))
			periodicSchedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, periodicSchedulePolicyName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching uid of periodic schedule policy named [%s]", periodicSchedulePolicyName))
		})
		Step("Creating a schedule backup for single namespace with namespace label filter", func() {
			log.InfoD("Creating a schedule backup for single namespace with namespace label filter")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			scheduleBkpSingleNs = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
			err = CreateScheduleBackupWithNamespaceLabel(scheduleBkpSingleNs, SourceClusterName, backupLocationName, backupLocationUID,
				nil, orgID, "", "", "", "", labelForSingleNamespace, periodicSchedulePolicyName, periodicSchedulePolicyUid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule backup [%s] with labels [%v]", scheduleBkpSingleNs, labelForSingleNamespace))
			firstScheduleBackupName, err = GetFirstScheduleBackupName(ctx, scheduleBkpSingleNs, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the first schedule backup [%s]", firstScheduleBackupName))
			err = NamespaceLabelBackupSuccessCheck(firstScheduleBackupName, ctx, singleNamespace, labelForSingleNamespace)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", singleNamespace, labelForSingleNamespace, firstScheduleBackupName))
			scheduleNames = append(scheduleNames, scheduleBkpSingleNs)
		})
		Step("Creating a schedule backup for multiple applications with namespace label filter", func() {
			log.InfoD("Creating a schedule backup for multiple applications with namespace label filter")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			scheduleBkpMultipleNs = fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
			err = CreateScheduleBackupWithNamespaceLabel(scheduleBkpMultipleNs, SourceClusterName, backupLocationName, backupLocationUID,
				nil, orgID, "", "", "", "", labelForMultipleNamespace, periodicSchedulePolicyName, periodicSchedulePolicyUid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule backup [%s] with labels [%v]", scheduleBkpMultipleNs, labelForMultipleNamespace))
			firstScheduleBackupForMultipleNs, err = GetFirstScheduleBackupName(ctx, scheduleBkpMultipleNs, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the first schedule backup [%s]", firstScheduleBackupForMultipleNs))
			err = NamespaceLabelBackupSuccessCheck(firstScheduleBackupForMultipleNs, ctx, multipleNamespace, labelForMultipleNamespace)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespaces [%v] are backed up and check for labels [%s] applied to backups [%s]", multipleNamespace, labelForMultipleNamespace, firstScheduleBackupForMultipleNs))
			scheduleNames = append(scheduleNames, scheduleBkpMultipleNs)
		})
		Step("Restoring manual backup of single application", func() {
			log.InfoD("Restoring backup of single application")
			namespaceMapping = make(map[string]string)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			// Restore backup to default namespace
			restoreName = fmt.Sprintf("%s-%v", manualBkpSingleNS, time.Now().Unix())
			err = CreateRestore(restoreName, manualBkpSingleNS, nil, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup restore with name [%s] in default namespace", restoreName))
			// Restore backup to custom namespace
			backupNamespace, err := FetchNamespacesFromBackup(ctx, manualBkpSingleNS, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %v from backup %s", backupNamespace, manualBkpSingleNS))
			customNamespace := fmt.Sprintf("%s-%v", manualBkpSingleNS, time.Now().Unix())
			customRestoreName := fmt.Sprintf("%s-%v", backupNamespace[0], time.Now().Unix())
			namespaceMapping[backupNamespace[0]] = customNamespace
			err = CreateRestore(customRestoreName, manualBkpSingleNS, namespaceMapping, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup restore [%s] in custom namespace [%s]", customRestoreName, customNamespace))
			restoreNames = append(restoreNames, restoreName, customRestoreName)
		})
		Step("Restoring manual backup of multiple applications backup", func() {
			log.InfoD("Restoring manual backup of multiple applications backup")
			multipleRestoreMapping = make(map[string]string)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			// Restore to default namespace
			restoreName = fmt.Sprintf("%s-%v", manualBkpMultipleNS, time.Now().Unix())
			err = CreateRestore(restoreName, manualBkpMultipleNS, nil, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying multiple backups [%s] restore in default namespace", restoreName))
			multipleBackupNamespace, err := FetchNamespacesFromBackup(ctx, manualBkpMultipleNS, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %v from backup %v", multipleBackupNamespace, manualBkpMultipleNS))
			// Restore to custom namespace
			for _, namespace := range multipleBackupNamespace {
				restoredNameSpace := fmt.Sprintf("%s-%v", manualBkpMultipleNS, time.Now().Unix())
				multipleRestoreMapping[namespace] = restoredNameSpace
			}
			customRestoreName := fmt.Sprintf("%s-%v", "multiple-application", time.Now().Unix())
			err = CreateRestore(customRestoreName, manualBkpMultipleNS, multipleRestoreMapping, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying multiple backup restore [%s] in custom namespace [%v]", customRestoreName, multipleRestoreMapping))
			restoreNames = append(restoreNames, restoreName, customRestoreName)
		})
		Step("Restoring the incremental scheduled backup of single namespace", func() {
			log.InfoD("Restoring the incremental scheduled backup of single namespace")
			scheduleRestoreMapping = make(map[string]string)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			log.InfoD("Waiting for the incremental scheduled backup for single namespace to be triggered")
			time.Sleep(2 * time.Minute)
			secondScheduleBackupName, err = GetOrdinalScheduleBackupName(ctx, scheduleBkpSingleNs, 2, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the second schedule backup [%s]", secondScheduleBackupName))
			err = NamespaceLabelBackupSuccessCheck(secondScheduleBackupName, ctx, singleNamespace, labelForSingleNamespace)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", bkpNamespaces, labelForSingleNamespace, secondScheduleBackupName))
			// Restore to default namespace
			restoreName = fmt.Sprintf("%s-%v", restoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, secondScheduleBackupName, nil, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of restoring scheduled backups - %s", restoreName))
			// Restore to custom namespace
			backupScheduleNamespace, err := FetchNamespacesFromBackup(ctx, secondScheduleBackupName, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %s from schedule backup %s", backupScheduleNamespace, secondScheduleBackupName))
			restoredNameSpace := fmt.Sprintf("%s-%v", backupScheduleNamespace[0], time.Now().Unix())
			scheduleRestoreMapping[backupScheduleNamespace[0]] = restoredNameSpace
			customRestoreName := fmt.Sprintf("%s-%v", scheduleBkpSingleNs, time.Now().Unix())
			err = CreateRestore(customRestoreName, secondScheduleBackupName, scheduleRestoreMapping, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of restoring scheduled backups %s in custom namespace %v", customRestoreName, scheduleRestoreMapping))
			restoreNames = append(restoreNames, restoreName, customRestoreName)
		})
		Step("Restoring the incremental backups for multiple applications", func() {
			log.InfoD("Restoring he incremental backups for multiple applications")
			scheduleMultipleRestoreMapping = make(map[string]string)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			secondScheduleBackupForMultipleNs, err = GetOrdinalScheduleBackupName(ctx, scheduleBkpMultipleNs, 2, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the second schedule backup [%s]", secondScheduleBackupForMultipleNs))
			err = NamespaceLabelBackupSuccessCheck(secondScheduleBackupForMultipleNs, ctx, multipleNamespace, labelForMultipleNamespace)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", bkpNamespaces, labelForMultipleNamespace, secondScheduleBackupForMultipleNs))
			// Restore to default namespace
			restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, secondScheduleBackupForMultipleNs, nil, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of restoring scheduled backups for multiple application %s in default namespace", restoreName))
			// Restore to custom namespace
			multipleBackupScheduleNamespace, err := FetchNamespacesFromBackup(ctx, secondScheduleBackupForMultipleNs, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %v from schedule backup %v", multipleBackupScheduleNamespace, secondScheduleBackupForMultipleNs))
			for _, namespace := range multipleBackupScheduleNamespace {
				restoredNameSpace := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
				scheduleMultipleRestoreMapping[namespace] = restoredNameSpace
			}
			customRestoreName := fmt.Sprintf("%s-%v", scheduleBkpMultipleNs, time.Now().Unix())
			err = CreateRestore(customRestoreName, secondScheduleBackupForMultipleNs, scheduleMultipleRestoreMapping, SourceClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of restoring scheduled backups for multiple application [%s] in custom namespace [%v]", customRestoreName, scheduleMultipleRestoreMapping))
			restoreNames = append(restoreNames, customRestoreName, restoreName)
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, scheduleName := range scheduleNames {
			scheduleUid, err := GetScheduleUID(scheduleName, orgID, ctx)
			log.FailOnError(err, "Error while getting schedule uid %s", scheduleName)
			err = DeleteSchedule(scheduleName, scheduleUid, orgID)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, []string{periodicSchedulePolicyName})
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", []string{periodicSchedulePolicyName}))
		for _, restoreName := range restoreNames {
			err := DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the restore named [%s]", restoreName))
		}
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed namespaces - %v", bkpNamespaces)
		ValidateAndDestroy(contexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// MultipleInPlaceRestoreSameTime issues multiple in place restores at the same time
var _ = Describe("{MultipleInPlaceRestoreSameTime}", func() {
	var (
		contexts          []*scheduler.Context
		appContexts       []*scheduler.Context
		bkpNamespaces     []string
		clusterUid        string
		clusterStatus     api.ClusterInfo_StatusInfo_Status
		backupLocationUID string
		cloudCredName     string
		cloudCredUID      string
		bkpLocationName   string
		backupName        string
		restoreList       []string
	)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	backupNamespaceMapping := map[string]string{}
	sleepToGetUniqueTimeStamp := 2 * time.Second

	JustBeforeEach(func() {
		StartTorpedoTest("MultipleInPlaceRestoreSameTime",
			"Issue multiple in-place restores at the same time", nil, 58051)
		log.InfoD("Deploy applications needed for backup")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < 5; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})
	It("Issue multiple in-place restores at the same time", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})
		Step("Register cluster for backup", func() {
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})
		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})
		Step("Taking backup of application for different combination of restores", func() {
			log.InfoD("Taking backup of application for different combination of restores")
			var sem = make(chan struct{}, 10)
			var wg sync.WaitGroup
			for _, bkpNameSpace := range bkpNamespaces {
				sem <- struct{}{}
				backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, bkpNameSpace, time.Now().Unix())
				//Adding 2 sec sleep to have unique timestamp
				time.Sleep(sleepToGetUniqueTimeStamp)
				wg.Add(1)
				go func(bkpNameSpace string, backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					err = CreateBackup(backupName, SourceClusterName, bkpLocationName, backupLocationUID, []string{bkpNameSpace},
						labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup: %s of namespce : %s", backupName, bkpNameSpace))
					backupNamespaceMapping[bkpNameSpace] = backupName
				}(bkpNameSpace, backupName)
			}
			wg.Wait()
		})
		Step("Issuing multiple in-place restore at the same time", func() {
			log.InfoD("Issuing multiple in-place restore at the same time")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			for bkpNameSpace, backupName := range backupNamespaceMapping {
				restoreName := fmt.Sprintf("%s-%s-%v", "test-restore-recent-backup", bkpNameSpace, time.Now().Unix())
				restoreList = append(restoreList, restoreName)
				//Adding 2 sec sleep to have unique timestamp
				time.Sleep(sleepToGetUniqueTimeStamp)
				wg.Add(1)
				go func(bkpNameSpace string, backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					err = CreateRestore(restoreName, backupName, make(map[string]string), SourceClusterName, orgID, ctx, make(map[string]string))
					dash.VerifyFatal(err, nil, fmt.Sprintf("Restoring backup %v into namespce %v with replacing existing resources", backupName, bkpNameSpace))
				}(bkpNameSpace, backupName)
			}
			wg.Wait()
		})
		Step("Issuing multiple in-place restore at the same time with replace existing resources", func() {
			log.InfoD("Issuing multiple in-place restore at the same time with replace existing resources")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			for bkpNameSpace, backupName := range backupNamespaceMapping {
				restoreName := fmt.Sprintf("%s-%s-%v", "test-restore-recent-backup", bkpNameSpace, time.Now().Unix())
				restoreList = append(restoreList, restoreName)
				//Adding 2 sec sleep to have unique time stamp
				time.Sleep(sleepToGetUniqueTimeStamp)
				wg.Add(1)
				go func(bkpNameSpace string, backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					err = CreateRestoreWithReplacePolicy(restoreName, backupName, make(map[string]string), SourceClusterName, orgID, ctx, make(map[string]string), ReplacePolicy_Delete)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Restoring backup %v into namespce %v with replacing existing resources", backupName, bkpNameSpace))
				}(bkpNameSpace, backupName)
			}
			wg.Wait()
		})
	})
	JustAfterEach(func() {
		var wg sync.WaitGroup
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)
		log.InfoD("Deleting created restores")
		for _, restoreName := range restoreList {
			wg.Add(1)
			go func(restoreName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err = DeleteRestore(restoreName, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))
			}(restoreName)
		}
		wg.Wait()
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// CloudSnapsSafeWhenBackupLocationDeleteTest takes a good number of backups to check if cloud snaps are
// safe (not deleted) if backup location is removed
var _ = Describe("{CloudSnapsSafeWhenBackupLocationDeleteTest}", func() {
	numberOfBackups, _ := strconv.Atoi(getEnv(maxBackupsToBeCreated, "10"))
	var (
		contexts                 []*scheduler.Context
		backupLocationUID        string
		cloudCredUID             string
		backupName               string
		cloudCredUidList         []string
		appContexts              []*scheduler.Context
		bkpNamespaces            []string
		clusterUid               string
		clusterStatus            api.ClusterInfo_StatusInfo_Status
		customBackupLocationName string
		credName                 string
	)
	timeBetweenConsecutiveBackups := 4 * time.Second
	backupNames := make([]string, 0)
	numberOfSimultaneousBackups := 20
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	backupNamespaceMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	backupLocationMapNew := make(map[string]string)
	credMap := make(map[string]map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("CloudSnapsSafeWhenBackupLocationDeleteTest",
			"Validate if cloud snaps present if backup location is deleted", nil, 58069)
		log.InfoD("Deploy applications")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})
	It("Validate that the backup sync syncs all the backups present in bucket", func() {
		providers := getProviders()
		Step("Validate applications and get their labels", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})

		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Using pre-provisioned bucket. Creating cloud credentials and backup location.")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				credMap[provider] = make(map[string]string)
				credMap[provider][cloudCredUID] = credName
				err := CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
				backupLocationMap[backupLocationUID] = customBackupLocationName
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
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

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			var sem = make(chan struct{}, numberOfSimultaneousBackups)
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.InfoD("Taking %d backups", numberOfBackups)
			for backupLocationUID, backupLocationName := range backupLocationMap {
				for _, namespace := range bkpNamespaces {
					for i := 0; i < numberOfBackups; i++ {
						sem <- struct{}{}
						time.Sleep(timeBetweenConsecutiveBackups)
						backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
						backupNames = append(backupNames, backupName)
						wg.Add(1)
						go func(backupName string) {
							defer GinkgoRecover()
							defer wg.Done()
							defer func() { <-sem }()
							err = CreateBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, []string{namespace},
								labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup creation: %s", backupName))
						}(backupName)
					}
					wg.Wait()
				}
			}
			log.Infof("List of backups - %v", backupNames)
		})

		Step("Remove the backup locations where backups were taken", func() {
			log.InfoD("Remove backup locations where backups were taken")
			// Issue a remove backup location call
			for backupLocationUID, customBackupLocationName = range backupLocationMap {
				err := DeleteBackupLocation(customBackupLocationName, backupLocationUID, orgID, false)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting backup location %s", customBackupLocationName))

				// Wait until backup location is removed
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				backupLocationDeleteStatusCheck := func() (interface{}, bool, error) {
					status, err := IsBackupLocationPresent(customBackupLocationName, ctx, orgID)
					if err != nil {
						return "", true, fmt.Errorf("backup location %s still present with error %v", customBackupLocationName, err)
					}
					if status == true {
						return "", true, fmt.Errorf("backup location %s is not deleted yet", customBackupLocationName)
					}
					return "", false, nil
				}
				_, err = DoRetryWithTimeoutWithGinkgoRecover(backupLocationDeleteStatusCheck, 3*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting backup location %s", customBackupLocationName))
			}
		})

		Step("Add the backup location again which had backups", func() {
			log.InfoD("Add the backup location with backups back")
			for provider := range credMap {
				for cloudCredUID, credName := range credMap[provider] {
					backupLocationUID = uuid.New()
					customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
					err := CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
					backupLocationMapNew[backupLocationUID] = customBackupLocationName
				}
			}
		})
		Step("Taking backup of applications to trigger BackupSync goroutine", func() {
			log.InfoD("Taking backup of applications to trigger BackupSync goroutine")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for backupLocationUID, customBackupLocationName = range backupLocationMapNew {
				for _, namespace := range bkpNamespaces {
					backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
					err = CreateBackup(backupName, SourceClusterName, customBackupLocationName, backupLocationUID, []string{namespace}, labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup creation %s", backupName))
					backupNames = append(backupNames, backupName)
					backupNamespaceMap[namespace] = backupName
				}
			}
		})

		Step("Check if all backups are synced or not", func() {
			log.InfoD("Check if backups created before are synced or not")
			// Wait for backups to get synced
			checkBackupSync := func() (interface{}, bool, error) {
				fetchedBackupNames, err := GetAllBackupsAdmin()
				if err != nil {
					return "", true, fmt.Errorf("unable to fetch backups. Error: %s", err.Error())
				}
				if len(fetchedBackupNames) == len(backupNames) {
					return "", false, nil
				}
				return "", true, fmt.Errorf("expected: %d and actual: %d", len(backupNames), len(fetchedBackupNames))
			}
			_, err := DoRetryWithTimeoutWithGinkgoRecover(checkBackupSync, 100*time.Minute, 30*time.Second)
			log.FailOnError(err, "Wait for BackupSync to complete")
			fetchedBackupNames, err := GetAllBackupsAdmin()
			log.FailOnError(err, "Getting a list of all backups")
			dash.VerifyFatal(len(fetchedBackupNames), len(backupNames), "Comparing the expected and actual number of backups")
			var bkp *api.BackupObject
			backupDriver := Inst().Backup
			bkpEnumerateReq := &api.BackupEnumerateRequest{
				OrgId: orgID,
			}
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			curBackups, err := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
			log.FailOnError(err, "Getting a list of all backups")
			log.InfoD("Check each backup for success status")
			for _, bkp = range curBackups.GetBackups() {
				backupInspectRequest := &api.BackupInspectRequest{
					Name:  bkp.Name,
					Uid:   bkp.Uid,
					OrgId: orgID,
				}
				resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
				log.FailOnError(err, fmt.Sprintf("failed to Inspect Backup %s", bkp.Name))
				actual := resp.GetBackup().GetStatus().Status
				expected := api.BackupInfo_StatusInfo_Success
				dash.VerifyFatal(actual, expected, fmt.Sprintf("backup [%s] has success status", bkp.Name))
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMapNew, credName, cloudCredUID, ctx)
	})
})

// SetUnsetNSLabelDuringScheduleBackup Create multiple namespaces and set unset namespace labels during the backup schedule
var _ = Describe("{SetUnsetNSLabelDuringScheduleBackup}", func() {
	var (
		err                        error
		backupLocationUID          string
		cloudCredUID               string
		clusterUid                 string
		credName                   string
		backupLocationName         string
		periodicSchedulePolicyName string
		periodicSchedulePolicyUid  string
		scheduleName               string
		nsLabelString              string
		nextScheduleBackupNameOne  string
		nextScheduleBackupNameTwo  string
		allScheduleBackupNames     []string
		cloudCredUidList           []string
		bkpNamespaces              []string
		nsLabelsMap                map[string]string
		contexts                   []*scheduler.Context
		appContexts                []*scheduler.Context
	)
	backupLocationMap := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	JustBeforeEach(func() {
		StartTorpedoTest("SetUnsetNSLabelDuringScheduleBackup", "Create multiple namespaces and set unset namespace labels during the backup schedule", nil, 84849)
		log.InfoD("Deploy applications")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < 3; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
		log.InfoD("Created namespaces %v", bkpNamespaces)
	})
	It("Create multiple namespaces and set unset namespace labels", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})
		Step("Adding labels to namespaces", func() {
			log.InfoD("Adding labels to namespaces")
			nsLabelsMap = GenerateRandomLabels(20)
			err = AddLabelsToMultipleNamespaces(nsLabelsMap, bkpNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding labels [%v] to namespaces [%v]", nsLabelsMap, bkpNamespaces))
		})
		Step("Generating namespace label string from label map for multiple namespace", func() {
			log.InfoD("Generating namespace label string from label map for multiple namespace")
			nsLabelString = MapToKeyValueString(nsLabelsMap)
			log.Infof("labels for multiple namespace %s", nsLabelString)
		})
		Step("Creating cloud credentials and registering backup location", func() {
			log.InfoD("Creating cloud credentials and registering backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err = CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating cloud credentials %v", credName))
				backupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateBackupLocation(provider, backupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %v", backupLocationName))
			}
		})
		Step("Configure source and destination clusters with px-central-admin ctx", func() {
			log.InfoD("Configuring source and destination clusters with px-central-admin ctx")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))
			appClusterName := destinationClusterName
			clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, appClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", appClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", appClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, appClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", appClusterName))
			log.InfoD("Uid of [%s] cluster is %s", appClusterName, clusterUid)
		})
		Step("Create schedule policy", func() {
			log.InfoD("Creating a schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			dash.VerifyFatal(err, nil, "Fetching px-central-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
			periodicSchedulePolicyUid = uuid.New()
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
			err = Inst().Backup.BackupSchedulePolicy(periodicSchedulePolicyName, periodicSchedulePolicyUid, orgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval 15 minutes named [%s]", periodicSchedulePolicyName))
			periodicSchedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, periodicSchedulePolicyName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching uid of periodic schedule policy named [%s]", periodicSchedulePolicyName))
		})
		Step("Creating a schedule backup", func() {
			log.InfoD("Creating a schedule backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
			err = CreateScheduleBackupWithNamespaceLabel(scheduleName, SourceClusterName, backupLocationName, backupLocationUID,
				nil, orgID, "", "", "", "", nsLabelString, periodicSchedulePolicyName, periodicSchedulePolicyUid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule backup with schedule name [%s]", scheduleName))
			firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the first schedule backup [%s]", firstScheduleBackupName))
			err = NamespaceLabelBackupSuccessCheck(firstScheduleBackupName, ctx, bkpNamespaces, nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", bkpNamespaces, nsLabelString, firstScheduleBackupName))
			log.InfoD("Waiting for 15 minutes for the next schedule backup to be triggered")
			time.Sleep(15 * time.Minute)
			secondScheduleBackupName, err := GetOrdinalScheduleBackupName(ctx, scheduleName, 2, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the second schedule backup [%s]", secondScheduleBackupName))
			err = NamespaceLabelBackupSuccessCheck(secondScheduleBackupName, ctx, bkpNamespaces, nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", bkpNamespaces, nsLabelString, secondScheduleBackupName))
		})
		Step("Unset namespace label from one namespace", func() {
			log.InfoD("Unset namespace label from one namespace")
			err = Inst().S.RemoveNamespaceLabel(bkpNamespaces[0], nsLabelsMap)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Removing namespace label %v from namespace %v", nsLabelsMap, bkpNamespaces[0]))
		})
		Step("Verify namespace with removed labels is not present in next schedule backup", func() {
			log.InfoD("Verify namespace with removed labels is not present in next schedule backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			nextScheduleBackupNameOne, err = GetNextScheduleBackupName(scheduleName, 15, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup success for %s schedule backup", nextScheduleBackupNameOne))
			err = NamespaceLabelBackupSuccessCheck(nextScheduleBackupNameOne, ctx, bkpNamespaces[1:], nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", bkpNamespaces, nsLabelString, nextScheduleBackupNameOne))
		})
		Step("Set the label back to previous namespace", func() {
			log.InfoD("Set the label back to previous namespace")
			err = Inst().S.AddNamespaceLabel(bkpNamespaces[0], nsLabelsMap)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Setting the labels %v back to namespace %v", nsLabelsMap, bkpNamespaces[0]))
		})
		Step("Verify namespace inclusion in next schedule backup after setting the namespace labels back", func() {
			log.InfoD("Verify namespace inclusion in next schedule backup after setting the namespace labels back")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			nextScheduleBackupNameTwo, err = GetNextScheduleBackupName(scheduleName, 15, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup success for %s schedule backup", nextScheduleBackupNameTwo))
			err = NamespaceLabelBackupSuccessCheck(nextScheduleBackupNameTwo, ctx, bkpNamespaces, nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", bkpNamespaces, nsLabelString, nextScheduleBackupNameTwo))
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Unable to fetch px-central-admin ctx")
		scheduleUid, err := GetScheduleUID(scheduleName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Fetching uid of schedule named [%s]", scheduleName))
		log.InfoD("Deleting schedule named [%s] along with its backups [%v] and schedule policies [%v]", scheduleName, allScheduleBackupNames, []string{periodicSchedulePolicyName})
		err = DeleteSchedule(scheduleName, scheduleUid, orgID)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, []string{periodicSchedulePolicyName})
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", []string{periodicSchedulePolicyName}))
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed namespaces - %v", bkpNamespaces)
		ValidateAndDestroy(contexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// BackupRestoreOnDifferentK8sVersions Restores from a duplicate backup on a cluster with a different kubernetes version
var _ = Describe("{BackupRestoreOnDifferentK8sVersions}", func() {
	var (
		cloudCredUID       string
		cloudCredName      string
		backupLocationUID  string
		backupLocationName string
		clusterUid         string
		appNamespaces      []string
		restoreNames       []string
		backupLocationMap  map[string]string
		srcVersion         semver.Version
		destVersion        semver.Version
		contexts           []*scheduler.Context
		clusterStatus      api.ClusterInfo_StatusInfo_Status
	)
	namespaceMapping := make(map[string]string)
	duplicateBackupNameMap := make(map[string]string)
	JustBeforeEach(func() {
		StartTorpedoTest("BackupRestoreOnDifferentK8sVersions", "Restoring from a duplicate backup on a cluster with a different kubernetes version", nil, 83721)
		log.InfoD("Scheduling applications")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				log.Infof("Scheduled application with namespace [%s]", namespace)
				appNamespaces = append(appNamespaces, namespace)
			}
		}
	})
	It("Restoring from a duplicate backup on a cluster with a different kubernetes version", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(contexts)
		})
		Step("Configure source and destination clusters with px-central-admin", func() {
			log.InfoD("Configuring source and destination clusters with px-central-admin")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
		})
		Step("Fetching destination cluster kubernetes version", func() {
			log.InfoD("Fetching destination cluster kubernetes version")
			err := SetDestinationKubeConfig()
			log.FailOnError(err, "Unable to switch context to destination cluster %s", destinationClusterName)
			version, err := k8s.ClusterVersion()
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching destination cluster version %v", version))
			destVersion, err = semver.Make(version)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching destination cluster version %v", destVersion))
		})
		Step("Switching context to source cluster for backup creation", func() {
			log.InfoD("Switching context to source cluster for backup creation")
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Unable to switch context to source cluster %s", SourceClusterName)
		})
		Step("Fetching source cluster kubernetes version", func() {
			log.InfoD("Fetching source cluster kubernetes version")
			version, err := k8s.ClusterVersion()
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching source cluster version %v", version))
			srcVersion, err = semver.Make(version)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching source cluster version %v", srcVersion))
		})
		Step("Compare source and destination cluster version", func() {
			log.InfoD("Source cluster version: %s ; destination cluster version: %s", srcVersion.String(), destVersion.String())
			isTrue := srcVersion.LT(destVersion)
			dash.VerifyFatal(isTrue, true, "Verifying if source cluster's kubernetes version is lesser than the destination cluster's kubernetes version")
		})
		Step("Creating cloud credentials and registering Backup location", func() {
			log.InfoD("Creating cloud credentials and registering Backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			providers := getProviders()
			backupLocationMap = make(map[string]string)
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				log.InfoD("Creating cloud credential named [%s] and uid [%s] using [%s] as provider", cloudCredUID, cloudCredName, provider)
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating cloud credentials %v", cloudCredName))
				backupLocationName = fmt.Sprintf("%s-%s-bl", provider, getGlobalBucketName(provider))
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				bucketName := getGlobalBucketName(provider)
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, bucketName, orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location named [%s] with uid [%s] of [%s] as provider", backupLocationName, backupLocationUID, provider))
			}
		})
		Step("Taking backup of applications and duplicating it", func() {
			log.InfoD("Taking backup of applications and duplicating it")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for _, namespace := range appNamespaces {
				backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
				err = CreateBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, []string{namespace}, nil, orgID, clusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup %s creation", backupName))
				duplicateBackupName := fmt.Sprintf("%s-duplicate-%v", BackupNamePrefix, time.Now().Unix())
				duplicateBackupNameMap[duplicateBackupName] = namespace
				err = CreateBackup(duplicateBackupName, SourceClusterName, backupLocationName, backupLocationUID, []string{namespace}, nil, orgID, clusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying duplicate backup creation: %s", duplicateBackupName))
			}
		})
		Step("Restoring duplicate backup on destination cluster with different kubernetes version", func() {
			log.InfoD("Restoring duplicate backup on destination cluster with different kubernetes version")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Unable to switch context to destination cluster %s", destinationClusterName)
			for duplicateBackupName, namespace := range duplicateBackupNameMap {
				restoreName := fmt.Sprintf("%s-%s-%v", restoreNamePrefix, duplicateBackupName, time.Now().Unix())
				restoreNames = append(restoreNames, restoreName)
				namespaceMapping[namespace] = namespace
				err := CreateRestore(restoreName, duplicateBackupName, namespaceMapping, destinationClusterName, orgID, ctx, nil)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore from duplicate backup [%s]", restoreName))
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		err := SetSourceKubeConfig()
		dash.VerifyFatal(err, nil, "Switching context to source cluster")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Unable to fetch px-central-admin ctx")
		for _, restoreName := range restoreNames {
			err := DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the restore named [%s] in ctx", restoreName))
		}
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed namespaces - %v", appNamespaces)
		ValidateAndDestroy(contexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// BackupCRs.MultipleRestoresOnHigherK8sVersion deploys CRs (CRD + webhook) -> backups them up -> creates two simulatanous restores on a cluster with higher K8s version :: one restore is Success and other PartialSuccess
var _ = Describe("{BackupCRs.MultipleRestoresOnHigherK8sVersion}", func() {

	var (
		backupNames              []string             // backups in px-backup
		restoreNames             []string             // restores in px-backup
		restoreLaterNames        []string             // restore-laters in px-backup
		scheduledAppContexts     []*scheduler.Context // Each Context is for one Namespace which corresponds to one App
		backedupAppContexts      []*scheduler.Context // Each Context is for one backup in px-backup
		restoredAppContexts      []*scheduler.Context // Each Context is for one restore in px-backup
		restoredLaterAppContexts []*scheduler.Context // Each Context is for one restore-later in px-backup
		preRuleNameList          []string
		postRuleNameList         []string
		scheduledClusterUid      string
		cloudCredName            string
		cloudCredUID             string
		backupLocationUID        string
		backupLocationName       string
	)

	var (
		appList           = Inst().AppList
		namespaceMapping  = make(map[string]string)
		backupLocationMap = make(map[string]string)
		labelSelectors    = make(map[string]string)
		providers         = getProviders()
	)

	JustBeforeEach(func() {
		StartTorpedoTest("BackupCRs.MultipleRestoresOnHigherK8sVersion", "deploy CRs (CRD + webhook); then backup; create two simulatanous restores on cluster with higher K8s version; one restore is Success and other PartialSuccess", nil, 83716)
	})

	It("Deploy CRs (CRD + webhook); Backup; two simulatanous Restores with one Success and other PartialSuccess. (Backup and Restore on different K8s version)", func() {

		defer func() {
			log.InfoD("switching to default context")
			err1 := SetClusterContext("")
			log.FailOnError(err1, "failed to SetClusterContext to default cluster")
		}()

		Step("Verify if app used to execute test is a valid/allowed spec (apps) for *this* test", func() {
			log.InfoD("specs (apps) allowed in execution of test: %v", appsWithCRDsAndWebhooks)
			for i := 0; i < len(appList); i++ {
				contains := Contains(appsWithCRDsAndWebhooks, appList[i])
				dash.VerifyFatal(contains, true, fmt.Sprintf("app [%s] allowed in execution of this test", appList[i]))
			}
		})

		Step("verify kubernetes version of source and destination cluster", func() {
			var srcVer, destVer semver.Version
			log.InfoD("begin verification kubernetes version of source and destination cluster")

			Step("Registering cluster for backup", func() {
				log.InfoD("Registering cluster for backup")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")

				err = CreateSourceAndDestClusters(orgID, "", "", ctx)
				dash.VerifyFatal(err, nil, "Creating source and destination cluster")

				clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))

				scheduledClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))

				clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
			})

			Step("Get kubernetes source cluster version", func() {
				log.InfoD("switched context to source")

				sourceClusterConfigPath, err := GetSourceClusterConfigPath()
				log.FailOnError(err, "failed to get kubeconfig path for source cluster. Error: [%v]", err)

				err = Inst().S.SetConfig(sourceClusterConfigPath)
				log.FailOnError(err, "failed to switch to context to source cluster [%v]", sourceClusterConfigPath)

				ver, err := k8s.ClusterVersion()
				log.FailOnError(err, "failed to get source cluster version")
				srcVer, err = semver.Make(ver)
				log.FailOnError(err, "failed to get source cluster version")
			})

			Step("Get kubernetes destination cluster version", func() {
				log.InfoD("switched context to destination")

				destinationClusterConfigPath, err := GetDestinationClusterConfigPath()
				log.FailOnError(err, "failed to get kubeconfig path for destination cluster. Error: [%v]", err)

				err = Inst().S.SetConfig(destinationClusterConfigPath)
				log.FailOnError(err, "failed to switch to context to destination cluster [%v]", destinationClusterConfigPath)

				ver, err := k8s.ClusterVersion()
				log.FailOnError(err, "failed to get destination cluster version")
				destVer, err = semver.Make(ver)
				log.FailOnError(err, "failed to get destination cluster version")
			})

			Step("Compare Source and Destination cluster version numbers", func() {
				log.InfoD("source cluster version: %s ; destination cluster version: %s", srcVer.String(), destVer.String())
				isValid := srcVer.LT(destVer)
				dash.VerifyFatal(isValid, true,
					"source cluster kubernetes version should be lesser than the destination cluster kubernetes version.")
			})

			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		})

		Step("deploy the applications on Src cluster", func() {
			log.InfoD("deploy the applications on Src cluster")

			Step("deploy applications", func() {
				log.InfoD("deploy applications")

				log.InfoD("switching to source context")
				err := SetSourceKubeConfig()
				log.FailOnError(err, "failed to switch to context to source cluster")

				log.InfoD("scheduling applications")
				scheduledAppContexts = make([]*scheduler.Context, 0)
				for i := 0; i < Inst().GlobalScaleFactor; i++ {
					taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
					appContexts := ScheduleApplications(taskName)
					for _, appCtx := range appContexts {
						appCtx.ReadinessTimeout = appReadinessTimeout
						namespace := GetAppNamespace(appCtx, taskName)
						appCtx.ScheduleOptions.Namespace = namespace
						scheduledAppContexts = append(scheduledAppContexts, appCtx)
					}
				}
			})

			Step("Validate applications", func() {
				ValidateApplications(scheduledAppContexts)

				log.InfoD("switching to default context")
				err := SetClusterContext("")
				log.FailOnError(err, "failed to SetClusterContext to default cluster")
			})

			log.InfoD("waiting (for 2 minutes) for any CRs to finish starting up.")
			time.Sleep(time.Minute * 2)
			log.Warnf("no verification is done; it might lead to undetectable errors.")
		})

		Step("Creating rules for backup", func() {
			log.InfoD("creating pre rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "pre")
				log.FailOnError(err, "creating pre rule for deployed apps failed")
				dash.VerifyFatal(preRuleStatus, true, "verifying pre rule for backup")

				if ruleName != "" {
					preRuleNameList = append(preRuleNameList, ruleName)
				}
			}
			log.InfoD("Creating post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "post")
				log.FailOnError(err, "creating post rule for deployed apps failed")
				dash.VerifyFatal(postRuleStatus, true, "verifying Post rule for backup")
				if ruleName != "" {
					postRuleNameList = append(postRuleNameList, ruleName)
				}
			}
		})

		Step("Creating bucket, backup location and cloud credentials", func() {
			log.InfoD("Creating backup location and cloud setting")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				backupLocationName = fmt.Sprintf("%s-%s-bl", provider, getGlobalBucketName(provider))
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, "Creating backup location")
			}
		})

		Step("Taking backup of application from source cluster", func() {
			log.InfoD("taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			backupNames = make([]string, 0)
			backedupAppContexts = make([]*scheduler.Context, 0)

			backedupAppContexts = make([]*scheduler.Context, 0)
			for i, appCtx := range scheduledAppContexts {
				scheduledNamespace := appCtx.ScheduleOptions.Namespace
				backupName := fmt.Sprintf("%s-%s-%v", BackupNamePrefix, scheduledNamespace, time.Now().Unix())
				log.InfoD("creating backup [%s] in source cluster [%s] (%s), organization [%s], of namespace [%s], in backup location [%s]", backupName, SourceClusterName, scheduledClusterUid, orgID, scheduledNamespace, backupLocationName)
				err := CreateBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, []string{scheduledNamespace}, labelSelectors, orgID, scheduledClusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", backupName))
				backupNames = append(backupNames, backupName)

				log.Infof("Obtaining SourceClusterConfigPath")
				sourceClusterConfigPath, err := GetSourceClusterConfigPath()
				log.FailOnError(err, "failed to get kubeconfig path for source cluster. Error: [%v]", err)

				log.InfoD("Validating Backup [%s]", backupName)
				backupCtxs, err := ValidateBackup(ctx, backupName, orgID, []*scheduler.Context{scheduledAppContexts[i]}, true, true, sourceClusterConfigPath)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validation of backup [%s]", backupName))
				backedupAppContexts = append(backedupAppContexts, backupCtxs[0])
			}
		})

		Step("Restoring the backed up applications on destination cluster", func() {

			log.InfoD("Restoring the backed up applications on destination cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			for i, appCtx := range scheduledAppContexts {
				var initialRestoreName, restoreLaterName string

				scheduledNamespace := appCtx.ScheduleOptions.Namespace

				Step("Restoring the backed up application to namespace of same name on destination cluster", func() {
					log.InfoD("restoring the backed up application to namespace of same name on destination cluster")

					initialRestoreName = fmt.Sprintf("%s-%s-initial-%v", restoreNamePrefix, scheduledNamespace, time.Now().Unix())
					restoreNamespace := scheduledNamespace
					namespaceMapping[scheduledNamespace] = restoreNamespace

					log.InfoD("creating Initial Restore [%s] in destination cluster [%s], organization [%s], in namespace [%s]", initialRestoreName, destinationClusterName, orgID, restoreNamespace)
					_, err = CreateRestoreWithoutCheck(initialRestoreName, backupNames[i], namespaceMapping, destinationClusterName, orgID, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("initiation of initial restore [%s]", initialRestoreName))
					restoreNames = append(restoreNames, initialRestoreName)

					restoreInspectRequest := &api.RestoreInspectRequest{
						Name:  initialRestoreName,
						OrgId: orgID,
					}
					restoreInProgressCheck := func() (interface{}, bool, error) {
						resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
						if err != nil {
							err := fmt.Errorf("failed getting restore status for - [%s]; Err: [%s]", initialRestoreName, err)
							return "", false, err
						}
						restoreResponseStatus := resp.GetRestore().GetStatus()

						// Status should be LATER than InProgress in order for next STEP to execute
						if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_InProgress {
							log.InfoD("restore status of [%s] is [%s]; expected [InProgress].\ncondition fulfilled.", initialRestoreName, restoreResponseStatus.GetStatus())
							return "", false, nil
						} else if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_PartialSuccess {
							err := fmt.Errorf("restore status of [%s] is [%s]; expected [InProgress].\nhelp: check for remnant cluster-level resources on destination cluster.", initialRestoreName, restoreResponseStatus.GetStatus())
							return "", false, err
						} else if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Success {
							err := fmt.Errorf("restore status of [%s] is [%s]; expected [InProgress].\nhelp: check for status frequently", initialRestoreName, restoreResponseStatus.GetStatus())
							return "", false, err
						} else if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Aborted ||
							restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Failed ||
							restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Deleting {
							err := fmt.Errorf("restore status of [%s] is [%s]; expected [InProgress].", initialRestoreName, restoreResponseStatus.GetStatus())
							return "", false, err
						}

						err = fmt.Errorf("restore status of [%s] is [%s]; waiting for [InProgress]...", initialRestoreName, restoreResponseStatus.GetStatus())
						return "", true, err
					}
					_, err = DoRetryWithTimeoutWithGinkgoRecover(restoreInProgressCheck, 10*time.Minute, 4*time.Second)
					dash.VerifyFatal(err, nil, fmt.Sprintf("status of initial restore [%s] is [InProgress]", initialRestoreName))
				})

				var restoreLaterStatusErr error
				var laterRestoreStatus interface{}

				Step("Restoring the backed up application to namespace with different name on destination cluster", func() {
					log.InfoD("Restoring the backed up application to namespace with different name on destination cluster")

					restoreLaterName = fmt.Sprintf("%s-%s-later-%v", restoreNamePrefix, scheduledNamespace, time.Now().Unix())
					restoreLaterNamespace := fmt.Sprintf("%s-%s", scheduledNamespace, "later")
					namespaceMapping := make(map[string]string) //using local version in order to not change 'global' mapping as the key is the same
					namespaceMapping[scheduledNamespace] = restoreLaterNamespace

					log.InfoD("creating Later Restore [%s] in destination cluster [%s], organization [%s], in namespace [%s]", restoreLaterName, destinationClusterName, orgID, restoreLaterNamespace)
					_, err = CreateRestoreWithoutCheck(restoreLaterName, backupNames[i], namespaceMapping, destinationClusterName, orgID, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("initiation of later restore [%s]", restoreLaterName))
					restoreLaterNames = append(restoreLaterNames, restoreLaterName)

					restoreInspectRequest := &api.RestoreInspectRequest{
						Name:  restoreLaterName,
						OrgId: orgID,
					}
					restorePartialSuccessOrSuccessCheck := func() (interface{}, bool, error) {
						resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
						if err != nil {
							err := fmt.Errorf("failed getting restore status for - [%s]; Err: [%s]", restoreLaterName, err)
							return "", false, err
						}
						restoreResponseStatus := resp.GetRestore().GetStatus()

						if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_PartialSuccess || restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Success {
							log.InfoD("restore status of [%s] is [%s]; expected 'PartialSuccess' or 'Success'.\ncondition fulfilled.", restoreLaterName, restoreResponseStatus.GetStatus())
							return restoreResponseStatus.GetStatus(), false, nil
						} else if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Aborted ||
							restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Failed ||
							restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Deleting {
							err := fmt.Errorf("restore status of [%s] is [%s]; expected 'PartialSuccess' or 'Success'.", restoreLaterName, restoreResponseStatus.GetStatus())
							return restoreResponseStatus.GetStatus(), false, err
						}

						err = fmt.Errorf("restore status of [%s] is [%s]; waiting for 'PartialSuccess' or 'Success'...", restoreLaterName, restoreResponseStatus.GetStatus())
						return "", true, err
					}
					laterRestoreStatus, restoreLaterStatusErr = DoRetryWithTimeoutWithGinkgoRecover(restorePartialSuccessOrSuccessCheck, 10*time.Minute, 30*time.Second)

					// we don't end the test if there is an error here, as we also want to ensure that we look into the status of the following `Step`, so that we have the full details of what went wrong.
					dash.VerifySafely(restoreLaterStatusErr, nil, fmt.Sprintf("status of later restore [%s] is either 'PartialSuccess' or 'Success'", restoreLaterName))

					// We can consider validation and cleanup for 'PartialSuccess' and 'Success'
					if restoreLaterStatusErr == nil {
						// Validation of Later Restore
						log.Infof("Obtaining DestinationClusterConfigPath")
						destinationClusterConfigPath, err := GetDestinationClusterConfigPath()
						log.FailOnError(err, "failed to get kubeconfig path for destination cluster. Error: [%v]", err)

						restoreLaterCtx, err := ValidateRestore(ctx, restoreLaterName, orgID, []*scheduler.Context{backedupAppContexts[i]}, make(map[string]string), make(map[string]string), destinationClusterConfigPath)
						dash.VerifyFatal(err, nil, fmt.Sprintf("validation of later restore [%s] is success", restoreLaterName))
					} else {
						log.Warnf("proceeding to next step, after which the test will be failed.")
					}
				})

				Step("Validating status of Initial and Later Restore", func() {
					log.InfoD("Step: Validating status of Initial and Later Restore")

					log.InfoD("Verifying status of Initial Restore")
					// getting the status of initial restore
					restoreInspectRequest := &api.RestoreInspectRequest{
						Name:  initialRestoreName,
						OrgId: orgID,
					}
					restoreSuccessOrPartialSuccessCheck := func() (interface{}, bool, error) {
						resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
						if err != nil {
							err := fmt.Errorf("failed getting restore status for - %s; Err: %s", initialRestoreName, err)
							return "", false, err
						}
						restoreResponseStatus := resp.GetRestore().GetStatus()

						if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Success || restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_PartialSuccess {
							log.InfoD("restore status of [%s] is [%s]; expected 'PartialSuccess' or 'Success'.\ncondition fulfilled.", initialRestoreName, restoreResponseStatus.GetStatus())
							return restoreResponseStatus.GetStatus(), false, nil
						} else if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Aborted ||
							restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Failed ||
							restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Deleting {
							err := fmt.Errorf("restore status of [%s] is [%s]; expected 'PartialSuccess' or 'Success'.", initialRestoreName, restoreResponseStatus.GetStatus())
							return restoreResponseStatus.GetStatus(), false, err
						}

						err = fmt.Errorf("restore status of [%s] is [%s]; waiting for 'PartialSuccess' or 'Success'...", initialRestoreName, restoreResponseStatus.GetStatus())
						return "", true, err
					}
					initialRestoreStatus, initialRestoreError := DoRetryWithTimeoutWithGinkgoRecover(restoreSuccessOrPartialSuccessCheck, 10*time.Minute, 30*time.Second)

					dash.VerifyFatal(initialRestoreError, nil, fmt.Sprintf("status of initial restore [%s] is 'PartialSuccess' or 'Success'", initialRestoreName))

					// Validation of Inital Restore
					log.Infof("Obtaining DestinationClusterConfigPath")
					destinationClusterConfigPath, err := GetDestinationClusterConfigPath()
					log.FailOnError(err, "failed to get kubeconfig path for destination cluster. Error: [%v]", err)

					restoreCtx, err := ValidateRestore(ctx, initialRestoreName, orgID, []*scheduler.Context{backedupAppContexts[i]}, make(map[string]string), make(map[string]string), destinationClusterConfigPath)
					dash.VerifyFatal(err, nil, fmt.Sprintf("validation of initial restore [%s] is success", initialRestoreName))
					restoredAppContexts = append(restoredAppContexts, restoreCtx)

					// If Later Restore was an error before, we have to fail the test at this point, having processed the other stage
					dash.VerifyFatal(restoreLaterStatusErr, nil, fmt.Sprintf("status of later restore [%s] is 'PartialSuccess' or 'Success'", restoreLaterName))

					// Checking actual validity of restore status
					log.InfoD("Validating status of Initial and Later Restore...")
					validity := false
					errHelpStr := ""
					log.InfoD("states of [initial,later] restore are [%s,%s]", initialRestoreStatus, laterRestoreStatus)
					if (initialRestoreStatus == api.RestoreInfo_StatusInfo_Success && laterRestoreStatus == api.RestoreInfo_StatusInfo_PartialSuccess) ||
						(initialRestoreStatus == api.RestoreInfo_StatusInfo_PartialSuccess && laterRestoreStatus == api.RestoreInfo_StatusInfo_Success) {
						validity = true
					} else if initialRestoreStatus == api.RestoreInfo_StatusInfo_PartialSuccess && laterRestoreStatus == api.RestoreInfo_StatusInfo_PartialSuccess {
						validity = false
						errHelpStr = "Error. help: ensure no remnant cluster-level resources on destination cluster."
					} else if initialRestoreStatus == api.RestoreInfo_StatusInfo_Success && laterRestoreStatus == api.RestoreInfo_StatusInfo_Success {
						validity = false
						errHelpStr = "Error. help: ensure app has cluster-level resources."
					}
					dash.VerifyFatal(validity, true, fmt.Sprintf("states of (initial,later) restore are (Success,PartialSuccess) or (PartialSuccess,Success).\n %s", errHelpStr))
				})

			}
		})

	})

	JustAfterEach(func() {
		log.InfoD("Begin JustAfterEach")
		defer func() { log.InfoD("End JustAfterEach") }()

		defer func() {
			log.InfoD("switching to default context")
			err1 := SetClusterContext("")
			log.FailOnError(err1, "failed to SetClusterContext to default cluster")
		}()

		defer EndTorpedoTest()

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "fetching px-central-admin ctx")

		if len(preRuleNameList) > 0 {
			for _, ruleName := range preRuleNameList {
				err := Inst().Backup.DeleteRuleForBackup(orgID, ruleName)
				dash.VerifySafely(err, nil, fmt.Sprintf("deleting backup pre rules %s", ruleName))
			}
		}
		if len(postRuleNameList) > 0 {
			for _, ruleName := range postRuleNameList {
				err := Inst().Backup.DeleteRuleForBackup(orgID, ruleName)
				dash.VerifySafely(err, nil, fmt.Sprintf("deleting backup post rules %s", ruleName))
			}
		}

		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = false
		log.InfoD("deleting deployed applications for source and destination clusters")

		log.InfoD("switching to source context")
		err = SetSourceKubeConfig()
		log.FailOnError(err, "failed to switch to context to source cluster")

		log.InfoD("deleting deployed applications on source clusters")
		ValidateAndDestroy(scheduledAppContexts, opts)

		log.InfoD("waiting (for 1 minute) for any Resources created by Operator of Custom Resources to finish being destroyed.")
		time.Sleep(time.Minute * 1)
		log.Warn("no verification of destruction is done; it might lead to undetectable errors")

		log.InfoD("switching to destination context")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "failed to switch to context to destination cluster")

		log.InfoD("deleting restored namespaces on destination clusters")
		ValidateAndDestroy(restoredAppContexts, opts)

		//TODO: delete restore-later apps
		log.Warn("not deleting deployed applications (restore-later) on destination clusters")

		log.InfoD("waiting (for 1 minute) for any Resources created by Operator of Custom Resources to finish being destroyed")
		time.Sleep(time.Minute * 1)
		log.Warn("no verification of destruction is done; it might lead to undetectable errors")

		log.InfoD("switching to default context")
		err = SetClusterContext("")
		log.FailOnError(err, "failed to SetClusterContext to default cluster")

		backupDriver := Inst().Backup
		log.InfoD("deleting backed up namespaces")
		for _, backupName := range backupNames {
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "failed while trying to get backup UID for - %s", backupName)
			backupDeleteResponse, err := DeleteBackup(backupName, backupUID, orgID, ctx)
			log.FailOnError(err, "backup [%s] could not be deleted", backupName)
			dash.VerifyFatal(backupDeleteResponse.String(), "", fmt.Sprintf("verifying [%s] backup deletion is successful", backupName))
		}

		log.InfoD("deleting restores")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("deleting Restore [%s]", restoreName))
		}

		log.InfoD("deleting restore-laters")
		for _, restoreLaterName := range restoreLaterNames {
			err = DeleteRestore(restoreLaterName, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("deleting Restore [%s]", restoreLaterName))
		}

		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})
