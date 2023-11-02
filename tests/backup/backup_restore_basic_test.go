package tests

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/blang/semver"

	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers"
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
)

// BasicSelectiveRestore selects random backed-up apps and restores them
var _ = Describe("{BasicSelectiveRestore}", func() {
	var (
		backupName           string
		scheduledAppContexts []*scheduler.Context
		bkpNamespaces        []string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		restoreName          string
		cloudCredName        string
		cloudCredUID         string
		backupLocationUID    string
		bkpLocationName      string
		numDeployments       int
		providers            []string
		backupLocationMap    map[string]string
		labelSelectors       map[string]string
	)
	JustBeforeEach(func() {
		backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
		bkpNamespaces = make([]string, 0)
		restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
		backupLocationMap = make(map[string]string)
		labelSelectors = make(map[string]string)

		numDeployments = 6 // For this test case to have relevance, it is necessary to raise the number of deployments.
		providers = getProviders()

		StartPxBackupTorpedoTest("BasicSelectiveRestore", "All namespace backup and restore selective namespaces", nil, 83717, KPhalgun, Q1FY24)
		log.InfoD(fmt.Sprintf("App list %v", Inst().AppList))
		scheduledAppContexts = make([]*scheduler.Context, 0)
		log.InfoD("Starting to deploy applications")
		for i := 0; i < numDeployments; i++ {
			log.InfoD(fmt.Sprintf("Iteration %v of deploying applications", i))
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
	It("Selective Restore From a Basic Backup", func() {

		Step("Validating deployed applications", func() {
			log.InfoD("Validating deployed applications")
			ValidateApplications(scheduledAppContexts)
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
			err = CreateApplicationClusters(orgID, "", "", ctx)
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

			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
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
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed applications")
		DestroyApps(scheduledAppContexts, opts)
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
	var scheduledAppContexts []*scheduler.Context
	labelSelectors := make(map[string]string)
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
	var backupNames []string
	var restoreNames []string
	bkpNamespaces = make([]string, 0)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("CustomResourceBackupAndRestore", "Create custom resource backup and restore", nil, 83720, Kshithijiyer, Q4FY23)
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
	It("Create custom resource backup and restore", func() {
		Step("Validate applications", func() {
			ValidateApplications(scheduledAppContexts)
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
			err = CreateApplicationClusters(orgID, "", "", ctx)
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
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithCustomResourceTypeWithValidation(ctx, backupName, SourceClusterName, backupLocation, backupLocationUID, appContextsToBackup, []string{"PersistentVolumeClaim"}, nil, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s] with custom resources", backupName))
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
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

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
		appList              = Inst().AppList
		backupName           string
		scheduledAppContexts []*scheduler.Context
		preRuleNameList      []string
		postRuleNameList     []string
		bkpNamespaces        []string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		restoreName          string
		cloudCredName        string
		cloudCredUID         string
		backupLocationUID    string
		bkpLocationName      string
		preRuleName          string
		postRuleName         string
		preRuleUid           string
		postRuleUid          string
	)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	var namespaceMapping map[string]string
	namespaceMapping = make(map[string]string)
	intervalName := fmt.Sprintf("%s-%v", "interval", time.Now().Unix())
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("DeleteAllBackupObjects", "Create the backup Objects and Delete", nil, 58088, Skonda, Q4FY23)
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
	It("Create backup objects and delete", func() {
		providers := getProviders()

		Step("Validate applications", func() {
			ValidateApplications(scheduledAppContexts)
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
			err = CreateApplicationClusters(orgID, "", "", ctx)
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
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, preRuleName, preRuleUid, postRuleName, postRuleUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
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
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.Infof(" Deleting deployed applications")
		DestroyApps(scheduledAppContexts, opts)
	})
})

// This testcase verifies schedule backup creation with all namespaces.
var _ = Describe("{ScheduleBackupCreationAllNS}", func() {
	var (
		scheduledAppContexts    []*scheduler.Context
		backupLocationName      string
		backupLocationUID       string
		cloudCredUID            string
		bkpNamespaces           []string
		scheduleNames           []string
		cloudAccountName        string
		backupName              string
		firstScheduleBackupName string
		schPolicyUid            string
		restoreName             string
		clusterStatus           api.ClusterInfo_StatusInfo_Status
	)
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58015
	namespaceMapping := make(map[string]string)
	labelSelectors := make(map[string]string)
	cloudCredUIDMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	timeStamp := strconv.Itoa(int(time.Now().Unix()))
	periodicPolicyName := fmt.Sprintf("%s-%s", "periodic", timeStamp)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("ScheduleBackupCreationAllNS", "Create schedule backup creation with all namespaces", nil, 58015, Vpinisetti, Q4FY23)
		log.Infof("Application installation")
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

	It("Schedule Backup Creation with all namespaces", func() {
		Step("Validate deployed applications", func() {
			ValidateApplications(scheduledAppContexts)
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
			err = CreateApplicationClusters(orgID, "", "", ctx)
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
			firstScheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, orgID, "", "", "", "", periodicPolicyName, schPolicyUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of schedule backup with schedule name [%s]", backupName))
			scheduleNames = append(scheduleNames, backupName)
		})

		Step("Restoring scheduled backups", func() {
			log.InfoD("Restoring scheduled backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-%s", restoreNamePrefix, firstScheduleBackupName)
			err = CreateRestore(restoreName, firstScheduleBackupName, namespaceMapping, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of restoring scheduled backups - %s", restoreName))
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Clean up objects after test execution")
		log.Infof("Deleting backup schedules")
		for _, scheduleName := range scheduleNames {
			err = DeleteSchedule(scheduleName, SourceClusterName, orgID, ctx)
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
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudAccountName, cloudCredUID, ctx)
	})
})

var _ = Describe("{CustomResourceRestore}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
		backupLocationUID    string
		cloudCredUID         string
		bkpNamespaces        []string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		backupName           string
		credName             string
		cloudCredUidList     []string
		backupLocationName   string
		deploymentName       string
		restoreName          string
		backupNames          []string
		restoreNames         []string
	)
	labelSelectors := make(map[string]string)
	namespaceMapping := make(map[string]string)
	newBackupLocationMap := make(map[string]string)
	backupNamespaceMap := make(map[string]string)
	deploymentBackupMap := make(map[string]string)
	bkpNamespaces = make([]string, 0)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("CustomResourceRestore", "Create custom resource restore", nil, 58041, Apimpalgaonkar, Q1FY24)
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
	It("Create custom resource restore", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
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
			err = CreateApplicationClusters(orgID, "", "", ctx)
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
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
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
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
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
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(newBackupLocationMap, credName, cloudCredUID, ctx)

	})
})

var _ = Describe("{AllNSBackupWithIncludeNewNSOption}", func() {
	var (
		scheduledAppContexts    []*scheduler.Context
		newScheduledAppContexts []*scheduler.Context
		cloudCredUID            string
		cloudCredName           string
		backupLocationName      string
		backupLocationUID       string
		backupLocationMap       map[string]string
		schedulePolicyName      string
		schedulePolicyUid       string
		scheduleName            string
		appNamespaces           []string
		newAppNamespaces        []string
		restoreName             string
		nextScheduleBackupName  string
		intervalInMins          int
		numDeployments          int
		ctx                     context.Context
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("AllNSBackupWithIncludeNewNSOption", "Verification of schedule backups created with include new namespaces option", nil, 84760, KPhalgun, Q1FY24)

		var err error
		ctx, err = backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		numDeployments = Inst().GlobalScaleFactor
		if len(Inst().AppList) == 1 && numDeployments < 2 {
			numDeployments = 2
		}
	})

	It("Validates schedule backups created with include new namespaces option includes newly created namespaces", func() {
		Step("Create cloud credentials and backup locations", func() {
			log.InfoD("Creating cloud credentials and backup locations")
			providers := getProviders()
			backupLocationMap = make(map[string]string)
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				log.InfoD("Creating cloud credential named [%s] and uid [%s] using [%s] as provider", cloudCredUID, cloudCredName, provider)
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				backupLocationName = fmt.Sprintf("%s-%s-bl-%v", provider, getGlobalBucketName(provider), time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				bucketName := getGlobalBucketName(provider)
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, bucketName, orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location named [%s] with uid [%s] of [%s] as provider", backupLocationName, backupLocationUID, provider))
			}
		})
		Step("Add source and destination clusters with px-central-admin ctx", func() {
			log.InfoD("Adding source and destination clusters with px-central-admin ctx")
			log.Infof("Creating source [%s] and destination [%s] clusters", SourceClusterName, destinationClusterName)
			err := CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))
			srcClusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err := Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.Infof("Cluster [%s] uid: [%s]", SourceClusterName, srcClusterUid)
			dstClusterStatus, err := Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(dstClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
			dstClusterUid, err := Inst().Backup.GetClusterUID(ctx, orgID, destinationClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", destinationClusterName))
			log.Infof("Cluster [%s] uid: [%s]", destinationClusterName, dstClusterUid)
		})
		Step("Create a schedule policy", func() {
			intervalInMins = 15
			log.InfoD("Creating a schedule policy with interval [%v] mins", intervalInMins)
			schedulePolicyName = fmt.Sprintf("interval-%v-%v", intervalInMins, time.Now().Unix())
			schedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, int64(intervalInMins), 5)
			err := Inst().Backup.BackupSchedulePolicy(schedulePolicyName, uuid.New(), orgID, schedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule policy [%s] with interval [%v] mins", schedulePolicyName, intervalInMins))
			schedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, schedulePolicyName)
			log.FailOnError(err, "Fetching uid of schedule policy [%s]", schedulePolicyName)
			log.Infof("Schedule policy [%s] uid: [%s]", schedulePolicyName, schedulePolicyUid)
		})
		Step("Schedule applications in destination cluster", func() {
			log.InfoD("Scheduling applications in destination cluster")
			err := SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			for i := 0; i < numDeployments; i++ {
				taskName := fmt.Sprintf("dst-%s-%d", taskNamePrefix, i)
				appContexts := ScheduleApplications(taskName)
				for index, ctx := range appContexts {
					appName := Inst().AppList[index]
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					log.InfoD("Scheduled application [%s] in destination cluster in namespace [%s]", appName, namespace)
					appNamespaces = append(appNamespaces, namespace)
					scheduledAppContexts = append(scheduledAppContexts, ctx)
				}
			}
		})
		Step("Validate app namespaces in destination cluster", func() {
			log.InfoD("Validating app namespaces in destination cluster")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Create schedule backup", func() {
			log.InfoD("Creating a schedule backup")
			scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
			namespaces := []string{"*"}
			labelSelectors := make(map[string]string)
			// not using CreateScheduleBackupWithValidation because list namespace is special character
			err := CreateScheduleBackup(scheduleName, destinationClusterName, backupLocationName, backupLocationUID, namespaces,
				labelSelectors, orgID, "", "", "", "", schedulePolicyName, schedulePolicyUid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule backup with schedule name [%s]", scheduleName))

			firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
			log.FailOnError(err, fmt.Sprintf("Fetching the name of the first schedule backup [%s]", firstScheduleBackupName))
			err = backupSuccessCheckWithValidation(ctx, firstScheduleBackupName, scheduledAppContexts, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Validation of the first schedule backup [%s]", firstScheduleBackupName))
		})
		// To ensure applications are deployed after a schedule backup is created
		Step("Schedule applications in destination cluster to create new namespaces", func() {
			log.InfoD("Scheduling applications in destination cluster to create new namespaces")
			for i := numDeployments; i < numDeployments+1; i++ {
				taskName := fmt.Sprintf("dst-%s-new-%d", taskNamePrefix, i)
				appContexts := ScheduleApplications(taskName)
				for index, ctx := range appContexts {
					appName := Inst().AppList[index]
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					log.InfoD("Scheduled application [%s] in destination cluster in namespace [%s]", appName, namespace)
					newAppNamespaces = append(newAppNamespaces, namespace)
					newScheduledAppContexts = append(newScheduledAppContexts, ctx)
				}
			}
		})
		Step("Validate new app namespaces in destination cluster", func() {
			log.InfoD("Validating new app namespaces in destination cluster")
			ValidateApplications(newScheduledAppContexts)
		})
		Step("Verify new application namespaces inclusion in next schedule backup", func() {
			log.InfoD("Verifying new application namespaces inclusion in next schedule backup")
			var err error
			allContexts := append(scheduledAppContexts, newScheduledAppContexts...)
			nextScheduleBackupName, err = GetNextCompletedScheduleBackupNameWithValidation(ctx, scheduleName, allContexts, time.Duration(intervalInMins))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Validation of next schedule backup [%s] of schedule [%s] after getting triggered and completing, and inclusion of new app namespaces [%v]", nextScheduleBackupName, scheduleName, newAppNamespaces))
		})
		Step("Restore new application namespaces from next schedule backup in source cluster", func() {
			log.InfoD("Restoring new application namespaces from next schedule backup in source cluster")
			namespaceMapping := make(map[string]string)
			// Modifying namespaceMapping to restore only new namespaces
			for _, namespace := range newAppNamespaces {
				namespaceMapping[namespace] = namespace + "-restored"
			}
			log.InfoD("Namespace mapping used for restoring - %v", namespaceMapping)
			restoreName = fmt.Sprintf("%s-%s", "test-restore", RandomString(4))
			err := CreateRestore(restoreName, nextScheduleBackupName, namespaceMapping, SourceClusterName, orgID, ctx, make(map[string]string))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
		})
	})

	JustAfterEach(func() {
		allContexts := append(scheduledAppContexts, newScheduledAppContexts...)
		defer EndPxBackupTorpedoTest(allContexts)
		defer func() {
			err := SetSourceKubeConfig()
			log.FailOnError(err, "failed to switch context to source cluster")
		}()

		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		allAppNamespaces := append(appNamespaces, newAppNamespaces...)
		log.InfoD("Deleting deployed namespaces - %v", allAppNamespaces)
		ValidateAndDestroy(allContexts, opts)

		err := SetSourceKubeConfig()
		log.FailOnError(err, "failed to switch context to source cluster")

		err = DeleteSchedule(scheduleName, destinationClusterName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		log.Infof("Deleting backup schedule policy")
		policyList := []string{schedulePolicyName}
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, policyList)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", policyList))
		err = DeleteRestore(restoreName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// BackupSyncBasicTest take a good number of backups check if backup sync is working
var _ = Describe("{BackupSyncBasicTest}", func() {
	numberOfBackups, _ := strconv.Atoi(getEnv(maxBackupsToBeCreated, "10"))
	timeBetweenConsecutiveBackups := 10 * time.Second
	backupNames := make([]string, 0)
	numberOfSimultaneousBackups := 20
	var (
		scheduledAppContexts     []*scheduler.Context
		customBucket             string
		backupLocationUID        string
		cloudCredUID             string
		backupName               string
		cloudCredUidList         []string
		customBackupLocationName string
		credName                 string
		clusterUid               string
		bkpNamespaces            []string
		clusterStatus            api.ClusterInfo_StatusInfo_Status
	)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	backupNamespaceMap := make(map[string]string)
	backupLocationMap := make(map[string]string)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("BackupSyncBasicTest",
			"Validate that the backup sync syncs all the backups present in bucket", nil, 58040, Kshithijiyer, Q1FY24)
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
	It("Validate that the backup sync syncs all the backups present in bucket", func() {
		providers := getProviders()
		Step("Validate applications and get their labels", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Deleting all admin backups at the start of the testcase", func() {
			log.InfoD("Deleting all admin backups at the start of the testcase")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = DeleteAllBackups(ctx, orgID)
			log.FailOnError(err, "Deleting all admin backups at the start of the testcase")
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
				customBucket = GetCustomBucketName(provider, "basicbackupsynctest")
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, customBucket, orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
				log.InfoD("Created Backup Location with name - %s", customBackupLocationName)
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
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
						appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
						err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
						dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
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
				err := CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, customBucket, orgID, "")
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
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
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
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)

		// Post test custom bucket delete
		providers := getProviders()
		for _, provider := range providers {
			DeleteBucket(provider, customBucket)
		}
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
		scheduledAppContexts        []*scheduler.Context
	)
	backupLocationMap := make(map[string]string)
	namespaceMapping := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("BackupMultipleNsWithSameLabel", "Taking backup and restoring multiple namespace having same labels", nil, 84851, Apimpalgaonkar, Q1FY24)
		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < 10; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
		log.InfoD("Created namespaces %v", bkpNamespaces)
	})
	It("Taking backup and restoring multiple namespace having same labels", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
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
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))
			appClusterName := SourceClusterName
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
			scheduledAppContextsExpectedToBeInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateBackupWithNamespaceLabelWithValidation(ctx, multipleNamespaceBackupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContextsExpectedToBeInBackup, nil, orgID, clusterUid, "", "", "", "", nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of namespace labelled backup [%s] of namespaces (scheduled contexts) [%v] with label [%s]", multipleNamespaceBackupName, bkpNamespaces, nsLabelString))
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
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		dash.VerifySafely(err, nil, "Fetching px-central-admin ctx")
		for _, restoreName := range restoreNames {
			err := DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the restore named [%s]", restoreName))
		}
		log.InfoD("Deleting labels from namespaces - %v", bkpNamespaces)
		err = DeleteLabelsFromMultipleNamespaces(nsLabelsMap, bkpNamespaces)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting labels [%v] from namespaces [%v]", nsLabelsMap, bkpNamespaces))
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed namespaces - %v", bkpNamespaces)
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// MultipleCustomRestoreSameTimeDiffStorageClassMapping issues multiple custom restores at the same time using different storage class mapping
var _ = Describe("{MultipleCustomRestoreSameTimeDiffStorageClassMapping}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
		bkpNamespaces        []string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		backupLocationUID    string
		cloudCredName        string
		cloudCredUID         string
		bkpLocationName      string
		backupName           string
		restoreList          []string
		sourceScName         *storageApi.StorageClass
		scNames              []string
		scCount              int
	)
	namespaceMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	storageClassMapping := make(map[string]string)
	k8sStorage := storage.Instance()
	params := make(map[string]string)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("MultipleCustomRestoreSameTimeDiffStorageClassMapping",
			"Issue multiple custom restores at the same time using different storage class mapping", nil, 58052, Sn, Q1FY24)
		log.InfoD("Deploy applications needed for backup")
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
	It("Issue multiple custom restores at the same time using different storage class mapping", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Register cluster for backup", func() {
			err = CreateApplicationClusters(orgID, "", "", ctx)
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
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
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
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
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

// AddMultipleNamespaceLabels add labels to multiple namespace, perform manual backup, schedule backup using namespace label selector and restore
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
		scheduledAppContexts       []*scheduler.Context
		scheduleRestoreMapping     map[string]string
		fetchedLabelMap            map[string]string
		labelMap                   map[string]string
		err                        error
	)
	bkpNamespaces = make([]string, 0)
	backupLocationMap := make(map[string]string)
	scheduleRestoreMapping = make(map[string]string)
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("AddMultipleNamespaceLabels", "Add multiple labels to namespaces, perform manual backup, schedule backup using namespace labels and restore", nil, 85583, Apimpalgaonkar, Q2FY24)
		log.InfoD("Deploy applications")
		batchSize = 10
		desiredNumLabels = 1000
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d-85583", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})
	It("Add multiple labels to namespaces, perform manual backup, schedule backup using namespace label and restore", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
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
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))
			appClusterName := SourceClusterName
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
			scheduledAppContextsExpectedToBeInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces[0:1])
			err = CreateBackupWithNamespaceLabelWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContextsExpectedToBeInBackup, nil, orgID, clusterUid, "", "", "", "", namespaceLabel)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of namespace labelled backup [%s] with label [%s]", backupName, namespaceLabel))

			err = NamespaceLabelBackupSuccessCheck(backupName, ctx, bkpNamespaces, namespaceLabel)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", bkpNamespaces, namespaceLabel, backupName))
		})
		Step("Create schedule policy", func() {
			log.InfoD("Creating a schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
			periodicSchedulePolicyUid = uuid.New()
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
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
			scheduledAppContextsExpectedToBeInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces[0:1])
			firstScheduleBackupName, err = CreateScheduleBackupWithNamespaceLabelWithValidation(ctx, scheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContextsExpectedToBeInBackup, nil, orgID, "", "", "", "", namespaceLabel, periodicSchedulePolicyName, periodicSchedulePolicyUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of namespace labelled schedule backup [%s] with label [%s]", scheduleName, namespaceLabel))

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
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Unable to px-central-admin ctx")
		err = DeleteSchedule(scheduleName, SourceClusterName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, []string{periodicSchedulePolicyName})
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", []string{periodicSchedulePolicyName}))
		for _, restoreName := range restoreNames {
			err := DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the restore named [%s]", restoreName))
		}
		log.InfoD("Deleting labels from namespaces - %v", bkpNamespaces)
		err = DeleteLabelsFromMultipleNamespaces(labelMap, bkpNamespaces)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting labels [%v] from namespaces [%v]", labelMap, bkpNamespaces))
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed namespaces - %v", bkpNamespaces)
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// MultipleInPlaceRestoreSameTime issues multiple in place restores at the same time
var _ = Describe("{MultipleInPlaceRestoreSameTime}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
		bkpNamespaces        []string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		backupLocationUID    string
		cloudCredName        string
		cloudCredUID         string
		bkpLocationName      string
		backupName           string
		restoreList          []string
	)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	backupNamespaceMapping := map[string]string{}
	sleepToGetUniqueTimeStamp := 2 * time.Second

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("MultipleInPlaceRestoreSameTime",
			"Issue multiple in-place restores at the same time", nil, 58051, Sn, Q1FY24)
		log.InfoD("Deploy applications needed for backup")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < 5; i++ {
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
	It("Issue multiple in-place restores at the same time", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Register cluster for backup", func() {
			err = CreateApplicationClusters(orgID, "", "", ctx)
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
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNameSpace})
					err := CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s] of namespace (scheduled Context) [%s]", backupName, bkpNameSpace))
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
					err = CreateRestoreWithReplacePolicy(restoreName, backupName, make(map[string]string), SourceClusterName, orgID, ctx, make(map[string]string), ReplacePolicyDelete)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Restoring backup %v into namespce %v with replacing existing resources", backupName, bkpNameSpace))
				}(bkpNameSpace, backupName)
			}
			wg.Wait()
		})
	})
	JustAfterEach(func() {
		var wg sync.WaitGroup
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
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
		scheduledAppContexts     []*scheduler.Context
		backupLocationUID        string
		cloudCredUID             string
		cloudCredUidList         []string
		bkpNamespaces            []string
		clusterUid               string
		clusterStatus            api.ClusterInfo_StatusInfo_Status
		customBackupLocationName string
		credName                 string
		customBucket             string
	)
	timeBetweenConsecutiveBackups := 10 * time.Second
	backupNames := make([]string, 0)
	numberOfSimultaneousBackups := 4
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	backupNamespaceMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	backupLocationMapNew := make(map[string]string)
	credMap := make(map[string]map[string]string)
	appContextsToBackupMap := make(map[string][]*scheduler.Context)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("CloudSnapsSafeWhenBackupLocationDeleteTest",
			"Validate if cloud snaps present if backup location is deleted", nil, 58069, Kshithijiyer, Q1FY24)
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
	It("Validate that the backup sync syncs all the backups present in bucket", func() {
		providers := getProviders()
		Step("Validate applications and get their labels", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Deleting all admin backups at the start of the testcase", func() {
			log.InfoD("Deleting all admin backups at the start of the testcase")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = DeleteAllBackups(ctx, orgID)
			log.FailOnError(err, "Deleting all admin backups at the start of the testcase")
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
				customBucket = GetCustomBucketName(provider, "cloudsnapssafewhenbackuplocationdelete")
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, customBucket, orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
				backupLocationMap[backupLocationUID] = customBackupLocationName
				// Check if custom bucket is empty or not
				result, err := IsBackupLocationEmpty(provider, customBucket)
				log.FailOnError(err, "Checking for errors while checking backup location")
				dash.VerifyFatal(result, true, "Checking if backup location is empty")
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
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

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			var sem = make(chan struct{}, numberOfSimultaneousBackups)
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.InfoD("Taking %d backups", numberOfBackups)
			var mutex sync.Mutex
			for backupLocationUID, backupLocationName := range backupLocationMap {
				for _, namespace := range bkpNamespaces {
					for i := 0; i < numberOfBackups; i++ {
						time.Sleep(timeBetweenConsecutiveBackups)
						backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
						backupNames = append(backupNames, backupName)
						sem <- struct{}{}
						wg.Add(1)
						go func(backupName, backupLocationName, backupLocationUID, namespace string) {
							defer GinkgoRecover()
							defer wg.Done()
							defer func() { <-sem }()
							appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
							mutex.Lock()
							appContextsToBackupMap[backupName] = appContextsToBackup
							mutex.Unlock()
							err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
							dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
						}(backupName, backupLocationName, backupLocationUID, namespace)
					}
				}
			}
			wg.Wait()
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
					err := CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, customBucket, orgID, "")
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
					backupName := fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					appContextsToBackupMap[backupName] = appContextsToBackup
					err := CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
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
				if len(fetchedBackupNames) >= len(backupNames) {
					return "", false, nil
				}
				return "", true, fmt.Errorf("expected: %d and actual: %d", len(backupNames), len(fetchedBackupNames))
			}
			_, err := DoRetryWithTimeoutWithGinkgoRecover(checkBackupSync, 100*time.Minute, 30*time.Second)
			log.FailOnError(err, "Wait for BackupSync to complete")
			fetchedBackupNames, err := GetAllBackupsAdmin()
			log.FailOnError(err, "Getting a list of all backups")
			log.InfoD(fmt.Sprintf("Expected backups %v", backupNames))
			log.InfoD(fmt.Sprintf("Fetched backups %v", fetchedBackupNames))
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
				err := backupSuccessCheckWithValidation(ctx, bkp.Name, appContextsToBackupMap[bkp.Name], orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of the backup [%s]", bkp.Name))
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMapNew, credName, cloudCredUID, ctx)

		// Post test custom bucket delete
		providers := getProviders()
		for _, provider := range providers {
			DeleteBucket(provider, customBucket)
		}
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
		scheduledAppContexts       []*scheduler.Context
	)
	backupLocationMap := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("SetUnsetNSLabelDuringScheduleBackup", "Create multiple namespaces and set unset namespace labels during the backup schedule", nil, 84849, Apimpalgaonkar, Q1FY24)
		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < 3; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
		log.InfoD("Created namespaces %v", bkpNamespaces)
	})
	It("Create multiple namespaces and set unset namespace labels", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
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
			err = CreateApplicationClusters(orgID, "", "", ctx)
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
			firstScheduleBackupName, err := CreateScheduleBackupWithNamespaceLabelWithValidation(ctx, scheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, nil, orgID, "", "", "", "", nsLabelString, periodicSchedulePolicyName, periodicSchedulePolicyUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of schedule backup with namespace labels, having schedule name [%s]", scheduleName))

			err = NamespaceLabelBackupSuccessCheck(firstScheduleBackupName, ctx, bkpNamespaces, nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", bkpNamespaces, nsLabelString, firstScheduleBackupName))

			log.InfoD("Waiting for 15 minutes for the next schedule backup to be triggered")
			time.Sleep(15 * time.Minute)
			secondScheduleBackupName, err := GetOrdinalScheduleBackupName(ctx, scheduleName, 2, orgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the second schedule backup [%s]", secondScheduleBackupName))
			err = backupSuccessCheckWithValidation(ctx, secondScheduleBackupName, scheduledAppContexts, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of second schedule backup named [%s] of schedule named [%s]", secondScheduleBackupName, scheduleName))
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

			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces[1:])
			nextScheduleBackupNameOne, err = GetNextCompletedScheduleBackupNameWithValidation(ctx, scheduleName, appContextsToBackup, 15)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Validation of schedule backup 1 [%s] of schedule [%s] after getting triggered and completing", nextScheduleBackupNameOne, scheduleName))
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

			nextScheduleBackupNameTwo, err = GetNextCompletedScheduleBackupNameWithValidation(ctx, scheduleName, scheduledAppContexts, 15)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Validation of schedule backup 2 [%s] of schedule [%s] after getting triggered and completing", nextScheduleBackupNameTwo, scheduleName))
			err = NamespaceLabelBackupSuccessCheck(nextScheduleBackupNameTwo, ctx, bkpNamespaces, nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespace [%v] is backed up and checks for labels [%s] applied to backup [%s]", bkpNamespaces, nsLabelString, nextScheduleBackupNameTwo))
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Unable to fetch px-central-admin ctx")
		log.InfoD("Deleting schedule named [%s] along with its backups [%v] and schedule policies [%v]", scheduleName, allScheduleBackupNames, []string{periodicSchedulePolicyName})
		err = DeleteSchedule(scheduleName, SourceClusterName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, []string{periodicSchedulePolicyName})
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", []string{periodicSchedulePolicyName}))
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed namespaces - %v", bkpNamespaces)
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// BackupRestoreOnDifferentK8sVersions Restores from a duplicate backup on a cluster with a different kubernetes version
var _ = Describe("{BackupRestoreOnDifferentK8sVersions}", func() {
	var (
		cloudCredUID         string
		cloudCredName        string
		backupLocationUID    string
		backupLocationName   string
		clusterUid           string
		appNamespaces        []string
		restoreNames         []string
		backupLocationMap    map[string]string
		srcVersion           semver.Version
		destVersion          semver.Version
		scheduledAppContexts []*scheduler.Context
		clusterStatus        api.ClusterInfo_StatusInfo_Status
	)
	namespaceMapping := make(map[string]string)
	duplicateBackupNameMap := make(map[string]string)
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("BackupRestoreOnDifferentK8sVersions", "Restoring from a duplicate backup on a cluster with a different kubernetes version", nil, 83721, Apimpalgaonkar, Q1FY24)
		log.InfoD("Scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				log.Infof("Scheduled application with namespace [%s]", namespace)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})
	It("Restoring from a duplicate backup on a cluster with a different kubernetes version", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Configure source and destination clusters with px-central-admin", func() {
			log.InfoD("Configuring source and destination clusters with px-central-admin")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
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
				backupLocationName = fmt.Sprintf("%s-%s-bl-%v", provider, getGlobalBucketName(provider), time.Now().Unix())
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
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, nil, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))

				duplicateBackupName := fmt.Sprintf("%s-duplicate-%v", BackupNamePrefix, time.Now().Unix())
				duplicateBackupNameMap[duplicateBackupName] = namespace
				err = CreateBackupWithValidation(ctx, duplicateBackupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, nil, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of duplicate backup [%s]", duplicateBackupName))
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
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
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
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// BackupCRsThenMultipleRestoresOnHigherK8sVersion deploys CRs via operator (CRD + webhook) -> backups them up -> creates two simultaneous restores on a cluster with higher K8s version :: one restore is Success and other PartialSuccess
var _ = Describe("{BackupCRsThenMultipleRestoresOnHigherK8sVersion}", func() {

	var (
		backupNames          []string
		restoreNames         []string
		restoreLaterNames    []string
		scheduledAppContexts []*scheduler.Context
		sourceClusterUid     string
		cloudCredName        string
		cloudCredUID         string
		backupLocationUID    string
		backupLocationName   string
	)

	var (
		originalAppList   = Inst().AppList
		namespaceMapping  = make(map[string]string)
		backupLocationMap = make(map[string]string)
		labelSelectors    = make(map[string]string)
		providers         = getProviders()
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("BackupCRsThenMultipleRestoresOnHigherK8sVersion", "Deploy CRs (CRD + webhook); then backup; create two simultaneous restores on cluster with higher K8s version; one restore is Success and other PartialSuccess", nil, 83716, Tthurlapati, Q2FY24)

		log.InfoD("specs (apps) allowed in execution of test: %v", appsWithCRDsAndWebhooks)
		Inst().AppList = appsWithCRDsAndWebhooks
	})

	It("Deploy CRs (CRD + webhook); Backup; two simultaneous Restores with one Success and other PartialSuccess. (Backup and Restore on different K8s version)", func() {

		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		}()

		Step("creating source and destination cluster", func() {
			log.InfoD("creating source and destination cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "create source and destination cluster")

			clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))

			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))

			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
		})

		Step("verify kubernetes version of source and destination cluster", func() {
			var srcVer, destVer semver.Version
			log.InfoD("begin verification kubernetes version of source and destination cluster")

			defer func() {
				log.InfoD("switching to default context")
				err := Inst().S.SetConfig("")
				log.FailOnError(err, "failed to SetClusterContext to default cluster")
			}()

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

			log.InfoD("source cluster version: %s ; destination cluster version: %s", srcVer.String(), destVer.String())
			isValid := srcVer.LT(destVer)
			dash.VerifyFatal(isValid, true,
				"source cluster kubernetes version should be lesser than the destination cluster kubernetes version.")
		})

		Step("schedule the applications on source cluster and validate", func() {
			log.InfoD("schedule the applications on source cluster and validate")

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

			log.InfoD("validating applications")
			ValidateApplications(scheduledAppContexts)

			log.InfoD("waiting (for 1 minutes) for any CRs to finish starting up.")
			time.Sleep(time.Minute * 1)
			log.Warnf("no verification is done; it might lead to undetectable errors.")
		})

		Step("Creating backup location and cloud credentials", func() {
			log.InfoD("Creating backup location and cloud setting")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				backupLocationName = fmt.Sprintf("%s-%s-bl-%v", provider, getGlobalBucketName(provider), time.Now().Unix())
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
			for _, appCtx := range scheduledAppContexts {
				scheduledNamespace := appCtx.ScheduleOptions.Namespace
				backupName := fmt.Sprintf("%s-%s-%v", BackupNamePrefix, scheduledNamespace, time.Now().Unix())
				log.InfoD("creating backup [%s] in source cluster [%s] (%s), organization [%s], of namespace [%s], in backup location [%s]", backupName, SourceClusterName, sourceClusterUid, orgID, scheduledNamespace, backupLocationName)
				err := CreateBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, []string{scheduledNamespace}, labelSelectors, orgID, sourceClusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", backupName))
				backupNames = append(backupNames, backupName)

				// Validation code will be added here in upcoming PR
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
							err := fmt.Errorf("restore status of [%s] is [%s]; expected [InProgress]", initialRestoreName, restoreResponseStatus.GetStatus())
							return "", false, err
						}

						err = fmt.Errorf("restore status of [%s] is [%s]; waiting for [InProgress]", initialRestoreName, restoreResponseStatus.GetStatus())
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
							err := fmt.Errorf("restore status of [%s] is [%s]; expected 'PartialSuccess' or 'Success'", restoreLaterName, restoreResponseStatus.GetStatus())
							return restoreResponseStatus.GetStatus(), false, err
						}

						err = fmt.Errorf("restore status of [%s] is [%s]; waiting for 'PartialSuccess' or 'Success'", restoreLaterName, restoreResponseStatus.GetStatus())
						return "", true, err
					}
					laterRestoreStatus, restoreLaterStatusErr = DoRetryWithTimeoutWithGinkgoRecover(restorePartialSuccessOrSuccessCheck, 10*time.Minute, 30*time.Second)

					// we don't end the test if there is an error here, as we also want to ensure that we look into the status of the following `Step`, so that we have the full details of what went wrong.
					dash.VerifySafely(restoreLaterStatusErr, nil, fmt.Sprintf("status of later restore [%s] is either 'PartialSuccess' or 'Success'", restoreLaterName))

					// We can consider validation and cleanup for 'PartialSuccess' and 'Success'
					if restoreLaterStatusErr == nil {
						// Validation code will be added here in upcoming PR
					} else {
						log.Warnf("proceeding to next step, after which the test will be failed.")
					}
				})

				Step("Verifying and validating status of Initial and Later Restore", func() {
					log.InfoD("Verifying status of Initial and Later Restore")
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
							err := fmt.Errorf("restore status of [%s] is [%s]; expected 'PartialSuccess' or 'Success'", initialRestoreName, restoreResponseStatus.GetStatus())
							return restoreResponseStatus.GetStatus(), false, err
						}

						err = fmt.Errorf("restore status of [%s] is [%s]; waiting for 'PartialSuccess' or 'Success'", initialRestoreName, restoreResponseStatus.GetStatus())
						return "", true, err
					}
					initialRestoreStatus, initialRestoreError := DoRetryWithTimeoutWithGinkgoRecover(restoreSuccessOrPartialSuccessCheck, 10*time.Minute, 30*time.Second)

					dash.VerifyFatal(initialRestoreError, nil, fmt.Sprintf("status of initial restore [%s] is 'PartialSuccess' or 'Success'", initialRestoreName))

					// Validation code will be added here in upcoming PR

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
		defer func() { Inst().AppList = originalAppList }()

		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		}()

		defer EndPxBackupTorpedoTest(scheduledAppContexts)

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "fetching px-central-admin ctx")

		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = false

		log.InfoD("deleting applications scheduled on source clusters")
		DestroyApps(scheduledAppContexts, opts)

		log.InfoD("waiting (for 1 minute) for any Resources created by Operator of Custom Resources to finish being destroyed.")
		time.Sleep(time.Minute * 1)
		log.Warn("no verification of destruction is done; it might lead to undetectable errors")

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

// ScheduleBackupDeleteAndRecreateNS Validates schedule backups when namespaces are deleted and recreated
var _ = Describe("{ScheduleBackupDeleteAndRecreateNS}", func() {
	var (
		scheduledAppContexts         []*scheduler.Context
		cloudCredUID                 string
		cloudCredName                string
		backupLocationName           string
		backupLocationUID            string
		backupLocationMap            map[string]string
		schedulePolicyName           string
		schedulePolicyUid            string
		scheduleName                 string
		latestScheduleBackupName     string
		restoreName                  string
		appNamespaces                []string
		backedUpNamespaces           []string
		schedulePolicyintervalInMins int
		numDeployments               int
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("ScheduleBackupDeleteAndRecreateNS", "Verification of schedule backups when namespaces are deleted and recreated", nil, 58037, Ak, Q2FY24)
		numDeployments = Inst().GlobalScaleFactor
		if len(Inst().AppList) == 1 && numDeployments < 2 {
			numDeployments = 2
		}
		log.InfoD("Scheduling applications")
		for i := 0; i < numDeployments; i++ {
			taskName := fmt.Sprintf("src-%s-%d", taskNamePrefix, i)
			namespace := fmt.Sprintf("test-namespace-%s", taskName)
			appContexts := ScheduleApplicationsOnNamespace(namespace, taskName)
			appNamespaces = append(appNamespaces, namespace)
			for index, ctx := range appContexts {
				appName := Inst().AppList[index]
				ctx.ReadinessTimeout = appReadinessTimeout
				log.InfoD("Scheduled application [%s] in source cluster in namespace [%s]", appName, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})

	It("Validates schedule backups when namespaces are deleted and recreated", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})
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
				backupLocationName = fmt.Sprintf("%s-%s-bl-%v", provider, getGlobalBucketName(provider), time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				bucketName := getGlobalBucketName(provider)
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, bucketName, orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location named [%s] with uid [%s] of [%s] as provider", backupLocationName, backupLocationUID, provider))
			}
		})
		Step("Add source and destination clusters with px-central-admin ctx", func() {
			log.InfoD("Adding source and destination clusters with px-central-admin ctx")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.Infof("Creating source [%s] and destination [%s] clusters", SourceClusterName, destinationClusterName)
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))
			srcClusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err := Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.Infof("Cluster [%s] uid: [%s]", SourceClusterName, srcClusterUid)
			dstClusterStatus, err := Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(dstClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
			dstClusterUid, err := Inst().Backup.GetClusterUID(ctx, orgID, destinationClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", destinationClusterName))
			log.Infof("Cluster [%s] uid: [%s]", destinationClusterName, dstClusterUid)
		})
		Step("Create a schedule policy", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schedulePolicyintervalInMins = 15
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
			_, err = CreateScheduleBackupWithValidation(ctx, scheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, orgID, "", "", "", "", schedulePolicyName, schedulePolicyUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of schedule backup with schedule name [%s]", scheduleName))
		})
		Step("Delete the App namespaces created", func() {
			log.InfoD("Delete the App namespaces created")
			for _, namespace := range appNamespaces {
				err := DeleteAppNamespace(namespace)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifiying the deletion of namespace [%s]", namespace))
			}
		})
		Step("Recreating the namespaces deleted with same names", func() {
			log.InfoD("Recreating the namespaces deleted with same names")
			scheduledAppContexts = scheduledAppContexts[:0] // clear out array; keep underlying allocation
			for i := 0; i < numDeployments; i++ {
				taskName := fmt.Sprintf("src-%s-%d", taskNamePrefix, i)
				namespace := fmt.Sprintf("test-namespace-%s", taskName)
				appContexts := ScheduleApplicationsOnNamespace(namespace, taskName)
				for index, ctx := range appContexts {
					appName := Inst().AppList[index]
					ctx.ReadinessTimeout = appReadinessTimeout
					log.InfoD("Scheduled application [%s] in source cluster in namespace [%s]", appName, namespace)
					scheduledAppContexts = append(scheduledAppContexts, ctx)
				}
			}
		})
		Step("Validate app namespaces recreated", func() {
			log.InfoD("Validating app namespaces recreated")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Verify inclusion of recreated application namespaces in next schedule backup", func() {
			log.InfoD("Verifying inclusion of recreated application namespaces in next schedule backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			latestScheduleBackupName, err = GetNextCompletedScheduleBackupNameWithValidation(ctx, scheduleName, scheduledAppContexts, time.Duration(schedulePolicyintervalInMins))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Validation of next schedule backup [%s] of schedule [%s] after getting triggered and completing, and inclusion of recreated app namespaces [%v]", latestScheduleBackupName, scheduleName, appNamespaces))
		})
		Step("Restoring the backed up applications from latest scheduled backup", func() {
			log.InfoD("Restoring the backed up applications from latest scheduled backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			namespaceMapping := make(map[string]string)
			for _, namespace := range backedUpNamespaces {
				namespaceMapping[namespace] = namespace + "-restored"
			}
			restoreName = fmt.Sprintf("%s-%s", "test-restore", RandomString(10))
			err = CreateRestore(restoreName, latestScheduleBackupName, namespaceMapping, destinationClusterName, orgID, ctx, make(map[string]string))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		err = DeleteSchedule(scheduleName, SourceClusterName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		log.Infof("Deleting backup schedule policy")
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, []string{schedulePolicyName})
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", []string{schedulePolicyName}))
		err = DeleteRestore(restoreName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// DeleteNSDeleteClusterRestore Validates deleted namespace is restored when the application cluster is removed and re-added
var _ = Describe("{DeleteNSDeleteClusterRestore}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
		cloudCredUID         string
		cloudCredName        string
		backupLocationName   string
		backupLocationUID    string
		backupLocationMap    map[string]string
		appNamespaces        []string
		numDeployments       int
		backupNames          []string
		srcClusterUid        string
		restoreNames         []string
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("DeleteNSDeleteClusterRestore", "Delete namespace from application cluster and delete cluster and add it back then restore for last backup", nil, 58061, Sn, Q2FY24)
		numDeployments = Inst().GlobalScaleFactor
		if len(Inst().AppList) == 1 && numDeployments < 2 {
			numDeployments = 2
		}
		log.InfoD("Scheduling applications")
		for i := 0; i < numDeployments; i++ {
			taskName := fmt.Sprintf("src-%s-%d", taskNamePrefix, i)
			namespace := fmt.Sprintf("test-namespace-%s", taskName)
			appContexts := ScheduleApplicationsOnNamespace(namespace, taskName)
			appNamespaces = append(appNamespaces, namespace)
			scheduledAppContexts = append(scheduledAppContexts, appContexts...)
			for index, ctx := range appContexts {
				appName := Inst().AppList[index]
				ctx.ReadinessTimeout = appReadinessTimeout
				log.InfoD("Scheduled application [%s] in source cluster in namespace [%s]", appName, namespace)
			}
		}
	})

	It("Validates deleted namespace is restored when the application cluster is removed and re-added", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})
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
				backupLocationName = fmt.Sprintf("%s-%s-bl-%v", provider, getGlobalBucketName(provider), time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				bucketName := getGlobalBucketName(provider)
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, bucketName, orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location named [%s] with uid [%s] of [%s] as provider", backupLocationName, backupLocationUID, provider))
			}
		})
		Step("Add source and destination clusters with px-central-admin ctx", func() {
			log.InfoD("Adding source and destination clusters with px-central-admin ctx")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.Infof("Creating source [%s] and destination [%s] clusters", SourceClusterName, destinationClusterName)
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))
			srcClusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.Infof("Cluster [%s] uid: [%s]", SourceClusterName, srcClusterUid)
			dstClusterStatus, err := Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(dstClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
			dstClusterUid, err := Inst().Backup.GetClusterUID(ctx, orgID, destinationClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", destinationClusterName))
			log.Infof("Cluster [%s] uid: [%s]", destinationClusterName, dstClusterUid)
		})
		Step("Taking backup of applications ", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for _, namespace := range appNamespaces {
				backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, nil, orgID, srcClusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup %s creation", backupName))
				backupNames = append(backupNames, backupName)
			}
		})
		Step("Delete the App namespaces created", func() {
			log.InfoD("Delete the App namespaces created")
			for _, namespace := range appNamespaces {
				err := DeleteAppNamespace(namespace)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifiying the deletion of namespace [%s]", namespace))
			}
		})
		Step("Delete source cluster where application is deployed", func() {
			log.InfoD("Delete source cluster where application is deployed")
			ctx, err := backup.GetAdminCtxFromSecret()
			err = DeleteCluster(SourceClusterName, orgID, ctx, false)
			Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting cluster %s", SourceClusterName))
		})
		Step("Add source cluster back with px-central-admin ctx", func() {
			log.InfoD("Adding source clusters with px-central-admin ctx")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.Infof("Creating source [%s] cluster", SourceClusterName)
			err = AddSourceCluster(ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] cluster with px-central-admin ctx", SourceClusterName))
			srcClusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err := Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.Infof("Cluster [%s] uid: [%s]", SourceClusterName, srcClusterUid)
		})
		Step("Restoring backup on source cluster", func() {
			log.InfoD("Restoring  backup on source cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for _, backupName := range backupNames {
				restoreName := fmt.Sprintf("%s-%s", "test-restore", RandomString(10))
				err = CreateRestore(restoreName, backupName, make(map[string]string), SourceClusterName, orgID, ctx, make(map[string]string))
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore from backup [%s]", restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		}
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// AlternateBackupBetweenNfsAndS3 Validates the type of backups(Full/Incremental) when alternate backups are taken between two different backup locations of NFS and S3
var _ = Describe("{AlternateBackupBetweenNfsAndS3}", func() {
	var (
		scheduledAppContexts     []*scheduler.Context
		sourceClusterUid         string
		backupLocationMap        map[string]string
		s3CloudCredName          string
		s3BackupLocationName     string
		s3CloudCredUID           string
		s3BackupLocationUID      string
		nfsBackupLocationName    string
		nfsBackupLocationUID     string
		bkpNamespaces            []string
		providers                []string
		labelSelectors           map[string]string
		backupNames              []string
		restoreNames             []string
		numberOfAlternateBackups = 2
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("AlternateBackupBetweenNfsAndS3", "To perform alternate backups between NFS and S3, and then perform the restore", nil, 86088, Sabrarhussaini, Q3FY24)
		backupLocationMap = make(map[string]string)
		labelSelectors = make(map[string]string)
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
		providers = getProviders()
	})

	It("To validate alternate backups between Nfs And S3", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Creating cloud setting for aws and backup locations for S3 and NFS", func() {
			log.InfoD("Creating cloud setting for aws and backup locations for S3 and NFS")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				log.InfoD("Creating NFS backup location")
				nfsBackupLocationName = fmt.Sprintf("%s-%s-%v", "nfs", getGlobalBucketName(drivers.ProviderNfs), RandomString(6))
				nfsBackupLocationUID = uuid.New()
				backupLocationMap[nfsBackupLocationUID] = nfsBackupLocationName
				err = CreateNFSBackupLocation(nfsBackupLocationName, nfsBackupLocationUID, orgID, " ", getGlobalBucketName(provider), true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of NFS backup location [%s]", nfsBackupLocationName))
				log.InfoD("Creating AWS cred and S3 backup location")
				s3CloudCredName = fmt.Sprintf("%s-%s-%v", "cred", "s3", RandomString(4))
				s3BackupLocationName = fmt.Sprintf("%s-%s-%v", "s3", getGlobalBucketName(provider), RandomString(4))
				s3CloudCredUID = uuid.New()
				s3BackupLocationUID = uuid.New()
				backupLocationMap[s3BackupLocationUID] = s3BackupLocationName
				if provider == drivers.ProviderNfs {
					err = CreateCloudCredential("aws", s3CloudCredName, s3CloudCredUID, orgID, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", s3CloudCredName, orgID, "AWS"))
				} else {
					err = CreateCloudCredential(provider, s3CloudCredName, s3CloudCredUID, orgID, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", s3CloudCredName, orgID, "AWS"))
				}
				err = CreateS3BackupLocation(s3BackupLocationName, s3BackupLocationUID, s3CloudCredName, s3CloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of S3 backup location [%s]", s3BackupLocationName))
			}
		})

		Step("Registering cluster for backup", func() {
			log.InfoD("Registering cluster for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			log.InfoD("Verifying cluster status for both source and destination clusters")
			clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
		})

		Step("Taking alternate backups of application from source cluster to both S3 and NFS backup locations", func() {
			log.InfoD("Taking alternate backups of application from source cluster to both S3 and NFS backup locations")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			for i := 0; i < numberOfAlternateBackups; i++ {
				for locationUID, locationName := range backupLocationMap {
					log.InfoD("Creating backup using the backup location of [%s]", locationName)
					backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, RandomString(10))
					backupNames = append(backupNames, backupName)
					err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, locationName, locationUID, appContextsToBackup, labelSelectors, orgID, sourceClusterUid, "", "", "", "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
					log.InfoD("Verifying the type of backup")
					//First backup for each backup location must be a full backup, rest should be incremental.
					if i == 0 {
						err = IsFullBackup(backupName, orgID, ctx)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if backup [%s] is a full backup", backupName))
					}
				}
			}
			log.Infof("List of backups - %v", backupNames)
		})

		Step("Restoring backups on destination cluster", func() {
			log.InfoD("Restoring backups on destination cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for _, backupName := range backupNames {
				restoreName := fmt.Sprintf("%s-%v", restoreNamePrefix, RandomString(10))
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, make(map[string]string), make(map[string]string), destinationClusterName, orgID, scheduledAppContexts)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s] from backup [%s]", restoreName, backupName))
				restoreNames = append(restoreNames, restoreName)
			}
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the restores")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		}
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Deleting the px-backup objects")
		CleanupCloudSettingsAndClusters(backupLocationMap, s3CloudCredName, s3CloudCredUID, ctx)
		log.InfoD("Switching context to destination cluster for clean up")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Unable to switch context to destination cluster [%s]", destinationClusterName)
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Switching back context to Source cluster")
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Unable to switch context to source cluster [%s]", SourceClusterName)
	})
})

// BackupNamespaceInNfsRestoredFromS3 take a backup of namespace in NFS which is restored from s3 bucket or vice-versa
var _ = Describe("{BackupNamespaceInNfsRestoredFromS3}", func() {
	var (
		s3CloudCredName                     string
		s3CloudCredUID                      string
		firstBkpLocationName                string
		firstBackupLocationUID              string
		secondBackupLocationName            string
		secondBackupLocationUID             string
		sourceClusterUid                    string
		firstBackupName                     string
		secondBackupName                    string
		providers                           []string
		restoreList                         []string
		appNamespaces                       []string
		sourceClusterRestoredNamespace      []string
		destinationClusterRestoredNamespace []string
		scheduledAppContexts                []*scheduler.Context
		contexts                            []*scheduler.Context
		appContexts                         []*scheduler.Context
	)
	backupLocationMap := make(map[string]string)
	namespaceMapping := make(map[string]string)
	sourceClusterNamespaceMapping := make(map[string]string)
	destinationClusterNamespaceMapping := make(map[string]string)
	restoredAppContexts := make([]*scheduler.Context, 0)
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("BackupNamespaceInNfsRestoredFromS3", "Take a backup of namespace in NFS which is restored from s3 bucket or vice-versa", nil, 86089, Sagrawal, Q3FY24)
		log.InfoD("Scheduling Applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < 5; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
		log.Infof("The list of namespaces deployed are", appNamespaces)
		providers = getProviders()
	})

	It("Take a backup of namespace in NFS which is restored from s3 bucket or vice-versa", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Adding cloud credential and backup locations", func() {
			log.InfoD("Adding cloud credential and backup locations")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				s3CloudCredName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, RandomString(5))
				s3CloudCredUID = uuid.New()
				firstBkpLocationName = fmt.Sprintf("%s-%s-%v-bl", provider, getGlobalBucketName(provider), RandomString(5))
				firstBackupLocationUID = uuid.New()
				err = CreateCloudCredential(provider, s3CloudCredName, s3CloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential:[%s] for org [%s] for %s backup location", s3CloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, firstBkpLocationName, firstBackupLocationUID, s3CloudCredName, s3CloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating %s backup location: %s", provider, firstBkpLocationName))
				backupLocationMap[firstBackupLocationUID] = firstBkpLocationName
				if provider != drivers.ProviderNfs {
					secondBackupLocationName = fmt.Sprintf("%s-%s-%v", "nfs", getGlobalBucketName(provider), RandomString(5))
					secondBackupLocationUID = uuid.New()
					err = CreateNFSBackupLocation(secondBackupLocationName, secondBackupLocationUID, orgID, "", getGlobalBucketName(provider), true)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating NFS backup location [%s]", secondBackupLocationName))
					backupLocationMap[secondBackupLocationUID] = secondBackupLocationName
				} else {
					// Creating cloud cred again because in case of NFS as provider, cloud cred will not be created above
					err = CreateCloudCredential("aws", s3CloudCredName, s3CloudCredUID, orgID, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential:[%s] for org [%s] for s3 backup location", s3CloudCredName, orgID))
					secondBackupLocationName = fmt.Sprintf("%s-%s-%v", "s3", getGlobalBucketName(provider), RandomString(5))
					secondBackupLocationUID = uuid.New()
					err = CreateS3BackupLocation(secondBackupLocationName, secondBackupLocationUID, s3CloudCredName, s3CloudCredUID, getGlobalBucketName(provider), orgID, "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of S3 backup location [%s]", secondBackupLocationName))
					backupLocationMap[secondBackupLocationUID] = secondBackupLocationName
				}
			}
		})

		Step("Registering application clusters for backup", func() {
			log.InfoD("Registering application clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			log.InfoD("Verifying cluster status for both source and destination clusters")
			clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
		})

		Step("Taking backup of applications for the first backup location", func() {
			log.InfoD("Taking backup of applications for the first backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			firstBackupName = fmt.Sprintf("first-%s-%v-%v", BackupNamePrefix, RandomString(5), providers[0])
			err = CreateBackupWithValidation(ctx, firstBackupName, SourceClusterName, firstBkpLocationName, firstBackupLocationUID, scheduledAppContexts, nil, orgID, sourceClusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup of application to %v backup location", providers[0]))
		})

		Step(fmt.Sprintf("Restoring the first backup taken with %s backup location to a new namespace on source cluster", providers[0]), func() {
			log.InfoD("Restoring the first backup taken with %s backup location to a new namespace on source cluster", providers[0])
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, app := range appNamespaces {
				restoreNamespace := fmt.Sprintf("res-%v-%v", app, RandomString(5))
				sourceClusterRestoredNamespace = append(sourceClusterRestoredNamespace, restoreNamespace)
				sourceClusterNamespaceMapping[app] = restoreNamespace
			}
			restoreName := fmt.Sprintf("first-%s-%v-%v", restoreNamePrefix, RandomString(5), providers[0])
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithValidation(ctx, restoreName, firstBackupName, sourceClusterNamespaceMapping, make(map[string]string), SourceClusterName, orgID, scheduledAppContexts)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating  restore: %s from backup: %s", restoreName, firstBackupName))
		})

		Step(fmt.Sprintf("Taking backup of restored applications to %v backup location", secondBackupLocationName), func() {
			log.InfoD("Taking backup of restored applications to %v backup location", secondBackupLocationName)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, scheduledAppContext := range scheduledAppContexts {
				namespaceMapping[scheduledAppContext.ScheduleOptions.Namespace] = sourceClusterNamespaceMapping[scheduledAppContext.ScheduleOptions.Namespace]
				restoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, namespaceMapping, make(map[string]string), true)
				if err != nil {
					log.FailOnError(err, "cloning restored app context")
				}
				restoredAppContexts = append(restoredAppContexts, restoredAppContext)
			}
			secondBackupName = fmt.Sprintf("second-%s-%v", BackupNamePrefix, RandomString(5))
			err = CreateBackupWithValidation(ctx, secondBackupName, SourceClusterName, secondBackupLocationName, secondBackupLocationUID, restoredAppContexts, nil, orgID, sourceClusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup of application to %v backup location", secondBackupLocationName))
		})

		Step(fmt.Sprintf("Restoring the second backup taken to %s backup location to a new namespace on destination cluster", secondBackupLocationName), func() {
			log.InfoD("Restoring the second backup taken to %s backup location to a new namespace on destination cluster", secondBackupLocationName)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, app := range sourceClusterRestoredNamespace {
				restoreNamespace := fmt.Sprintf("dest-%v", app)
				destinationClusterRestoredNamespace = append(destinationClusterRestoredNamespace, restoreNamespace)
				destinationClusterNamespaceMapping[app] = restoreNamespace
			}
			restoreName := fmt.Sprintf("second-%s-%v-%v", restoreNamePrefix, RandomString(5), providers[0])
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithValidation(ctx, restoreName, secondBackupName, destinationClusterNamespaceMapping, make(map[string]string), destinationClusterName, orgID, restoredAppContexts)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore: %s from backup: %s", restoreName, secondBackupName))
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		defer func() {
			err := SetSourceKubeConfig()
			log.FailOnError(err, "failed to switch context to source cluster")
		}()
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the restores")
		for _, restoreName := range restoreList {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		}
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		log.Infof("Deleting restored namespace from source cluster")
		for _, ns := range sourceClusterRestoredNamespace {
			err = DeleteAppNamespace(ns)
			log.FailOnError(err, "Deletion of namespace %s from source cluster failed", ns)
		}
		log.InfoD("Deleting the px-backup objects")
		CleanupCloudSettingsAndClusters(backupLocationMap, s3CloudCredName, s3CloudCredUID, ctx)
		log.InfoD("Switching context to destination cluster for clean up")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Unable to switch context to destination cluster [%s]", destinationClusterName)
		log.Infof("Deleting restored namespace from destination cluster")
		for _, ns := range destinationClusterRestoredNamespace {
			err = DeleteAppNamespace(ns)
			log.FailOnError(err, "Deletion of namespace %s from destination cluster failed", ns)
		}
	})
})

// DeleteS3ScheduleAndCreateNfsSchedule deletes s3 schedule and starts NFS schedule or vice-versa
var _ = Describe("{DeleteS3ScheduleAndCreateNfsSchedule}", func() {
	var (
		s3CloudCredName          string
		s3CloudCredUID           string
		firstBkpLocationName     string
		firstBackupLocationUID   string
		secondBackupLocationName string
		secondBackupLocationUID  string
		schedulePolicyName       string
		schedulePolicyUID        string
		secondScheduleName       string
		firstScheduleName        string
		firstSchBackupName       string
		scheduleUid              string
		srcClusterUid            string
		providers                []string
		appNamespaces            []string
		scheduledAppContexts     []*scheduler.Context
		contexts                 []*scheduler.Context
		appContexts              []*scheduler.Context
		schedulePolicyInterval   = int64(15)
	)
	backupLocationMap := make(map[string]string)
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("DeleteS3ScheduleAndCreateNfsSchedule", "Take a schedule backup of namespace in S3 backup location,delete the s3 schedule, create new schedule backup with NFS backup location or vice-versa", nil, 86099, Sagrawal, Q3FY24)
		log.InfoD("Scheduling Applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < 5; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
		log.Infof("The list of namespaces deployed are", appNamespaces)
		providers = getProviders()
	})

	It("Take a schedule backup of namespace in S3 backup location, delete the s3 schedule, create new schedule backup with NFS backup location or vice-versa", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Adding cloud credential and backup locations", func() {
			log.InfoD("Adding cloud credential and backup locations")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				s3CloudCredName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, RandomString(5))
				s3CloudCredUID = uuid.New()
				firstBkpLocationName = fmt.Sprintf("%s-%s-%v-bl", provider, getGlobalBucketName(provider), RandomString(5))
				firstBackupLocationUID = uuid.New()
				err = CreateCloudCredential(provider, s3CloudCredName, s3CloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential:[%s] for org [%s] for %s backup location", s3CloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, firstBkpLocationName, firstBackupLocationUID, s3CloudCredName, s3CloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating %s backup location: %s", provider, firstBkpLocationName))
				backupLocationMap[firstBackupLocationUID] = firstBkpLocationName
				if provider != drivers.ProviderNfs {
					secondBackupLocationName = fmt.Sprintf("%s-%s-%v", "nfs", getGlobalBucketName(provider), RandomString(5))
					secondBackupLocationUID = uuid.New()
					err = CreateNFSBackupLocation(secondBackupLocationName, secondBackupLocationUID, orgID, "", getGlobalBucketName(provider), true)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating NFS backup location [%s]", secondBackupLocationName))
					backupLocationMap[secondBackupLocationUID] = secondBackupLocationName
				} else {
					// Creating cloud cred again because in case of NFS as provider, cloud cred will not be created above
					err = CreateCloudCredential("aws", s3CloudCredName, s3CloudCredUID, orgID, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential:[%s] for org [%s] for s3 backup location", s3CloudCredName, orgID))
					secondBackupLocationName = fmt.Sprintf("%s-%s-%v", "s3", getGlobalBucketName(provider), RandomString(5))
					secondBackupLocationUID = uuid.New()
					err = CreateS3BackupLocation(secondBackupLocationName, secondBackupLocationUID, s3CloudCredName, s3CloudCredUID, getGlobalBucketName(provider), orgID, "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of S3 backup location [%s]", secondBackupLocationName))
					backupLocationMap[secondBackupLocationUID] = secondBackupLocationName
				}
			}
		})

		Step("Registering application clusters for backup", func() {
			log.InfoD("Registering application clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			log.InfoD("Verifying cluster status for both source and destination clusters")
			clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
		})

		Step("Create schedule policy", func() {
			log.InfoD("Creating schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schedulePolicyName = fmt.Sprintf("%s-%v", "periodic-schedule-policy", RandomString(5))
			schedulePolicyUID = uuid.New()
			err = CreateBackupScheduleIntervalPolicy(5, schedulePolicyInterval, 5, schedulePolicyName, schedulePolicyUID, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule policy %s", schedulePolicyName))
		})

		Step(fmt.Sprintf("Creating schedule backup for %s backup location and deleting it", firstBkpLocationName), func() {
			log.InfoD("Creating schedule backup for %s backup location and deleting it", firstBkpLocationName)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			firstScheduleName = fmt.Sprintf("first-schedule-%v", RandomString(5))
			firstSchBackupName, err = CreateScheduleBackupWithValidation(ctx, firstScheduleName, SourceClusterName, firstBkpLocationName, firstBackupLocationUID, scheduledAppContexts, make(map[string]string), orgID, "", "", "", "", schedulePolicyName, schedulePolicyUID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of scheduled backup with schedule name [%s] for backup location %s", firstScheduleName, firstBkpLocationName))
			err = IsFullBackup(firstSchBackupName, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the first schedule backup [%s] for backup location %s is a full backup", firstSchBackupName, firstBkpLocationName))
			_, err = GetNextPeriodicScheduleBackupName(firstScheduleName, 15, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the next schedule backup for schedule: [%s] for backup location %s", firstScheduleName, firstBkpLocationName))
			scheduleUid, err = Inst().Backup.GetBackupScheduleUID(ctx, firstScheduleName, orgID)
			log.FailOnError(err, "failed to fetch backup schedule: %s uid", firstScheduleName)
			err = DeleteScheduleWithUIDAndWait(firstScheduleName, scheduleUid, SourceClusterName, srcClusterUid, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting schedule %s for backup location %s", firstScheduleName, firstBkpLocationName))
		})

		Step(fmt.Sprintf("Creating second schedule backup for %s backup location and deleting it", secondBackupLocationName), func() {
			log.InfoD("Creating second schedule backup for %s backup location and deleting it", secondBackupLocationName)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			secondScheduleName = fmt.Sprintf("second-schedule-%v", RandomString(5))
			firstSchBackupName, err = CreateScheduleBackupWithValidation(ctx, secondScheduleName, SourceClusterName, secondBackupLocationName, secondBackupLocationUID, scheduledAppContexts, make(map[string]string), orgID, "", "", "", "", schedulePolicyName, schedulePolicyUID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of scheduled backup with schedule name [%s] for backup location %s", secondScheduleName, secondBackupLocationName))
			err = IsFullBackup(firstSchBackupName, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the first schedule backup [%s] for backup location %s is a full backup", firstSchBackupName, secondBackupLocationName))
			_, err = GetNextPeriodicScheduleBackupName(secondScheduleName, 15, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the next schedule backup for schedule: [%s] for backup location %s", secondScheduleName, secondBackupLocationName))
			scheduleUid, err = Inst().Backup.GetBackupScheduleUID(ctx, secondScheduleName, orgID)
			log.FailOnError(err, "failed to fetch backup schedule: %s uid", secondScheduleName)
			err = DeleteScheduleWithUIDAndWait(secondScheduleName, scheduleUid, SourceClusterName, srcClusterUid, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting schedule %s for backup location %s", secondScheduleName, secondBackupLocationName))
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		err = DeleteBackupSchedulePolicyWithContext(orgID, []string{schedulePolicyName}, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of schedule policy [%s]", schedulePolicyName))
		log.InfoD("Deleting the px-backup objects")
		CleanupCloudSettingsAndClusters(backupLocationMap, s3CloudCredName, s3CloudCredUID, ctx)
	})
})

// KubeAndPxNamespacesSkipOnAllNSBackup check if namespaces like kube-system and px namespace
// are backed up while taking a backup
var _ = Describe("{KubeAndPxNamespacesSkipOnAllNSBackup}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
		cloudCredUID         string
		cloudCredName        string
		backupLocationName   string
		backupLocationUID    string
		backupLocationMap    map[string]string
		schedulePolicyName   string
		schedulePolicyUid    string
		scheduleName         string
		appNamespaces        []string
		backupNames          []string
		restoreNames         []string
		restoreName          string
		intervalInMins       int
		numDeployments       int
		ctx                  context.Context
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("KubeAndPxNamespacesSkipOnAllNSBackup", "Verify if kube-system, kube-node-lease, kube-public and Px Namespace is skipped on all namespace backup", nil, 92858, Ak, Q3FY24)

		var err error
		ctx, err = backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		numDeployments = Inst().GlobalScaleFactor
		if len(Inst().AppList) == 1 && numDeployments < 3 {
			numDeployments = 3
		}
	})

	It("Verify if kube-system, kube-node-lease, kube-public and Px Namespace is skipped on all namespace backup", func() {

		Step("Schedule applications in destination cluster", func() {
			log.InfoD("Scheduling applications in destination cluster")
			err := SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			for i := 0; i < numDeployments; i++ {
				taskName := fmt.Sprintf("dst-%s-%d", taskNamePrefix, i)
				appContexts := ScheduleApplications(taskName)
				for index, ctx := range appContexts {
					appName := Inst().AppList[index]
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					log.InfoD("Scheduled application [%s] in destination cluster in namespace [%s]", appName, namespace)
					appNamespaces = append(appNamespaces, namespace)
					scheduledAppContexts = append(scheduledAppContexts, ctx)
				}
			}
		})

		Step("Validate app namespaces in destination cluster", func() {
			ValidateApplications(scheduledAppContexts)
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
		})

		Step("Create cloud credentials and backup locations", func() {
			log.InfoD("Creating cloud credentials and backup locations")
			providers := getProviders()
			backupLocationMap = make(map[string]string)
			for _, provider := range providers {

				cloudCredUID = uuid.New()
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())

				log.InfoD("Creating cloud credential named [%s] and uid [%s] using [%s] as provider", cloudCredUID, cloudCredName, provider)
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				backupLocationName = fmt.Sprintf("%s-%s-bl-%v", provider, getGlobalBucketName(provider), time.Now().Unix())

				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				bucketName := getGlobalBucketName(provider)
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, bucketName, orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location named [%s] with uid [%s] of [%s] as provider", backupLocationName, backupLocationUID, provider))
			}
		})
		Step("Add source and destination clusters with px-central-admin ctx", func() {
			log.InfoD("Adding source and destination clusters with px-central-admin ctx")
			log.Infof("Creating source [%s] and destination [%s] clusters", SourceClusterName, destinationClusterName)

			err := CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))

			srcClusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))

			srcClusterUid, err := Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.Infof("Cluster [%s] uid: [%s]", SourceClusterName, srcClusterUid)

			dstClusterStatus, err := Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(dstClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))

			dstClusterUid, err := Inst().Backup.GetClusterUID(ctx, orgID, destinationClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", destinationClusterName))
			log.Infof("Cluster [%s] uid: [%s]", destinationClusterName, dstClusterUid)
		})
		Step("Create a schedule policy", func() {
			intervalInMins = 15
			log.InfoD("Creating a schedule policy with interval [%v] mins", intervalInMins)
			schedulePolicyName = fmt.Sprintf("interval-%v-%v", intervalInMins, time.Now().Unix())
			schedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(7, int64(intervalInMins), 6)

			err := Inst().Backup.BackupSchedulePolicy(schedulePolicyName, uuid.New(), orgID, schedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule policy [%s] with interval [%v] mins", schedulePolicyName, intervalInMins))

			schedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, schedulePolicyName)
			log.FailOnError(err, "Fetching uid of schedule policy [%s]", schedulePolicyName)
			log.Infof("Schedule policy [%s] uid: [%s]", schedulePolicyName, schedulePolicyUid)
		})

		Step("Create a manual backup", func() {
			log.InfoD("Creating a manual backup")
			backupName := fmt.Sprintf("%s-all-%v", BackupNamePrefix, time.Now().Unix())

			namespaces := []string{"*"}
			labelSelectors := make(map[string]string)
			destinationClusterUid, err := Inst().Backup.GetClusterUID(ctx, orgID, destinationClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", destinationClusterName))

			err = CreateBackup(backupName, destinationClusterName, backupLocationName, backupLocationUID, namespaces, labelSelectors, orgID, destinationClusterUid, "", "", "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", backupName))
			backupNames = append(backupNames, backupName)

			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")

			err = backupSuccessCheckWithValidation(ctx, backupName, scheduledAppContexts, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Validation of the manual backup [%s]", backupName))

			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
		})

		Step("Check if kube-system and px namespace was backed up or not", func() {
			err := checkBackupObjectForUnexpectedNS(ctx, backupNames[0])
			dash.VerifyFatal(err, nil, "Checking backup objects for namespaces")
		})

		Step("Restore manual backup and validate status post restore", func() {
			log.InfoD("Restoring new application namespaces from next schedule backup in source cluster")
			oldPodAge, err := getPodAge()
			dash.VerifyFatal(err, nil, "Getting namespace age")
			restoreName = fmt.Sprintf("%s-%s", "test-restore-manual", RandomString(4))
			err = CreateRestoreWithReplacePolicy(restoreName, backupNames[0], make(map[string]string), destinationClusterName, orgID, ctx, make(map[string]string), 2)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
			restoreNames = append(restoreNames, restoreName)

			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")

			ValidateApplications(scheduledAppContexts)

			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")

			err = comparePodAge(oldPodAge)
			dash.VerifyFatal(err, nil, "Comparing namespace age namespace age")
		})

		Step("Create schedule backup", func() {
			log.InfoD("Creating a schedule backup")
			scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
			namespaces := []string{"*"}
			labelSelectors := make(map[string]string)
			err := CreateScheduleBackup(scheduleName, destinationClusterName, backupLocationName, backupLocationUID, namespaces,
				labelSelectors, orgID, "", "", "", "", schedulePolicyName, schedulePolicyUid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule backup with schedule name [%s]", scheduleName))

			firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
			log.FailOnError(err, fmt.Sprintf("Fetching the name of the first schedule backup [%s]", firstScheduleBackupName))

			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")

			err = backupSuccessCheckWithValidation(ctx, firstScheduleBackupName, scheduledAppContexts, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Validation of the first schedule backup [%s]", firstScheduleBackupName))

			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
		})

		Step("Check if kube-system and px namespace was backed up or not", func() {
			firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
			log.FailOnError(err, fmt.Sprintf("Fetching the name of the first schedule backup [%s]", firstScheduleBackupName))
			err = checkBackupObjectForUnexpectedNS(ctx, firstScheduleBackupName)
			dash.VerifyFatal(err, nil, "Checking backup objects for namespaces")
		})

		Step("Restore schedule backup and validate post restpre", func() {
			log.InfoD("Restore schedule backup")
			oldPodAge, err := getPodAge()
			dash.VerifyFatal(err, nil, "Getting namespace age")

			restoreName = fmt.Sprintf("%s-%s", "test-restore", RandomString(4))
			firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
			log.FailOnError(err, fmt.Sprintf("Fetching the name of the first schedule backup [%s]", firstScheduleBackupName))

			err = CreateRestoreWithReplacePolicy(restoreName, firstScheduleBackupName, make(map[string]string), destinationClusterName, orgID, ctx, make(map[string]string), 2)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
			restoreNames = append(restoreNames, restoreName)

			ValidateApplications(scheduledAppContexts)
			err = comparePodAge(oldPodAge)
			dash.VerifyFatal(err, nil, "Comparing namespace age")
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		defer func() {
			err := SetSourceKubeConfig()
			log.FailOnError(err, "failed to switch context to source cluster")
		}()

		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed namespaces - %v", appNamespaces)
		ValidateAndDestroy(scheduledAppContexts, opts)

		err := SetSourceKubeConfig()
		log.FailOnError(err, "failed to switch context to source cluster")

		err = DeleteSchedule(scheduleName, destinationClusterName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		log.Infof("Deleting backup schedule policy")

		policyList := []string{schedulePolicyName}
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, policyList)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", policyList))

		for _, restoreName = range restoreNames {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		}

		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})
