package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/rke"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	_ "github.com/rancher/norman/clientbase"
	_ "github.com/rancher/rancher/pkg/client/generated/management/v3"
)

// This testcase takes backup of single namespace and restore to namespace in same and different project
var _ = Describe("{SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject}", func() {

	var (
		credName                    string
		credUid                     string
		customBackupLocationName    string
		backupLocationUID           string
		sourceClusterUid            string
		backupName                  string
		appNamespaces               []string
		restoreNamespacesAll        []string
		restoreList                 []string
		sourceClusterProjectList    []string
		sourceClusterProjectUIDList []string
		destClusterProjectList      []string
		destClusterProjectUIDList   []string
		destRestoreNamespacesAll    []string
		contexts                    []*scheduler.Context
		appContexts                 []*scheduler.Context
		scheduledAppContexts        []*scheduler.Context
	)
	backupLocationMap := make(map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject",
			"Take backup of single namespace and restore to namespace in same and different project", nil, 84872)
		log.InfoD("Deploying applications required for the testcase")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
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
	})

	It("Take backup of single namespace and restore to namespace in same and different project", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			backupLocationProviders := getProviders()
			for _, provider := range backupLocationProviders {
				credName = fmt.Sprintf("%s-cred-%v", provider, RandomString(10))
				credUid = uuid.New()
				err := CreateCloudCredential(provider, credName, credUid, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s]  as provider %s", credName, orgID, provider))
				customBackupLocationName = fmt.Sprintf("%s-backup-location-%v", provider, RandomString(10))
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = customBackupLocationName
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, credUid, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
			}
		})

		Step("Registering application clusters for backup", func() {
			log.InfoD("Registering application clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating source and destination rancher project in source cluster", func() {
			log.InfoD("Creating source and destination rancher project in source cluster")
			for i := 0; i < 2; i++ {
				project := fmt.Sprintf("rke-project-%v", RandomString(10))
				_, err = Inst().S.(*rke.Rancher).CreateRancherProject(project, "new project", "local")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating rancher project %s", project))
				projectID, err := Inst().S.(*rke.Rancher).GetProjectID(project)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Getting Project ID for project %s", project))
				sourceClusterProjectList = append(sourceClusterProjectList, project)
				sourceClusterProjectUIDList = append(sourceClusterProjectUIDList, projectID)
			}
		})

		Step("Adding source namespaces to source project and taking backup", func() {
			log.InfoD("Adding source namespaces to source project and taking backup")
			err = Inst().S.(*rke.Rancher).AddNamespacesToProject(sourceClusterProjectList[0], appNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding namespaces %s to project %s", appNamespaces, sourceClusterProjectList[0]))
			err = Inst().S.(*rke.Rancher).ValidateProjectOfNamespaces(sourceClusterProjectList[0], appNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying project %s of namespace %s", sourceClusterProjectList[0], appNamespaces))
			log.InfoD("Taking Backup of application")
			for _, namespace := range appNamespaces {
				backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, RandomString(10))
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, nil, orgID, sourceClusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
			}
		})

		Step("Restoring to same project but different namespace in same cluster", func() {
			log.InfoD("Restoring to same project but different namespace in same cluster")
			var restoredNamespaceList []string
			projectNameMapping := make(map[string]string)
			projectUIDMapping := make(map[string]string)
			namespaceMapping := make(map[string]string)
			log.InfoD("Restoring to same project but different namespace in same cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				restoredNamespace := "restore-to-same-pro-diff-ns" + RandomString(5)
				namespaceMapping[namespace] = restoredNamespace
				restoreNamespacesAll = append(restoreNamespacesAll, restoredNamespace)
				restoredNamespaceList = append(restoredNamespaceList, restoredNamespace)
			}
			projectNameMapping[sourceClusterProjectList[0]] = sourceClusterProjectList[0]
			projectUIDMapping[sourceClusterProjectUIDList[0]] = sourceClusterProjectUIDList[0]
			restoreName := fmt.Sprintf("%s-same-project-%v", restoreNamePrefix, RandomString(10))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, SourceClusterName, orgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s", restoreName, backupName))
		})

		Step("Restoring to different project and different namespace in same cluster", func() {
			log.InfoD("Restoring to different project and different namespace in same cluster")
			var restoredNamespaceList []string
			projectNameMapping := make(map[string]string)
			projectUIDMapping := make(map[string]string)
			namespaceMapping := make(map[string]string)
			log.InfoD("Restoring to different project and different namespace in same cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				restoredNamespace := "restored-different-project-" + RandomString(5)
				namespaceMapping[namespace] = restoredNamespace
				restoreNamespacesAll = append(restoreNamespacesAll, restoredNamespace)
				restoredNamespaceList = append(restoredNamespaceList, restoredNamespace)
			}
			projectNameMapping[sourceClusterProjectList[0]] = sourceClusterProjectList[1]
			projectUIDMapping[sourceClusterProjectUIDList[0]] = sourceClusterProjectUIDList[1]
			restoreName := fmt.Sprintf("%s-diff-project-%v", restoreNamePrefix, RandomString(10))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, SourceClusterName, orgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s", restoreName, backupName))
		})

		Step("Creating rancher project in destination cluster", func() {
			log.InfoD("Creating rancher project in destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			project := fmt.Sprintf("dest-rke-project-%v", RandomString(10))
			_, err = Inst().S.(*rke.Rancher).CreateRancherProject(project, "new project", "local")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating rancher project %s", project))
			projectID, err := Inst().S.(*rke.Rancher).GetProjectID(project)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating rancher project ID for destination cluster %s", project))
			destClusterProjectList = append(destClusterProjectList, project)
			destClusterProjectUIDList = append(destClusterProjectUIDList, projectID)
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
		})

		Step("Restoring to different project but same name of namespace in different cluster", func() {
			log.InfoD("Restoring to different project but same name of namespace in different cluster")
			var restoredNamespaceList []string
			projectNameMapping := make(map[string]string)
			projectUIDMapping := make(map[string]string)
			namespaceMapping := make(map[string]string)
			log.InfoD("Restoring to different project but same name of namespace in different cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				//restoredNamespace := "restored-diff-project-diff-cluster-same-ns" + RandomString(5)
				namespaceMapping[namespace] = namespace
				destRestoreNamespacesAll = append(destRestoreNamespacesAll, namespace)
				restoredNamespaceList = append(restoredNamespaceList, namespace)
			}
			projectNameMapping[sourceClusterProjectList[0]] = destClusterProjectList[0]
			projectUIDMapping[sourceClusterProjectUIDList[0]] = destClusterProjectUIDList[0]
			restoreName := fmt.Sprintf("%s-diff-proj-same-ns-diff-cluster%v", restoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, destinationClusterName, orgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s", restoreName, backupName))
		})

		Step("Restoring to different project and different namespace in different cluster", func() {
			log.InfoD("Restoring to different project and different namespace in different cluster")
			var restoredNamespaceList []string
			projectNameMapping := make(map[string]string)
			projectUIDMapping := make(map[string]string)
			namespaceMapping := make(map[string]string)
			log.InfoD("Restoring to different project but same name of namespace in different cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				restoredNamespace := "restored-diff-project-diff-cluster-same-ns-" + RandomString(5)
				namespaceMapping[namespace] = restoredNamespace
				destRestoreNamespacesAll = append(destRestoreNamespacesAll, restoredNamespace)
				restoredNamespaceList = append(restoredNamespaceList, restoredNamespace)
			}
			projectNameMapping[sourceClusterProjectList[0]] = destClusterProjectList[0]
			projectUIDMapping[sourceClusterProjectUIDList[0]] = destClusterProjectUIDList[0]
			restoreName := fmt.Sprintf("%s-diff-proj-diff-ns-diff-cluster%v", restoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, destinationClusterName, orgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s", restoreName, backupName))
		})
	})

	JustAfterEach(func() {
		defer func() {
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")
			EndPxBackupTorpedoTest(scheduledAppContexts)
		}()
		err := SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		for _, ns := range restoreNamespacesAll {
			err = core.Instance().DeleteNamespace(ns)
			log.FailOnError(err, "Deletion of namespace %s failed", ns)
		}
		for _, restoreName := range restoreList {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying restore deletion - %s", restoreName))
		}
		log.Infof("Deleting projects from source cluster")
		for i, project := range sourceClusterProjectList {
			err = Inst().S.(*rke.Rancher).DeleteRancherProject(sourceClusterProjectUIDList[i])
			log.FailOnError(err, "Deletion of project %s failed", project)
		}
		log.Infof("Deleting projects from destination cluster")
		// Switch context to destination cluster
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		for i, project := range destClusterProjectList {
			err = Inst().S.(*rke.Rancher).DeleteRancherProject(destClusterProjectUIDList[i])
			log.FailOnError(err, "Deletion of project %s from destination cluster failed", project)
		}
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster failed")
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, credUid, ctx)
	})
})
