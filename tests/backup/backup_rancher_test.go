package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/scheduler/rke"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	_ "github.com/rancher/norman/clientbase"
	_ "github.com/rancher/rancher/pkg/client/generated/management/v3"
	v1 "k8s.io/api/core/v1"
	storageApi "k8s.io/api/storage/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
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
	projectLabel := make(map[string]string)
	projectAnnotation := make(map[string]string)

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
		projectLabel[RandomString(10)] = RandomString(10)
		projectAnnotation[RandomString(10)] = RandomString(10)
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
				_, err = Inst().S.(*rke.Rancher).CreateRancherProject(project, rancherProjectDescription, rancherActiveCluster, projectLabel, projectAnnotation)
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
			_, err = Inst().S.(*rke.Rancher).CreateRancherProject(project, rancherProjectDescription, rancherActiveCluster, projectLabel, projectAnnotation)
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

// This testcase move the ns from project to project and then to no project while restore is in progress
var _ = Describe("{NamespaceMoveFromProjectToProjectToNoProjectWhileRestore}", func() {

	var (
		credName                 string
		credUid                  string
		customBackupLocationName string
		backupLocationUID        string
		sourceClusterUid         string
		backupName               string
		sourceProject            string
		restoreName              string
		sourceProjectID          string
		destProjectIDList        []string
		appNamespaces            []string
		restoreList              []string
		destProjectList          []string
		restoreNamespaceList     []string
		contexts                 []*scheduler.Context
		appContexts              []*scheduler.Context
		scheduledAppContexts     []*scheduler.Context
	)
	projectNameMapping := make(map[string]string)
	projectUIDMapping := make(map[string]string)
	namespaceMapping := make(map[string]string)
	backupLocationMap := make(map[string]string)
	projectLabel := make(map[string]string)
	projectAnnotation := make(map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("NamespaceMoveFromProjectToProjectToNoProjectWhileRestore",
			"Take backup and move the namespace from project to project to no project during restore", nil, 84881)
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
		projectLabel[RandomString(10)] = RandomString(10)
		projectAnnotation[RandomString(10)] = RandomString(10)
	})

	It("Take backup of single namespace and move the namespace from project to project to no project while restore", func() {
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
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] as provider %s", credName, orgID, provider))
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

		Step("Creating a rancher project in source cluster", func() {
			log.InfoD("Creating a rancher project in source cluster")
			sourceProject = fmt.Sprintf("source-project-%v", RandomString(10))
			_, err = Inst().S.(*rke.Rancher).CreateRancherProject(sourceProject, rancherProjectDescription, rancherActiveCluster, projectLabel, projectAnnotation)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating rancher project %s in source cluster", sourceProject))
			sourceProjectID, err = Inst().S.(*rke.Rancher).GetProjectID(sourceProject)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Getting Project ID for project %s", sourceProject))
		})

		Step("Adding namespaces to project in source cluster and taking backup", func() {
			log.InfoD("Adding namespaces to project in source cluster and taking backup")
			err = Inst().S.(*rke.Rancher).AddNamespacesToProject(sourceProject, appNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding namespaces %s to project %s in source cluster", appNamespaces, sourceProject))
			err = Inst().S.(*rke.Rancher).ValidateProjectOfNamespaces(sourceProject, appNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if namespace %s is added to project %s", appNamespaces, sourceProject))
			log.InfoD("Taking Backup of applications %s", appNamespaces)
			backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, RandomString(10))
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, appNamespaces)
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, nil, orgID, sourceClusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
		})

		Step("Creating 2 rancher projects in destination cluster", func() {
			log.InfoD("Creating 2 rancher projects in destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			for i := 0; i < 2; i++ {
				destProject := fmt.Sprintf("dest-rke-project-%v-%v", RandomString(5), i)
				_, err = Inst().S.(*rke.Rancher).CreateRancherProject(destProject, rancherProjectDescription, rancherActiveCluster, projectLabel, projectAnnotation)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating rancher project %s in destination cluster", destProject))
				destProjectList = append(destProjectList, destProject)
				destProjectID, err := Inst().S.(*rke.Rancher).GetProjectID(destProject)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Getting project Id for project: %s in destination cluster", destProject))
				destProjectIDList = append(destProjectIDList, destProjectID)
			}
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
		})

		Step("Restoring the backup taken with namespace and project mapping", func() {
			log.InfoD("Restoring the backup taken with namespace and project mapping")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for i, app := range appNamespaces {
				restoreNamespace := fmt.Sprintf("restore-%v-%v-%v", app, RandomString(5), i)
				namespaceMapping[appNamespaces[i]] = restoreNamespace
				restoreNamespaceList = append(restoreNamespaceList, restoreNamespace)
			}
			projectNameMapping[sourceProject] = destProjectList[0]
			projectUIDMapping[sourceProjectID] = destProjectIDList[0]
			restoreName = fmt.Sprintf("%s-%v-default", restoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, destinationClusterName, orgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s having namespaces: %v", restoreName, backupName, appNamespaces))
		})

		Step("Restore the backup taken with replace policy and move destination ns from one project to another project while restoring", func() {
			log.InfoD("Restore the backup taken with replace policy and move ns from one project to another project while restoring")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-replace-ns-project-move-%v", restoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreOnRancherWithoutCheck(restoreName, backupName, namespaceMapping, destinationClusterName, orgID, ctx, nil, projectUIDMapping, projectNameMapping, 2)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s with replace policy from backup %s", restoreName, backupName))
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.Infof("Move namespaces in destination cluster to diff project while restore is going on")
			err = Inst().S.(*rke.Rancher).ChangeProjectForNamespace(destProjectList[1], restoreNamespaceList)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Moving namespaces %v from project %s to project %s", restoreNamespaceList, destProjectList[0], destProjectList[1]))
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
			log.Infof("Verifying if restore is successful after moving destination namespaces from one project to another")
			err = restoreSuccessCheck(restoreName, orgID, maxWaitPeriodForRestoreCompletionInMinute*time.Minute, restoreJobProgressRetryTime*time.Minute, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restore %s taken from backup %v having namespaces %v with destination namespaces: %v", restoreName, backupName, appNamespaces, restoreNamespaceList))
		})

		Step("Restore the backup taken with replace policy and move destination ns to no project while restoring", func() {
			log.InfoD("Restore the backup taken with replace policy and move destination ns to no project while restoring")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-replace-ns-move-to-no-project-%v", restoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreOnRancherWithoutCheck(restoreName, backupName, namespaceMapping, destinationClusterName, orgID, ctx, nil, projectUIDMapping, projectNameMapping, 2)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s", restoreName, backupName))
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.Infof("Move destination ns to no project while restore is going on")
			err = Inst().S.(*rke.Rancher).RemoveNamespaceFromProject(restoreNamespaceList)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Moving namespaces %v from project %s to no project", restoreNamespaceList, destProjectList[1]))
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
			log.Infof("Verifying if restore is successful after moving destination namespaces to no project")
			err = restoreSuccessCheck(restoreName, orgID, maxWaitPeriodForRestoreCompletionInMinute*time.Minute, restoreJobProgressRetryTime*time.Minute, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restore %s with destination namespaces: %v", restoreName, restoreNamespaceList))
		})
	})

	JustAfterEach(func() {
		defer func() {
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")
			EndPxBackupTorpedoTest(scheduledAppContexts)
		}()
		// Switch context to destination cluster
		err := SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		log.Infof("Deleting restored namespace from destination cluster")
		for _, ns := range restoreNamespaceList {
			err = core.Instance().DeleteNamespace(ns)
			log.FailOnError(err, "Deletion of namespace %s from destination cluster failed", ns)
		}
		log.Infof("Deleting projects from destination cluster")
		for i, project := range destProjectList {
			err = Inst().S.(*rke.Rancher).DeleteRancherProject(destProjectIDList[i])
			log.FailOnError(err, "Deletion of project %s from destination cluster failed", project)
		}
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster failed")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx for source cluster")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.Infof("Deleting restores created")
		for _, restoreName := range restoreList {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying restore deletion - %s", restoreName))
		}
		log.Infof("Deleting projects from source cluster")
		err = Inst().S.(*rke.Rancher).DeleteRancherProject(sourceProjectID)
		log.FailOnError(err, "Deletion of project %s from source cluster failed", sourceProject)
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, credUid, ctx)
	})
})

// This testcase takes backup and restore of multiple namespaces belonging to multiple projects
var _ = Describe("{MultipleProjectsAndNamespacesBackupAndRestore}", func() {

	var (
		credName                          string
		credUid                           string
		customBackupLocationName          string
		backupLocationUID                 string
		sourceClusterUid                  string
		backupName                        string
		sourceProject                     string
		restoreName                       string
		sourceProjectID                   string
		scName                            string
		noProjectBackup                   string
		destProjectIDList                 []string
		appNamespaces                     []string
		destProjectList                   []string
		sourceProjectList                 []string
		sourceProjectIDList               []string
		backupList                        []string
		sourceClusterRestoreNamespaceList []string
		fewNamespaceFromEachProject       []string
		restoreList                       []string
		destClusterRestoreNamespaceList   []string
		namespaceList                     [][]string
		sourceScName                      *storageApi.StorageClass
		contexts                          []*scheduler.Context
		appContexts                       []*scheduler.Context
		scheduledAppContexts              []*scheduler.Context
	)
	projectNamespaces := make(map[string][]string)
	projectNameMapping := make(map[string]string)
	projectUIDMapping := make(map[string]string)
	namespaceMapping := make(map[string]string)
	backupLocationMap := make(map[string]string)
	params := make(map[string]string)
	storageClassMapping := make(map[string]string)
	labelSelectors := make(map[string]string)
	projectLabel := make(map[string]string)
	projectAnnotation := make(map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("MultipleProjectsAndNamespacesBackupAndRestore",
			"Take backups and restores of multiple namespaces belonging to multiple projects", nil, 84874)
		log.InfoD("Deploying multiple instances of applications required for the testcase")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < 4; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
			log.Infof("The list of namespaces deployed are", appNamespaces)
		}
		projectLabel[RandomString(10)] = RandomString(10)
		projectAnnotation[RandomString(10)] = RandomString(10)
	})

	It("Take backup of multiple namespaces belonging to multiple projects and restore them", func() {
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
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] as provider %s", credName, orgID, provider))
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

		Step("Creating 2 rancher projects in source cluster", func() {
			log.InfoD("Creating 2 rancher projects in source cluster")
			for i := 0; i < 2; i++ {
				sourceProject = fmt.Sprintf("source-project-%v-%v", RandomString(10), i)
				_, err = Inst().S.(*rke.Rancher).CreateRancherProject(sourceProject, rancherProjectDescription, rancherActiveCluster, projectLabel, projectAnnotation)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating rancher project %s in source cluster", sourceProject))
				sourceProjectID, err = Inst().S.(*rke.Rancher).GetProjectID(sourceProject)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Getting Project ID for project %s", sourceProject))
				sourceProjectList = append(sourceProjectList, sourceProject)
				sourceProjectIDList = append(sourceProjectIDList, sourceProjectID)
			}
		})

		Step("Adding namespaces to project in source cluster and taking backup", func() {
			log.InfoD("Adding namespaces to project in source cluster and taking backup")
			projectNamespaces[sourceProjectList[0]] = appNamespaces[0 : len(appNamespaces)/2]
			projectNamespaces[sourceProjectList[1]] = appNamespaces[len(appNamespaces)/2:]
			log.Infof("The value of project to namespace mapping in source cluster is %v", projectNamespaces)
			fewNamespaceFromEachProject = append(fewNamespaceFromEachProject, appNamespaces[0:len(appNamespaces)/4]...)
			fewNamespaceFromEachProject = append(fewNamespaceFromEachProject, appNamespaces[len(appNamespaces)/2:len(appNamespaces)/2+len(appNamespaces)/4]...)
			log.Infof("The list of few namespaces from both the projects are %v:", fewNamespaceFromEachProject)
			log.Infof("Adding half of the namespaces to first project and second half to second project in source cluster")
			for key, value := range projectNamespaces {
				err = Inst().S.(*rke.Rancher).AddNamespacesToProject(key, value)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Adding namespaces %s to project %s in source cluster", value, key))
				err = Inst().S.(*rke.Rancher).ValidateProjectOfNamespaces(key, value)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if namespaces %s are added to project %s", value, key))
			}

			log.InfoD("Taking backup of all applications %s from both the projects in source cluster", appNamespaces)
			namespaceList = append(namespaceList, appNamespaces)
			namespaceList = append(namespaceList, fewNamespaceFromEachProject)
			for _, val := range namespaceList {
				backupName = fmt.Sprintf("%s-%v-ns", BackupNamePrefix, RandomString(10))
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, val)
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, nil, orgID, sourceClusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and validation of backup [%s] with the namespaces %s", backupName, val))
				backupList = append(backupList, backupName)
			}
			log.Infof("The backup list is %v", backupList)
		})

		Step("Taking default restore of the backups in destination cluster", func() {
			log.InfoD("Taking default restore of the backups taken in destination cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupList {
				restoreName = fmt.Sprintf("%s-%v-default", restoreNamePrefix, backupName)
				restoreList = append(restoreList, restoreName)
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, make(map[string]string), make(map[string]string), destinationClusterName, orgID, scheduledAppContexts)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating default restore: %s from backup: %s", restoreName, backupName))
			}
		})

		Step("Restoring to same project but different namespace in source cluster", func() {
			log.InfoD("Restoring to same project but different namespace in source cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for i, app := range appNamespaces {
				restoreNamespace := fmt.Sprintf("restore-%v-%v-%v", app, RandomString(5), i)
				namespaceMapping[app] = restoreNamespace
				sourceClusterRestoreNamespaceList = append(sourceClusterRestoreNamespaceList, restoreNamespace)
			}
			for i, project := range sourceProjectList {
				projectNameMapping[project] = project
				projectUIDMapping[sourceProjectIDList[i]] = sourceProjectIDList[i]
			}
			restoreName = fmt.Sprintf("%s-%v-same-proj-diff-ns", restoreNamePrefix, backupName)
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupList[0], namespaceMapping, SourceClusterName, orgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s in same project but different namespace from backup %s in source cluster", restoreName, backupList[0]))
		})

		Step("Creating 2 rancher projects in destination cluster", func() {
			log.Infof("Creating 2 rancher projects in destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			for i := 0; i < 2; i++ {
				destProject := fmt.Sprintf("dest-rke-project-%v-%v", RandomString(5), i)
				_, err = Inst().S.(*rke.Rancher).CreateRancherProject(destProject, "new project", rancherActiveCluster, projectLabel, projectAnnotation)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating rancher project %s in destination cluster", destProject))
				destProjectList = append(destProjectList, destProject)
				destProjectID, err := Inst().S.(*rke.Rancher).GetProjectID(destProject)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Getting project Id for project: %s in destination cluster", destProject))
				destProjectIDList = append(destProjectIDList, destProjectID)
			}
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
		})

		Step("Restoring to different project but same namespace in destination cluster", func() {
			log.InfoD("Restoring to different project but same namespace in destination cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupList {
				for i, project := range sourceProjectList {
					projectNameMapping[project] = destProjectList[i]
					projectUIDMapping[sourceProjectIDList[i]] = destProjectIDList[i]
				}
				restoreName = fmt.Sprintf("%s-%v-diff-proj-same-ns-%v", restoreNamePrefix, backupName, RandomString(5))
				restoreList = append(restoreList, restoreName)
				err = CreateRestoreWithProjectMapping(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctx, nil, projectUIDMapping, projectNameMapping)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s in diff project but same namespace from backup %s in destination cluster", restoreName, backupName))
			}
		})

		Step("Restoring to different project and different namespace in destination cluster", func() {
			log.InfoD("Restoring to different project and different namespace in destination cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for i, project := range sourceProjectList {
				projectNameMapping[project] = destProjectList[i]
				projectUIDMapping[sourceProjectIDList[i]] = destProjectIDList[i]
			}
			for i, app := range appNamespaces {
				restoreNamespace := fmt.Sprintf("restore-diff-proj-diff-ns-%v-%v", RandomString(5), i)
				namespaceMapping[app] = restoreNamespace
				destClusterRestoreNamespaceList = append(destClusterRestoreNamespaceList, restoreNamespace)
			}
			restoreName = fmt.Sprintf("%s-%v-diff-proj-diff-ns", restoreNamePrefix, backupList[0])
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupList[0], namespaceMapping, destinationClusterName, orgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s in different project and different namespace from backup %s in destination cluster", restoreName, backupList[0]))
		})

		Step("Getting storage class of the source cluster", func() {
			log.InfoD("Getting storage class of the source cluster")
			pvcs, err := core.Instance().GetPersistentVolumeClaims(appNamespaces[0], labelSelectors)
			singlePvc := pvcs.Items[0]
			sourceScName, err = core.Instance().GetStorageClassForPVC(&singlePvc)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Getting SC %v from PVC in source cluster", sourceScName.Name))
		})

		Step("Create new storage class on destination cluster and restore with storage class mapping", func() {
			log.InfoD("Create new storage class on destination cluster and restore with storage class mapping")
			log.InfoD("Switching cluster context to destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Failed to set destination kubeconfig")
			params["repl"] = "2"
			scName = fmt.Sprintf("replica-sc-%v", time.Now().Unix())
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
			log.InfoD("Create new storage class on destination cluster for storage class mapping for restore")
			_, err = storage.Instance().CreateStorageClass(&scObj)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating new storage class %v on destination cluster %s", scName, destinationClusterName))
			storageClassMapping[sourceScName.Name] = scName
			log.InfoD("Switching cluster context back to source cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Failed to set source kubeconfig")
			log.InfoD("Restoring to different project and different namespace with storage class mapping in destination cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for i, project := range sourceProjectList {
				projectNameMapping[project] = destProjectList[i]
				projectUIDMapping[sourceProjectIDList[i]] = destProjectIDList[i]
			}
			for i, app := range appNamespaces {
				restoreNamespace := fmt.Sprintf("restore-diff-proj-diff-ns-sc-mapping-%v-%v", RandomString(5), i)
				namespaceMapping[app] = restoreNamespace
				destClusterRestoreNamespaceList = append(destClusterRestoreNamespaceList, restoreNamespace)
			}
			restoreName = fmt.Sprintf("%s-%v-diff-proj-diff-ns-sc-mapping", restoreNamePrefix, backupList[0])
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupList[0], namespaceMapping, destinationClusterName, orgID, ctx, storageClassMapping, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s in different project and different namespace with storage class mapping from backup %s in destination cluster", restoreName, backupList[0]))
		})

		Step("Remove the source cluster namespaces from the project", func() {
			log.Infof("Remove the source cluster namespaces from the project")
			err = Inst().S.(*rke.Rancher).RemoveNamespaceFromProject(appNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Removing the source cluster namespaces %s from the project", appNamespaces))
		})

		Step("Taking backup of namespaces after moving them to no project", func() {
			log.InfoD("Taking backup of namespaces %s after moving them to no project", appNamespaces)
			noProjectBackup = fmt.Sprintf("%s-%v-no-project", BackupNamePrefix, RandomString(10))
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, appNamespaces)
			err = CreateBackupWithValidation(ctx, noProjectBackup, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, nil, orgID, sourceClusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and validation of backup [%s] with the namespaces %s after removing the namespaces from project", noProjectBackup, appNamespaces))
		})
		Step("Restore the backup taken after all the namespaces are removed from the project", func() {
			log.InfoD("Restore the backup taken after all the namespaces are removed from the project")
			restoreName := fmt.Sprintf("%s-%v-no-project", restoreNamePrefix, RandomString(10))
			err = CreateRestoreWithValidation(ctx, restoreName, noProjectBackup, make(map[string]string), make(map[string]string), destinationClusterName, orgID, scheduledAppContexts)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s] from backup [%s]", restoreName, noProjectBackup))
			restoreList = append(restoreList, restoreName)
		})
	})

	JustAfterEach(func() {
		defer func() {
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")
			EndPxBackupTorpedoTest(scheduledAppContexts)
		}()
		err := SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		log.Infof("Deleting restored namespace from destination cluster")
		destClusterRestoreNamespaceList = append(destClusterRestoreNamespaceList, appNamespaces...)
		for _, ns := range destClusterRestoreNamespaceList {
			err = core.Instance().DeleteNamespace(ns)
			log.FailOnError(err, "Deletion of namespace %s from destination cluster failed", ns)
		}
		log.Infof("Deleting projects from destination cluster")
		for i, project := range destProjectList {
			err = Inst().S.(*rke.Rancher).DeleteRancherProject(destProjectIDList[i])
			log.FailOnError(err, "Deletion of project %s from destination cluster failed", project)
		}
		log.InfoD("Deleting the newly created storage class in destination cluster")
		err = storage.Instance().DeleteStorageClass(scName)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting storage class %s from source cluster cluster", scName))

		err = SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster failed")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx for source cluster")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.Infof("Deleting restore ")
		for _, restoreName := range restoreList {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying restore deletion - %s", restoreName))
		}
		log.Infof("Deleting restored namespace from source cluster")
		for _, ns := range sourceClusterRestoreNamespaceList {
			err = core.Instance().DeleteNamespace(ns)
			log.FailOnError(err, "Deletion of namespace %s from source cluster failed", ns)
		}
		log.Infof("Deleting projects from source cluster")
		for _, projectId := range sourceProjectIDList {
			err = Inst().S.(*rke.Rancher).DeleteRancherProject(projectId)
			log.FailOnError(err, "Deletion of project %s from source cluster failed", sourceProject)
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, credUid, ctx)
	})
})
