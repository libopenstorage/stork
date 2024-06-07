package tests

import (
	"fmt"
	rancherClient "github.com/rancher/rancher/pkg/client/generated/management/v3"
	"time"

	. "github.com/onsi/ginkgo/v2"
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
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	storageApi "k8s.io/api/storage/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// This testcase takes backup of single namespace and restore to namespace in same and different project
var _ = Describe("{SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject}", Label(TestCaseLabelsMap[SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject]...), func() {

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
		controlChannel              chan string
		errorGroup                  *errgroup.Group
	)
	backupLocationMap := make(map[string]string)
	projectLabel := make(map[string]string)
	projectAnnotation := make(map[string]string)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject",
			"Take backup of single namespace and restore to namespace in same and different project", nil, 84872, Sagrawal, Q2FY24)
		log.InfoD("Deploying applications required for the testcase")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = AppReadinessTimeout
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
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})

		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			backupLocationProviders := GetBackupProviders()
			for _, provider := range backupLocationProviders {
				credName = fmt.Sprintf("%s-cred-%v", provider, RandomString(10))
				credUid = uuid.New()
				err := CreateCloudCredential(provider, credName, credUid, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s]  as provider %s", credName, BackupOrgID, provider))
				customBackupLocationName = fmt.Sprintf("%s-backup-location-%v", provider, RandomString(10))
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = customBackupLocationName
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, credUid, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
			}
		})

		Step("Registering application clusters for backup", func() {
			log.InfoD("Registering application clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating source and destination rancher project in source cluster", func() {
			log.InfoD("Creating source and destination rancher project in source cluster")
			for i := 0; i < 2; i++ {
				project := fmt.Sprintf("rke-project-%v", RandomString(10))
				_, err = Inst().S.(*rke.Rancher).CreateRancherProject(project, RancherProjectDescription, RancherActiveCluster, projectLabel, projectAnnotation)
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
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, nil, BackupOrgID, sourceClusterUid, "", "", "", "")
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
			restoreName := fmt.Sprintf("%s-same-project-%v", RestoreNamePrefix, RandomString(10))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, SourceClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping)
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
			restoreName := fmt.Sprintf("%s-diff-project-%v", RestoreNamePrefix, RandomString(10))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, SourceClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s", restoreName, backupName))
		})

		Step("Creating rancher project in destination cluster", func() {
			log.InfoD("Creating rancher project in destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			project := fmt.Sprintf("dest-rke-project-%v", RandomString(10))
			_, err = Inst().S.(*rke.Rancher).CreateRancherProject(project, RancherProjectDescription, RancherActiveCluster, projectLabel, projectAnnotation)
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
			restoreName := fmt.Sprintf("%s-diff-proj-same-ns-diff-cluster%v", RestoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping)
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
			restoreName := fmt.Sprintf("%s-diff-proj-diff-ns-diff-cluster%v", RestoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping)
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
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")
		for _, ns := range restoreNamespacesAll {
			err = DeleteAppNamespace(ns)
			log.FailOnError(err, "Deletion of namespace %s failed", ns)
		}
		for _, restoreName := range restoreList {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
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
var _ = Describe("{NamespaceMoveFromProjectToProjectToNoProjectWhileRestore}", Label(TestCaseLabelsMap[NamespaceMoveFromProjectToProjectToNoProjectWhileRestore]...), func() {

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
		controlChannel           chan string
		errorGroup               *errgroup.Group
	)
	projectNameMapping := make(map[string]string)
	projectUIDMapping := make(map[string]string)
	namespaceMapping := make(map[string]string)
	backupLocationMap := make(map[string]string)
	projectLabel := make(map[string]string)
	projectAnnotation := make(map[string]string)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("NamespaceMoveFromProjectToProjectToNoProjectWhileRestore",
			"Take backup and move the namespace from project to project to no project during restore", nil, 84881, Sagrawal, Q3FY24)
		log.InfoD("Deploying applications required for the testcase")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = AppReadinessTimeout
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
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})

		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			backupLocationProviders := GetBackupProviders()
			for _, provider := range backupLocationProviders {
				credName = fmt.Sprintf("%s-cred-%v", provider, RandomString(10))
				credUid = uuid.New()
				err := CreateCloudCredential(provider, credName, credUid, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] as provider %s", credName, BackupOrgID, provider))
				customBackupLocationName = fmt.Sprintf("%s-backup-location-%v", provider, RandomString(10))
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = customBackupLocationName
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, credUid, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
			}
		})

		Step("Registering application clusters for backup", func() {
			log.InfoD("Registering application clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating a rancher project in source cluster", func() {
			log.InfoD("Creating a rancher project in source cluster")
			sourceProject = fmt.Sprintf("source-project-%v", RandomString(10))
			_, err = Inst().S.(*rke.Rancher).CreateRancherProject(sourceProject, RancherProjectDescription, RancherActiveCluster, projectLabel, projectAnnotation)
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
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, nil, BackupOrgID, sourceClusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
		})

		Step("Creating 2 rancher projects in destination cluster", func() {
			log.InfoD("Creating 2 rancher projects in destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			for i := 0; i < 2; i++ {
				destProject := fmt.Sprintf("dest-rke-project-%v-%v", RandomString(5), i)
				_, err = Inst().S.(*rke.Rancher).CreateRancherProject(destProject, RancherProjectDescription, RancherActiveCluster, projectLabel, projectAnnotation)
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
			restoreName = fmt.Sprintf("%s-%v-default", RestoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s having namespaces: %v", restoreName, backupName, appNamespaces))
		})

		Step("Restore the backup taken with replace policy and move destination ns from one project to another project while restoring", func() {
			log.InfoD("Restore the backup taken with replace policy and move ns from one project to another project while restoring")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-replace-ns-project-move-%v", RestoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreOnRancherWithoutCheck(restoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping, 2)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s with replace policy from backup %s", restoreName, backupName))
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.Infof("Move namespaces in destination cluster to diff project while restore is going on")
			err = Inst().S.(*rke.Rancher).ChangeProjectForNamespace(destProjectList[1], restoreNamespaceList)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Moving namespaces %v from project %s to project %s", restoreNamespaceList, destProjectList[0], destProjectList[1]))
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
			log.Infof("Verifying if restore is successful after moving destination namespaces from one project to another")
			err = RestoreSuccessCheck(restoreName, BackupOrgID, MaxWaitPeriodForRestoreCompletionInMinute*time.Minute, RestoreJobProgressRetryTime*time.Minute, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restore %s taken from backup %v having namespaces %v with destination namespaces: %v", restoreName, backupName, appNamespaces, restoreNamespaceList))
		})

		Step("Restore the backup taken with replace policy and move destination ns to no project while restoring", func() {
			log.InfoD("Restore the backup taken with replace policy and move destination ns to no project while restoring")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-replace-ns-move-to-no-project-%v", RestoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreOnRancherWithoutCheck(restoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping, 2)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s", restoreName, backupName))
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.Infof("Move destination ns to no project while restore is going on")
			err = Inst().S.(*rke.Rancher).RemoveNamespaceFromProject(restoreNamespaceList)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Moving namespaces %v from project %s to no project", restoreNamespaceList, destProjectList[1]))
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
			log.Infof("Verifying if restore is successful after moving destination namespaces to no project")
			err = RestoreSuccessCheck(restoreName, BackupOrgID, MaxWaitPeriodForRestoreCompletionInMinute*time.Minute, RestoreJobProgressRetryTime*time.Minute, ctx)
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
			err = DeleteAppNamespace(ns)
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
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")
		log.Infof("Deleting restores created")
		for _, restoreName := range restoreList {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying restore deletion - %s", restoreName))
		}
		log.Infof("Deleting projects from source cluster")
		err = Inst().S.(*rke.Rancher).DeleteRancherProject(sourceProjectID)
		log.FailOnError(err, "Deletion of project %s from source cluster failed", sourceProject)
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, credUid, ctx)
	})
})

// This testcase takes backup and restore of multiple namespaces belonging to multiple projects
var _ = Describe("{MultipleProjectsAndNamespacesBackupAndRestore}", Label(TestCaseLabelsMap[MultipleProjectsAndNamespacesBackupAndRestore]...), func() {

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
		controlChannel                    chan string
		errorGroup                        *errgroup.Group
	)
	projectNamespaces := make(map[string][]string)
	projectNameMapping := make(map[string]string)
	projectUIDMapping := make(map[string]string)
	backupLocationMap := make(map[string]string)
	params := make(map[string]string)
	storageClassMapping := make(map[string]string)
	labelSelectors := make(map[string]string)
	projectLabel := make(map[string]string)
	projectAnnotation := make(map[string]string)
	namespaceMappingDiffProjectDiffNsDestCluster := make(map[string]string)
	namespaceMappingStorageClassMappingDestCluster := make(map[string]string)
	namespaceMappingSameProjectDiffNamespaceSourceCluster := make(map[string]string)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("MultipleProjectsAndNamespacesBackupAndRestore",
			"Take backups and restores of multiple namespaces belonging to multiple projects", nil, 84874, Sagrawal, Q3FY24)
		log.InfoD("Deploying multiple instances of applications required for the testcase")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < 4; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = AppReadinessTimeout
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
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})

		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			backupLocationProviders := GetBackupProviders()
			for _, provider := range backupLocationProviders {
				credName = fmt.Sprintf("%s-cred-%v", provider, RandomString(10))
				credUid = uuid.New()
				err := CreateCloudCredential(provider, credName, credUid, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] as provider %s", credName, BackupOrgID, provider))
				customBackupLocationName = fmt.Sprintf("%s-backup-location-%v", provider, RandomString(10))
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = customBackupLocationName
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, credUid, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
			}
		})

		Step("Registering application clusters for backup", func() {
			log.InfoD("Registering application clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating 2 rancher projects in source cluster", func() {
			log.InfoD("Creating 2 rancher projects in source cluster")
			for i := 0; i < 2; i++ {
				sourceProject = fmt.Sprintf("source-project-%v-%v", RandomString(10), i)
				_, err = Inst().S.(*rke.Rancher).CreateRancherProject(sourceProject, RancherProjectDescription, RancherActiveCluster, projectLabel, projectAnnotation)
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
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, nil, BackupOrgID, sourceClusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and validation of backup [%s] with the namespaces %s", backupName, val))
				backupList = append(backupList, backupName)
			}
			log.Infof("The backup list is %v", backupList)
		})

		Step("Taking default restore of the backups in destination cluster", func() {
			log.InfoD("Taking default restore of the backups taken in destination cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for i, backupName := range backupList {
				restoreName = fmt.Sprintf("%s-%v-default", RestoreNamePrefix, backupName)
				restoreList = append(restoreList, restoreName)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, namespaceList[i])
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating default restore: %s from backup: %s", restoreName, backupName))
			}
		})

		Step("Restoring to same project but different namespace in source cluster", func() {
			log.InfoD("Restoring to same project but different namespace in source cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for i, app := range appNamespaces {
				restoreNamespace := fmt.Sprintf("restore-%v-%v-%v", app, RandomString(5), i)
				namespaceMappingSameProjectDiffNamespaceSourceCluster[app] = restoreNamespace
				sourceClusterRestoreNamespaceList = append(sourceClusterRestoreNamespaceList, restoreNamespace)
			}
			for i, project := range sourceProjectList {
				projectNameMapping[project] = project
				projectUIDMapping[sourceProjectIDList[i]] = sourceProjectIDList[i]
			}
			restoreName = fmt.Sprintf("%s-%v-same-proj-diff-ns", RestoreNamePrefix, backupName)
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupList[0], namespaceMappingSameProjectDiffNamespaceSourceCluster, SourceClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s in same project but different namespace from backup %s in source cluster", restoreName, backupList[0]))
		})

		Step("Creating 2 rancher projects in destination cluster", func() {
			log.Infof("Creating 2 rancher projects in destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			for i := 0; i < 2; i++ {
				destProject := fmt.Sprintf("dest-rke-project-%v-%v", RandomString(5), i)
				_, err = Inst().S.(*rke.Rancher).CreateRancherProject(destProject, "new project", RancherActiveCluster, projectLabel, projectAnnotation)
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
				restoreName = fmt.Sprintf("%s-%v-diff-proj-same-ns-%v", RestoreNamePrefix, backupName, RandomString(5))
				restoreList = append(restoreList, restoreName)
				err = CreateRestoreWithProjectMapping(restoreName, backupName, make(map[string]string), DestinationClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping)
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
				namespaceMappingDiffProjectDiffNsDestCluster[app] = restoreNamespace
				destClusterRestoreNamespaceList = append(destClusterRestoreNamespaceList, restoreNamespace)
			}
			restoreName = fmt.Sprintf("%s-%v-diff-proj-diff-ns", RestoreNamePrefix, backupList[0])
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupList[0], namespaceMappingDiffProjectDiffNsDestCluster, DestinationClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping)
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
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating new storage class %v on destination cluster %s", scName, DestinationClusterName))
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
				namespaceMappingStorageClassMappingDestCluster[app] = restoreNamespace
				destClusterRestoreNamespaceList = append(destClusterRestoreNamespaceList, restoreNamespace)
			}
			restoreName = fmt.Sprintf("%s-%v-diff-proj-diff-ns-sc-mapping", RestoreNamePrefix, backupList[0])
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupList[0], namespaceMappingStorageClassMappingDestCluster, DestinationClusterName, BackupOrgID, ctx, storageClassMapping, projectUIDMapping, projectNameMapping)
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
			err = CreateBackupWithValidation(ctx, noProjectBackup, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, nil, BackupOrgID, sourceClusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and validation of backup [%s] with the namespaces %s after removing the namespaces from project", noProjectBackup, appNamespaces))
		})

		Step("Restore the backup taken after all the namespaces are removed from the project", func() {
			log.InfoD("Restore the backup taken after all the namespaces are removed from the project")
			restoreName := fmt.Sprintf("%s-%v-no-project", RestoreNamePrefix, RandomString(10))
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, appNamespaces)
			err = CreateRestoreWithValidation(ctx, restoreName, noProjectBackup, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
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
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		err := SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		log.Infof("Deleting restored namespace from destination cluster")
		restoredAppContextsInDestinationCluster := make([]*scheduler.Context, 0)

		for _, scheduledAppContext := range scheduledAppContexts {
			restoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, namespaceMappingDiffProjectDiffNsDestCluster, make(map[string]string), true)
			if err != nil {
				log.Errorf("TransformAppContextWithMappings: %v", err)
				continue
			}
			restoredAppContextsInDestinationCluster = append(restoredAppContextsInDestinationCluster, restoredAppContext)
		}

		for _, scheduledAppContext := range scheduledAppContexts {
			restoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, namespaceMappingStorageClassMappingDestCluster, make(map[string]string), true)
			if err != nil {
				log.Errorf("TransformAppContextWithMappings: %v", err)
				continue
			}
			restoredAppContextsInDestinationCluster = append(restoredAppContextsInDestinationCluster, restoredAppContext)
		}

		for _, scheduledAppContext := range scheduledAppContexts {
			restoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, make(map[string]string), make(map[string]string), true)
			if err != nil {
				log.Errorf("TransformAppContextWithMappings: %v", err)
				continue
			}
			restoredAppContextsInDestinationCluster = append(restoredAppContextsInDestinationCluster, restoredAppContext)
		}
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")

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
		DestroyApps(scheduledAppContexts, opts)
		log.Infof("Deleting restore")
		for _, restoreName := range restoreList {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying restore deletion - %s", restoreName))
		}
		restoredAppContextsInSourceCluster := make([]*scheduler.Context, 0)
		for _, scheduledAppContext := range scheduledAppContexts {
			restoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, namespaceMappingSameProjectDiffNamespaceSourceCluster, make(map[string]string), true)
			if err != nil {
				log.Errorf("TransformAppContextWithMappings: %v", err)
				continue
			}
			restoredAppContextsInSourceCluster = append(restoredAppContextsInSourceCluster, restoredAppContext)
		}
		for _, scheduledAppContext := range scheduledAppContexts {
			restoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, make(map[string]string), make(map[string]string), true)
			if err != nil {
				log.Errorf("TransformAppContextWithMappings: %v", err)
				continue
			}
			restoredAppContextsInSourceCluster = append(restoredAppContextsInSourceCluster, restoredAppContext)
		}
		DestroyApps(restoredAppContextsInSourceCluster, opts)

		log.Infof("Deleting projects from source cluster")
		for _, projectId := range sourceProjectIDList {
			err = Inst().S.(*rke.Rancher).DeleteRancherProject(projectId)
			log.FailOnError(err, "Deletion of project %s from source cluster failed", sourceProject)
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, credUid, ctx)
	})
})

// This testcase takes backup of a multiple member project with single namespace and performs different combinations of restores
var _ = Describe("{MultipleMemberProjectBackupAndRestoreForSingleNamespace}", Label(TestCaseLabelsMap[MultipleMemberProjectBackupAndRestoreForSingleNamespace]...), func() {

	var (
		credName                    string
		credUid                     string
		customBackupLocationName    string
		backupLocationUID           string
		sourceClusterUid            string
		backupName                  string
		userIDList                  []string
		appNamespaces               []string
		sourceClusterProjectList    []string
		sourceClusterProjectUIDList []string
		restoreNamespacesAll        []string
		restoreList                 []string
		destClusterProjectList      []string
		destClusterProjectUIDList   []string
		destRestoreNamespacesAll    []string
		contexts                    []*scheduler.Context
		appContexts                 []*scheduler.Context
		scheduledAppContexts        []*scheduler.Context
		numUsers                    = 5
	)

	backupLocationMap := make(map[string]string)
	projectLabel := make(map[string]string)
	projectAnnotation := make(map[string]string)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("MultipleMemberProjectBackupAndRestoreForSingleNamespace",
			"Take backup of multiple member project with single namespace and perform restores", nil, 84876, Sabrarhussaini, Q4FY23)
		log.InfoD("Deploying applications required for the testcase")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
		projectLabel[RandomString(10)] = RandomString(10)
		projectAnnotation[RandomString(10)] = RandomString(10)
	})

	It("Takes backup of multiple member project with single namespace and performs restores in same and different projects", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			backupLocationProviders := GetBackupProviders()
			for _, provider := range backupLocationProviders {
				credName = fmt.Sprintf("%s-cred-%v", provider, RandomString(10))
				credUid = uuid.New()
				err := CreateCloudCredential(provider, credName, credUid, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s]  as provider %s", credName, BackupOrgID, provider))
				customBackupLocationName = fmt.Sprintf("%s-backup-location-%v", provider, RandomString(10))
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = customBackupLocationName
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, credUid, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
			}
		})

		Step("Registering application clusters for backup", func() {
			log.InfoD("Registering application clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating rancher projects on source cluster", func() {
			log.InfoD("Creating rancher projects on source cluster")
			for i := 0; i < 2; i++ {
				project := fmt.Sprintf("source-rke-project-%v-%v", i+1, RandomString(5))
				_, err = Inst().S.(*rke.Rancher).CreateRancherProject(project, RancherProjectDescription, RancherActiveCluster, projectLabel, projectAnnotation)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating rancher project %s", project))
				projectID, err := Inst().S.(*rke.Rancher).GetProjectID(project)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Getting Project ID for project %s", project))
				sourceClusterProjectList = append(sourceClusterProjectList, project)
				sourceClusterProjectUIDList = append(sourceClusterProjectUIDList, projectID)
			}
		})

		Step("Adding multiple users to the source project of rancher source cluster", func() {
			log.InfoD("Adding multiple users to the source project of rancher source cluster")
			userMap := make(map[string]string)
			password := RandomString(12)
			for i := 1; i <= numUsers; i++ {
				username := fmt.Sprintf("user-%d-%s", i, RandomString(6))
				userMap[username] = password
			}
			userIDList, err = Inst().S.(*rke.Rancher).CreateMultipleUsersForRancherProject(sourceClusterProjectList[0], userMap)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating rancher users and adding them to the project [%s]", sourceClusterProjectList[0]))
		})

		Step("Adding namespace to source project and taking backup of it", func() {
			log.InfoD("Adding namespace to source project and taking backup of it")
			err = Inst().S.(*rke.Rancher).AddNamespacesToProject(sourceClusterProjectList[0], appNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding namespaces %s to project %s", appNamespaces, sourceClusterProjectList[0]))
			err = Inst().S.(*rke.Rancher).ValidateProjectOfNamespaces(sourceClusterProjectList[0], appNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying project %s of namespace %s", sourceClusterProjectList[0], appNamespaces))
			log.InfoD("Taking Backup of application")
			for _, namespace := range appNamespaces {
				backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, RandomString(5))
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, nil, BackupOrgID, sourceClusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
			}
		})

		Step("Restoring backup to different namespace of same project of same cluster", func() {
			log.InfoD("Restoring backup to different namespace of same project of same cluster")
			var restoredNamespaceList []string
			projectNameMapping := make(map[string]string)
			projectUIDMapping := make(map[string]string)
			namespaceMapping := make(map[string]string)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				restoredNamespace := "restore-same-proj-diff-ns-" + RandomString(5)
				namespaceMapping[namespace] = restoredNamespace
				restoreNamespacesAll = append(restoreNamespacesAll, restoredNamespace)
				restoredNamespaceList = append(restoredNamespaceList, restoredNamespace)
			}
			projectNameMapping[sourceClusterProjectList[0]] = sourceClusterProjectList[0]
			projectUIDMapping[sourceClusterProjectUIDList[0]] = sourceClusterProjectUIDList[0]
			restoreName := fmt.Sprintf("%s-same-project-%v", RestoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, SourceClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s", restoreName, backupName))
		})

		Step("Restoring backup to a different namespace of different project of same cluster", func() {
			log.InfoD("Restoring backup to a different namespace of different project of same cluster")
			var restoredNamespaceList []string
			projectNameMapping := make(map[string]string)
			projectUIDMapping := make(map[string]string)
			namespaceMapping := make(map[string]string)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				restoredNamespace := "restored-diff-proj-diff-ns-" + RandomString(5)
				namespaceMapping[namespace] = restoredNamespace
				restoreNamespacesAll = append(restoreNamespacesAll, restoredNamespace)
				restoredNamespaceList = append(restoredNamespaceList, restoredNamespace)
			}
			projectNameMapping[sourceClusterProjectList[0]] = sourceClusterProjectList[1]
			projectUIDMapping[sourceClusterProjectUIDList[0]] = sourceClusterProjectUIDList[1]
			restoreName := fmt.Sprintf("%s-diff-project-%v", RestoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, SourceClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s", restoreName, backupName))
		})

		Step("Creating a rancher project in destination cluster", func() {
			log.InfoD("Creating a rancher project in destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			project := fmt.Sprintf("destination-rke-project-%v", RandomString(5))
			_, err = Inst().S.(*rke.Rancher).CreateRancherProject(project, RancherProjectDescription, RancherActiveCluster, projectLabel, projectAnnotation)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating rancher project %s", project))
			projectID, err := Inst().S.(*rke.Rancher).GetProjectID(project)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating rancher project ID for destination cluster %s", project))
			destClusterProjectList = append(destClusterProjectList, project)
			destClusterProjectUIDList = append(destClusterProjectUIDList, projectID)
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
		})

		Step("Restoring backup to the same namespace of a different project of different cluster", func() {
			log.InfoD("Restoring backup to the same namespace of a different project of different cluster")
			var restoredNamespaceList []string
			projectNameMapping := make(map[string]string)
			projectUIDMapping := make(map[string]string)
			namespaceMapping := make(map[string]string)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				namespaceMapping[namespace] = namespace
				destRestoreNamespacesAll = append(destRestoreNamespacesAll, namespace)
				restoredNamespaceList = append(restoredNamespaceList, namespace)
			}
			projectNameMapping[sourceClusterProjectList[0]] = destClusterProjectList[0]
			projectUIDMapping[sourceClusterProjectUIDList[0]] = destClusterProjectUIDList[0]
			restoreName := fmt.Sprintf("%s-diff-proj-same-ns-diff-cluster%v", RestoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s", restoreName, backupName))
		})

		Step("Restoring backup to the different namespace of a different project of different cluster", func() {
			log.InfoD("Restoring backup to the different namespace of a different project of different cluster")
			var restoredNamespaceList []string
			projectNameMapping := make(map[string]string)
			projectUIDMapping := make(map[string]string)
			namespaceMapping := make(map[string]string)
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
			restoreName := fmt.Sprintf("%s-diff-proj-diff-ns-diff-cluster%v", RestoreNamePrefix, RandomString(5))
			restoreList = append(restoreList, restoreName)
			err = CreateRestoreWithProjectMapping(restoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, nil, projectUIDMapping, projectNameMapping)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore- %s from backup %s", restoreName, backupName))
		})

		Step("Validating project members after the restore", func() {
			log.InfoD("Validating project -[%s] members after the restore", sourceClusterProjectList[0])
			err := Inst().S.(*rke.Rancher).ValidateUsersInProject(sourceClusterProjectList[0], userIDList)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if all the project members remain intact"))
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
			err = DeleteAppNamespace(ns)
			log.FailOnError(err, "Deletion of namespace %s failed", ns)
		}
		for _, restoreName := range restoreList {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying restore deletion - %s", restoreName))
		}
		log.Infof("Deleting projects from source cluster")
		for i, project := range sourceClusterProjectList {
			err = Inst().S.(*rke.Rancher).DeleteRancherProject(sourceClusterProjectUIDList[i])
			log.FailOnError(err, "Deletion of project %s failed", project)
		}
		log.Infof("Deleting users from source cluster")
		err = Inst().S.(*rke.Rancher).DeleteRancherUsers(userIDList)
		log.FailOnError(err, "Failed to delete users")
		// Switch context to destination cluster
		log.Infof("Deleting projects from destination cluster")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		for i, project := range destClusterProjectList {
			err = Inst().S.(*rke.Rancher).DeleteRancherProject(destClusterProjectUIDList[i])
			log.FailOnError(err, "Deletion of project %s from destination cluster failed", project)
		}
		for _, ns := range destRestoreNamespacesAll {
			err = DeleteAppNamespace(ns)
			log.FailOnError(err, "Deletion of namespace %s failed", ns)
		}
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster failed")
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, credUid, ctx)
	})
})

// This is a dummy PSA testcase to validate the PSA related methods for RKE
var _ = Describe("{DummyPSATestcase}", Label(TestCaseLabelsMap[DummyPSATestcase]...), func() {
	JustBeforeEach(func() {
		log.InfoD("No pre-configuration needed")
	})

	It("Dummy PSA testcase to validate the PSA related methods for RKE", func() {
		Step("Dummy PSA testcase to validate the PSA related methods for RKE", func() {
			var clusterList []string
			var err1 error
			var defaultExemptListForRestrictedPSA []string
			log.InfoD("Getting the list of all the RKE clusters added to rancher")
			clusterList, err1 = Inst().S.(*rke.Rancher).GetRKEClusterList()
			log.FailOnError(err1, "Getting RKE cluster list")
			log.InfoD("The RKE cluster list is %v", clusterList)
			err := RemoveElementByValue(&clusterList, RancherActiveCluster)

			log.InfoD("Getting the cluster wide PSA setting at the beginning of the testcase")
			currentPSA, err := Inst().S.(*rke.Rancher).GetCurrentClusterWidePSA(clusterList[0])
			log.FailOnError(err, "Fetching cluster wide PSA setting for cluster %v", clusterList[0])
			log.InfoD("Cluster wide PSA for cluster %v is %v", clusterList[0], currentPSA)

			log.InfoD("Getting the list of default PSA templates present in the Rancher")
			psaList, err := Inst().S.(*rke.Rancher).GetPodSecurityAdmissionConfigurationTemplateList()
			log.FailOnError(err, "Getting default PSA list")
			log.InfoD("Default PSA Template list is %v", psaList)
			for _, psa := range psaList.Data {
				if psa.Name == "rancher-restricted" {
					defaultExemptListForRestrictedPSA = psa.Configuration.Exemptions.Namespaces
					break
				}
			}
			log.InfoD("Exempted list of namespaces for default PSA rancher-restricted is %v", defaultExemptListForRestrictedPSA)

			log.InfoD("Creating a custom PSA template with restricted mode")
			psaTemplateDefaults := &rancherClient.PodSecurityAdmissionConfigurationTemplateDefaults{
				Enforce: "restricted",
			}
			pxBackupNS, err := backup.GetPxBackupNamespace()
			log.FailOnError(err, "Getting backup namespace")
			portworxNamespace, err := Inst().S.GetPortworxNamespace()
			log.FailOnError(err, "Getting portworx namespace")
			nsExemptList := []string{"default", pxBackupNS, portworxNamespace}
			newList := AppendList(nsExemptList, defaultExemptListForRestrictedPSA)
			psaTemplateExemptions := &rancherClient.PodSecurityAdmissionConfigurationTemplateExemptions{
				Namespaces: newList,
			}
			err = Inst().S.(*rke.Rancher).CreateCustomPodSecurityAdmissionConfigurationTemplate("custom-restricted-psa3", "Added custom PSA with restricted mode", psaTemplateDefaults, psaTemplateExemptions)
			log.FailOnError(err, "Creating new PSA template with restricted mode")
			Inst().S.(*rke.Rancher).UpdateClusterWidePSA(clusterList[0], "custom-restricted-psa3")
			log.FailOnError(err, "Adding custom PSA with restricted mode")

			log.InfoD("Verifying if custom PSA is applied to the cluster")
			currentPSA, err = Inst().S.(*rke.Rancher).GetCurrentClusterWidePSA(clusterList[0])
			log.FailOnError(err, "Fetching cluster wide PSA setting for cluster %v", clusterList[0])
			log.InfoD("Cluster wide PSA for cluster %v is %v", clusterList[0], currentPSA)
		})
	})

	JustAfterEach(func() {
		log.InfoD("Nothing to be deleted")
	})
})
