package tests

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

// NodeCountForLicensing applies label portworx.io/nobackup=true on any worker node of application cluster and verifies that this worker node is not counted for licensing
var _ = Describe("{NodeCountForLicensing}", func() {
	var (
		sourceClusterWorkerNodes      []node.Node
		destinationClusterWorkerNodes []node.Node
		totalNumberOfWorkerNodes      []node.Node
		contexts                      []*scheduler.Context
		srcClusterStatus              api.ClusterInfo_StatusInfo_Status
		destClusterStatus             api.ClusterInfo_StatusInfo_Status
	)
	JustBeforeEach(func() {
		StartTorpedoTest("NodeCountForLicensing",
			"Verify worker node on application cluster with label portworx.io/nobackup=true is not counted for licensing", nil, 82777)
	})

	It("Verify worker node on application cluster with label portworx.io/nobackup=true is not counted for licensing", func() {
		Step("Adding source and destination cluster for backup", func() {
			log.InfoD("Adding source and destination cluster for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			log.FailOnError(err, fmt.Sprintf("Creating source cluster %s and destination cluster %s", SourceClusterName, destinationClusterName))
			srcClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			destClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(destClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
		})
		Step("Getting the total number of worker nodes in source and destination cluster", func() {
			log.InfoD("Getting the total number of worker nodes in source and destination cluster")
			sourceClusterWorkerNodes = node.GetWorkerNodes()
			log.InfoD("Total number of worker nodes in source cluster are %v", len(sourceClusterWorkerNodes))
			totalNumberOfWorkerNodes = append(totalNumberOfWorkerNodes, sourceClusterWorkerNodes...)
			log.InfoD("Switching cluster context to destination cluster")
			err := SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			destinationClusterWorkerNodes = node.GetWorkerNodes()
			log.InfoD("Total number of worker nodes in destination cluster are %v", len(destinationClusterWorkerNodes))
			totalNumberOfWorkerNodes = append(totalNumberOfWorkerNodes, destinationClusterWorkerNodes...)
			log.InfoD("Total number of worker nodes in source and destination cluster are %v", len(totalNumberOfWorkerNodes))
			log.InfoD("Switching cluster context back to source cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")
		})
		Step("Verifying the license count after adding source and destination clusters", func() {
			log.InfoD("Verifying the license count after adding source and destination clusters")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = VerifyLicenseConsumedCount(ctx, orgID, int64(len(totalNumberOfWorkerNodes)))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the license count when source cluster with %d worker nodes and destination cluster with %d worker nodes are added to backup", len(sourceClusterWorkerNodes), len(destinationClusterWorkerNodes)))
		})
		Step("Verify worker node on application cluster with label portworx.io/nobackup=true is not counted for licensing", func() {
			log.InfoD("Applying label portworx.io/nobackup=true to one of the worker node on source cluster and verifying the license count")
			err := Inst().S.AddLabelOnNode(sourceClusterWorkerNodes[0], "portworx.io/nobackup", "true")
			log.FailOnError(err, fmt.Sprintf("Failed to apply label portworx.io/nobackup=true to worker node %v", sourceClusterWorkerNodes[0].Name))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = VerifyLicenseConsumedCount(ctx, orgID, int64(len(totalNumberOfWorkerNodes)-1))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying license count after applying label portworx.io/nobackup=true to node %v", sourceClusterWorkerNodes[0].Name))
			log.InfoD("Switching cluster context to destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.InfoD("Applying label portworx.io/nobackup=true to one of the worker node on destination cluster and verifying the license count")
			err = Inst().S.AddLabelOnNode(destinationClusterWorkerNodes[0], "portworx.io/nobackup", "true")
			log.FailOnError(err, fmt.Sprintf("Failed to apply label portworx.io/nobackup=true to worker node %v", destinationClusterWorkerNodes[0].Name))
			log.InfoD("Switching cluster context back to source cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")
			err = VerifyLicenseConsumedCount(ctx, orgID, int64(len(totalNumberOfWorkerNodes)-2))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying license count after applying label portworx.io/nobackup=true to node %v", destinationClusterWorkerNodes[0].Name))
		})
		Step("Removing label portworx.io/nobackup=true from worker nodes and verifying the license count", func() {
			log.InfoD("Removing label from worker node on source cluster on which label was applied earlier and verifying the license count")
			err := Inst().S.RemoveLabelOnNode(sourceClusterWorkerNodes[0], "portworx.io/nobackup")
			log.FailOnError(err, fmt.Sprintf("Failed to remove label portworx.io/nobackup=true from worker node %v", sourceClusterWorkerNodes[0].Name))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = VerifyLicenseConsumedCount(ctx, orgID, int64(len(totalNumberOfWorkerNodes)-1))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying license count after removing label portworx.io/nobackup=true from node %v", sourceClusterWorkerNodes[0].Name))
			log.InfoD("Switching cluster context to destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.InfoD("Removing label from worker node on destination cluster on which label was applied earlier and verifying the license count")
			err = Inst().S.RemoveLabelOnNode(destinationClusterWorkerNodes[0], "portworx.io/nobackup")
			log.FailOnError(err, fmt.Sprintf("Failed to remove label portworx.io/nobackup=true from worker node %v", destinationClusterWorkerNodes[0].Name))
			log.InfoD("Switching cluster context back to source cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")
			err = VerifyLicenseConsumedCount(ctx, orgID, int64(len(totalNumberOfWorkerNodes)))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying license count after removing label portworx.io/nobackup=true from node %v", destinationClusterWorkerNodes[0].Name))
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		err := SetDestinationKubeConfig()
		dash.VerifySafely(err, nil, "Switching context to destination cluster")
		log.InfoD("Removing label portworx.io/nobackup=true from all worker nodes on destination cluster if present")
		for _, workerNode := range destinationClusterWorkerNodes {
			err = RemoveLabelFromNodesIfPresent(workerNode, "portworx.io/nobackup")
			dash.VerifySafely(err, nil, fmt.Sprintf("Removing label portworx.io/nobackup=true from worker node %s", workerNode.Name))
		}
		log.InfoD("Switching cluster context back to source cluster")
		err = SetSourceKubeConfig()
		dash.VerifySafely(err, nil, "Switching context to source cluster")
		log.InfoD("Removing label portworx.io/nobackup=true from all worker nodes on source cluster if present")
		for _, workerNode := range sourceClusterWorkerNodes {
			err = RemoveLabelFromNodesIfPresent(workerNode, "portworx.io/nobackup")
			dash.VerifySafely(err, nil, fmt.Sprintf("Removing label portworx.io/nobackup=true from worker node %s", workerNode.Name))
		}
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(nil, "", "", ctx)
	})
})

// LicensingCountWithNodeLabelledBeforeClusterAddition applies label portworx.io/nobackup=true before adding application cluster to backup and verifies the license count
var _ = Describe("{LicensingCountWithNodeLabelledBeforeClusterAddition}", func() {
	var (
		cloudCredName                 string
		cloudCredUID                  string
		bkpLocationName               string
		backupLocationUID             string
		backupName                    string
		clusterUid                    string
		restoreName                   string
		bkpNamespaces                 []string
		sourceClusterWorkerNodes      []node.Node
		destinationClusterWorkerNodes []node.Node
		totalNumberOfWorkerNodes      []node.Node
		scheduledAppContexts          []*scheduler.Context
	)
	backupLocationMap := make(map[string]string)
	JustBeforeEach(func() {
		StartTorpedoTest("LicensingCountWithNodeLabelledBeforeClusterAddition",
			"Applies label portworx.io/nobackup=true before adding of application cluster to backup and verifies the license count", nil, 82954)
		log.InfoD("Deploy applications needed for taking backup")
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

	It("Label the application cluster nodes before adding to backup and verify the license count", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		Step("Removing any cluster if present", func() {
			clusterEnumerateReq := &api.ClusterEnumerateRequest{
				OrgId: orgID,
			}
			enumerateRsp, err := Inst().Backup.EnumerateCluster(ctx, clusterEnumerateReq)
			log.FailOnError(err, "cluster enumeration failed")
			for _, cluster := range enumerateRsp.GetClusters() {
				err := DeleteCluster(cluster.GetName(), orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting cluster %s", cluster.GetName()))
			}
		})
		Step("Validate applications", func() {
			log.InfoD("Validate applications ")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Creating cloud account and backup location", func() {
			log.InfoD("Creating cloud account and backup location")
			providers := getProviders()
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-%v-bl", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})
		Step("Getting the number of worker nodes in source cluster and applying label portworx.io/nobackup=true to all its worker nodes", func() {
			log.InfoD("Getting the total number of worker nodes in source cluster")
			sourceClusterWorkerNodes = node.GetWorkerNodes()
			log.InfoD("Total number of worker nodes in source cluster are %v", len(sourceClusterWorkerNodes))
			totalNumberOfWorkerNodes = append(totalNumberOfWorkerNodes, sourceClusterWorkerNodes...)

			log.InfoD("Applying label portworx.io/nobackup=true to all the worker node on source cluster")
			for _, workerNode := range sourceClusterWorkerNodes {
				err := Inst().S.AddLabelOnNode(workerNode, "portworx.io/nobackup", "true")
				log.FailOnError(err, fmt.Sprintf("Failed to apply label portworx.io/nobackup=true to source cluster worker node %v", workerNode.Name))
			}
		})
		Step("Getting the number of worker nodes in destination cluster and applying label portworx.io/nobackup=true to all its worker nodes", func() {
			log.InfoD("Switching cluster context to destination cluster")
			err := SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.InfoD("Getting the total number of worker nodes in destination cluster")
			destinationClusterWorkerNodes = node.GetWorkerNodes()
			log.InfoD("Total number of worker nodes in destination cluster are %v", len(destinationClusterWorkerNodes))
			totalNumberOfWorkerNodes = append(totalNumberOfWorkerNodes, destinationClusterWorkerNodes...)
			log.InfoD("Total number of worker nodes in source and destination cluster are %v", len(totalNumberOfWorkerNodes))
			log.InfoD("Applying label portworx.io/nobackup=true to all the worker node on destination cluster")
			for _, workerNode := range destinationClusterWorkerNodes {
				err := Inst().S.AddLabelOnNode(workerNode, "portworx.io/nobackup", "true")
				log.FailOnError(err, fmt.Sprintf("Failed to apply label portworx.io/nobackup=true to destination cluster worker node %v", workerNode.Name))
			}
			log.InfoD("Switching cluster context to source cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
		})
		Step("Verifying the license count before adding source and destination clusters", func() {
			log.InfoD("Verifying the license count before adding source and destination clusters")
			err = VerifyLicenseConsumedCount(ctx, orgID, 0)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying that license count before adding source and destination cluster to backup should be 0"))
		})
		Step("Adding source and destination cluster for backup", func() {
			log.InfoD("Adding source and destination cluster for backup")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			log.FailOnError(err, fmt.Sprintf("Adding source cluster %s and destination cluster %s", SourceClusterName, destinationClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})
		Step("Verifying the license count after adding source and destination clusters with all worker nodes labelled portworx.io/nobackup=true", func() {
			log.InfoD("Verifying the license count after adding source and destination clusters with all worker nodes labelled portworx.io/nobackup=true")
			err = VerifyLicenseConsumedCount(ctx, orgID, 0)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying that license count after adding source and destination clusters with all worker nodes labelled portworx.io/nobackup=true should be 0"))
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking Backup of application")
			backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err := CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, nil, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
		})
		Step("Restoring the backed up application", func() {
			log.InfoD("Restoring the backed up application")
			restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, nil, destinationClusterName, orgID, ctx, nil)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore: %s of backup: %s", backupName, backupName))
		})
		Step("Removing label portworx.io/nobackup=true from worker nodes and verifying the license count", func() {
			log.InfoD("Removing label from one worker node on source cluster and verifying the license count")
			err := Inst().S.RemoveLabelOnNode(sourceClusterWorkerNodes[0], "portworx.io/nobackup")
			log.FailOnError(err, fmt.Sprintf("Failed to remove label portworx.io/nobackup=true from worker node %v", sourceClusterWorkerNodes[0].Name))
			err = VerifyLicenseConsumedCount(ctx, orgID, 1)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying license count after removing label portworx.io/nobackup=true from one worker node: %v should be 1", sourceClusterWorkerNodes[0].Name))
			log.InfoD("Switching cluster context to destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.InfoD("Removing label from all worker nodes on destination cluster and verifying the license count")
			for _, workerNode := range destinationClusterWorkerNodes {
				err = Inst().S.RemoveLabelOnNode(workerNode, "portworx.io/nobackup")
				log.FailOnError(err, fmt.Sprintf("Failed to remove label portworx.io/nobackup=true from worker node %v", workerNode.Name))
			}
			log.InfoD("Switching cluster context back to source cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")
			err = VerifyLicenseConsumedCount(ctx, orgID, int64(len(destinationClusterWorkerNodes))+1)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying license count after removing label portworx.io/nobackup=true all worker node on destination cluster should be %v", len(destinationClusterWorkerNodes)+1))
			log.InfoD("Removing label from rest of all worker nodes on source cluster and verifying the license count")
			for _, workerNode := range sourceClusterWorkerNodes[1:] {
				err = Inst().S.RemoveLabelOnNode(workerNode, "portworx.io/nobackup")
				log.FailOnError(err, fmt.Sprintf("Failed to remove label portworx.io/nobackup=true from worker node %v", workerNode.Name))
			}
			err = VerifyLicenseConsumedCount(ctx, orgID, int64(len(totalNumberOfWorkerNodes)))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying license count after removing label portworx.io/nobackup=true from all worker nodes on source and destination cluster should be %v", len(totalNumberOfWorkerNodes)))
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		err := SetDestinationKubeConfig()
		dash.VerifySafely(err, nil, "Switching context to destination cluster")
		log.InfoD("Removing label portworx.io/nobackup=true from all worker nodes on destination cluster if present")
		for _, workerNode := range destinationClusterWorkerNodes {
			err = RemoveLabelFromNodesIfPresent(workerNode, "portworx.io/nobackup")
			dash.VerifySafely(err, nil, fmt.Sprintf("Removing label portworx.io/nobackup=true from worker node %s", workerNode.Name))
		}
		log.InfoD("Switching cluster context back to source cluster")
		err = SetSourceKubeConfig()
		dash.VerifySafely(err, nil, "Switching context to source cluster")

		log.InfoD("Removing label portworx.io/nobackup=true from all worker nodes on source cluster if present")
		for _, workerNode := range sourceClusterWorkerNodes {
			err = RemoveLabelFromNodesIfPresent(workerNode, "portworx.io/nobackup")
			dash.VerifySafely(err, nil, fmt.Sprintf("Removing label portworx.io/nobackup=true from worker node %s", workerNode.Name))
		}
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the restore taken")
		err = DeleteRestore(restoreName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting the restore taken: %s", restoreName))
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// LicensingCountBeforeAndAfterBackupPodRestart verifies the license count before and after the backup pods restart
var _ = Describe("{LicensingCountBeforeAndAfterBackupPodRestart}", func() {
	var (
		pxbNamespace                  string
		sourceClusterWorkerNodes      []node.Node
		destinationClusterWorkerNodes []node.Node
		totalNumberOfWorkerNodes      []node.Node
		contexts                      []*scheduler.Context
	)
	JustBeforeEach(func() {
		StartTorpedoTest("LicensingCountBeforeAndAfterBackupPodRestart",
			"Verifies the license count before and after the backup pod restarts", nil, 82956)
	})

	It("Verify the license count before and after the backup pod restarts", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		Step("Adding source and destination clusters for backup", func() {
			log.InfoD("Adding source and destination clusters for backup")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			log.FailOnError(err, fmt.Sprintf("Adding source cluster %s and destination cluster %s", SourceClusterName, destinationClusterName))
		})
		Step("Getting the total number of worker nodes in source and destination cluster", func() {
			log.InfoD("Getting the total number of worker nodes in source and destination cluster")
			sourceClusterWorkerNodes = node.GetWorkerNodes()
			log.InfoD("Total number of worker nodes in source cluster are %v", len(sourceClusterWorkerNodes))
			totalNumberOfWorkerNodes = append(totalNumberOfWorkerNodes, sourceClusterWorkerNodes...)
			log.InfoD("Switching cluster context to destination cluster")
			err := SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			destinationClusterWorkerNodes = node.GetWorkerNodes()
			log.InfoD("Total number of worker nodes in destination cluster are %v", len(destinationClusterWorkerNodes))
			totalNumberOfWorkerNodes = append(totalNumberOfWorkerNodes, destinationClusterWorkerNodes...)
			log.InfoD("Total number of worker nodes in source and destination cluster are %v", len(totalNumberOfWorkerNodes))
			log.InfoD("Switching cluster context back to source cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")
		})
		Step("Verifying the license count after adding source and destination clusters", func() {
			log.InfoD("Verifying the license count after adding source and destination clusters")
			err = VerifyLicenseConsumedCount(ctx, orgID, int64(len(totalNumberOfWorkerNodes)))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the license count when source cluster with %d worker nodes and destination cluster with %d worker nodes are added to backup", len(sourceClusterWorkerNodes), len(destinationClusterWorkerNodes)))
		})
		Step("Verify worker nodes on application cluster with label portworx.io/nobackup=true is not counted for licensing before pod restart", func() {
			log.InfoD("Applying label portworx.io/nobackup=true to one of the worker node on source cluster")
			err := Inst().S.AddLabelOnNode(sourceClusterWorkerNodes[0], "portworx.io/nobackup", "true")
			log.FailOnError(err, fmt.Sprintf("Failed to apply label portworx.io/nobackup=true to worker node %v", sourceClusterWorkerNodes[0].Name))
			log.InfoD("Switching cluster context to destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.InfoD("Applying label portworx.io/nobackup=true to one of the worker node on destination cluster")
			err = Inst().S.AddLabelOnNode(destinationClusterWorkerNodes[0], "portworx.io/nobackup", "true")
			log.FailOnError(err, fmt.Sprintf("Failed to apply label portworx.io/nobackup=true to worker node %v", destinationClusterWorkerNodes[0].Name))
			log.InfoD("Switching cluster context to source cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
			log.InfoD("Verify worker nodes with label portworx.io/nobackup=true is not counted for licensing")
			err = VerifyLicenseConsumedCount(ctx, orgID, int64(len(totalNumberOfWorkerNodes)-2))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying license count after applying label portworx.io/nobackup=true to 2 worker nodes"))
		})
		Step("Restart all the backup pod and wait for it to come up", func() {
			pxbNamespace, err = backup.GetPxBackupNamespace()
			log.FailOnError(err, "Getting px-backup namespace")
			err = DeletePodWithLabelInNamespace(pxbNamespace, nil)
			dash.VerifyFatal(err, nil, "Restart all the backup pods")
			log.InfoD("Validate if all the backup pods are up")
			err = ValidateAllPodsInPxBackupNamespace()
			log.FailOnError(err, "Failed to validate pods in px-backup namespace")
		})
		Step("Verify the license count after backup pods restart", func() {
			err = VerifyLicenseConsumedCount(ctx, orgID, int64(len(totalNumberOfWorkerNodes)-2))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying license count after backup pods restart should be same as before pod restart"))
		})
		Step("Label all the remaining worker nodes on source and destination cluster before pod restart", func() {
			log.InfoD("Applying label portworx.io/nobackup=true to all the remaining worker nodes on source cluster before pod restart")
			for _, workerNode := range sourceClusterWorkerNodes[1:] {
				err := Inst().S.AddLabelOnNode(workerNode, "portworx.io/nobackup", "true")
				log.FailOnError(err, fmt.Sprintf("Failed to apply label portworx.io/nobackup=true to source cluster worker node %v", workerNode.Name))
			}
			log.InfoD("Switching cluster context to destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.InfoD("Applying label portworx.io/nobackup=true to all the remaining worker nodes on destination cluster after pod restart")
			for _, workerNode := range destinationClusterWorkerNodes[1:] {
				err := Inst().S.AddLabelOnNode(workerNode, "portworx.io/nobackup", "true")
				log.FailOnError(err, fmt.Sprintf("Failed to apply label portworx.io/nobackup=true to destination cluster worker node %v", workerNode.Name))
			}
			log.InfoD("Switching cluster context to source cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
		})
		Step("Restart all the backup pod again and wait for it to come up", func() {
			err = DeletePodWithLabelInNamespace(pxbNamespace, nil)
			dash.VerifyFatal(err, nil, "Restart all the backup pods")
			log.InfoD("Validate if all the backup pods are up")
			err = ValidateAllPodsInPxBackupNamespace()
			log.FailOnError(err, "Failed to validate pods in px-backup namespace")
		})
		Step("Verify the license count again after pod restart with all the worker nodes labelled portworx.io/nobackup=true", func() {
			err = VerifyLicenseConsumedCount(ctx, orgID, 0)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying license count after applying label portworx.io/nobackup=true to all the worker node"))
		})
		Step("Removing label portworx.io/nobackup=true from all the worker nodes from both source and destination cluster", func() {
			log.InfoD("Removing label from all worker nodes on source cluster")
			for _, workerNode := range sourceClusterWorkerNodes {
				err := Inst().S.RemoveLabelOnNode(workerNode, "portworx.io/nobackup")
				log.FailOnError(err, fmt.Sprintf("Failed to remove label portworx.io/nobackup=true from worker node %v", workerNode.Name))
			}
			log.InfoD("Switching cluster context to destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.InfoD("Removing label from all worker nodes on destination cluster")
			for _, workerNode := range destinationClusterWorkerNodes {
				err := Inst().S.RemoveLabelOnNode(workerNode, "portworx.io/nobackup")
				log.FailOnError(err, fmt.Sprintf("Failed to remove label portworx.io/nobackup=true from worker node %v", workerNode.Name))
			}
			log.InfoD("Switching cluster context to source cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
		})
		Step("Verify the license count when no worker nodes are labelled portworx.io/nobackup=true", func() {
			err = VerifyLicenseConsumedCount(ctx, orgID, int64(len(totalNumberOfWorkerNodes)))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying license count when no worker nodes are labelled portworx.io/nobackup=true"))
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		err := SetDestinationKubeConfig()
		dash.VerifySafely(err, nil, "Switching context to destination cluster")
		log.InfoD("Removing label portworx.io/nobackup=true from all worker nodes on destination cluster if present")
		for _, workerNode := range destinationClusterWorkerNodes {
			err = RemoveLabelFromNodesIfPresent(workerNode, "portworx.io/nobackup")
			dash.VerifySafely(err, nil, fmt.Sprintf("Removing label portworx.io/nobackup=true from worker node %s", workerNode.Name))
		}
		log.InfoD("Switching cluster context back to source cluster")
		err = SetSourceKubeConfig()
		dash.VerifySafely(err, nil, "Switching context to source cluster")
		log.InfoD("Removing label portworx.io/nobackup=true from all worker nodes on source cluster if present")
		for _, workerNode := range sourceClusterWorkerNodes {
			err = RemoveLabelFromNodesIfPresent(workerNode, "portworx.io/nobackup")
			dash.VerifySafely(err, nil, fmt.Sprintf("Removing label portworx.io/nobackup=true from worker node %s", workerNode.Name))
		}
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(nil, "", "", ctx)
	})
})
