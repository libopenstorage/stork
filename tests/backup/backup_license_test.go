package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
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
		srcClusterStatus              api.ClusterInfo_StatusInfo_Status
		destClusterStatus             api.ClusterInfo_StatusInfo_Status
		contexts                      []*scheduler.Context
	)
	JustBeforeEach(func() {
		StartTorpedoTest("NodeCountForLicensing",
			"Verify worker node on application cluster with label portworx.io/nobackup=true is not counted for licensing", nil, 82777)
	})

	It("Verify worker node on application cluster with label portworx.io/nobackup=true is not counted for licensing", func() {
		Step("Registering source and destination clusters for backup", func() {
			log.InfoD("Registering source and destination clusters for backup")
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
			SetDestinationKubeConfig()
			destinationClusterWorkerNodes = node.GetWorkerNodes()
			log.InfoD("Total number of worker nodes in destination cluster are %v", len(destinationClusterWorkerNodes))
			totalNumberOfWorkerNodes = append(totalNumberOfWorkerNodes, destinationClusterWorkerNodes...)
			log.InfoD("Total number of worker nodes in source and destination cluster are %v", len(totalNumberOfWorkerNodes))
			log.InfoD("Switching cluster context back to source cluster")
			err := SetSourceKubeConfig()
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
			SetDestinationKubeConfig()
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
			SetDestinationKubeConfig()
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
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		nodeLabels, err := core.Instance().GetLabelsOnNode(destinationClusterWorkerNodes[0].Name)
		if err != nil {
			dash.VerifySafely(err, nil, fmt.Sprintf("Getting label from worker node %v", destinationClusterWorkerNodes[0].Name))
		}
		for key := range nodeLabels {
			if key == "portworx.io/nobackup" {
				log.InfoD("Removing the applied label portworx.io/nobackup=true from worker nodes on destination cluster at the end of the testcase")
				err = Inst().S.RemoveLabelOnNode(destinationClusterWorkerNodes[0], "portworx.io/nobackup")
				dash.VerifySafely(err, nil, fmt.Sprintf("Removing label portworx.io/nobackup=true from worker node %v", destinationClusterWorkerNodes[0].Name))
				break
			}
		}
		log.InfoD("Switching cluster context back to source cluster")
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster")
		nodeLabels, err = core.Instance().GetLabelsOnNode(sourceClusterWorkerNodes[0].Name)
		if err != nil {
			dash.VerifySafely(err, nil, fmt.Sprintf("Getting label from worker node %v", sourceClusterWorkerNodes[0].Name))
		}
		for key := range nodeLabels {
			if key == "portworx.io/nobackup" {
				log.InfoD("Removing the applied label portworx.io/nobackup=true from worker nodes on source cluster at the end of the testcase")
				err = Inst().S.RemoveLabelOnNode(sourceClusterWorkerNodes[0], "portworx.io/nobackup")
				dash.VerifySafely(err, nil, fmt.Sprintf("Removing label portworx.io/nobackup=true from worker node %v", sourceClusterWorkerNodes[0].Name))
				break
			}
		}
		CleanupCloudSettingsAndClusters(nil, "", "", ctx)
	})
})
