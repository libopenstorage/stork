package tests

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	restoreBkp "github.com/portworx/torpedo/drivers/pds/pdsrestore"
	tc "github.com/portworx/torpedo/drivers/pds/targetcluster"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	v1 "k8s.io/api/apps/v1"
)

var _ = Describe("{PerformPDSWorkflowMultipleIterations}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PerformPerformPDSWorkflowMultipleIterations", "SSIE: Perform multiple Iterations of deploy->backup->restore->unregister->register", pdsLabels, 0)

		//Initializing the parameters required for workload generation
		wkloadParams = pdsdriver.LoadGenParams{
			LoadGenDepName: params.LoadGen.LoadGenDepName,
			Namespace:      params.InfraToTest.Namespace,
			NumOfRows:      params.LoadGen.NumOfRows,
			Timeout:        params.LoadGen.Timeout,
			Replicas:       params.LoadGen.Replicas,
			TableName:      params.LoadGen.TableName,
			Iterations:     params.LoadGen.Iterations,
			FailOnError:    params.LoadGen.FailOnError,
		}
	})

	isRegistered := true
	It("Perform multiple Iterations of deploy, backup, restore and deregister within same cluster", func() {
		var deps []*pds.ModelsDeployment
		pdsdeploymentsmd5Hash := make(map[string]string)
		restoredDeploymentsmd5Hash := make(map[string]string)
		backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
		log.FailOnError(err, "Error while fetching the backup supported ds.")

		for index := 1; index <= params.SSIE.NumIterations; index++ {
			stepLog := "Deploy data services"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				for _, ds := range params.DataServiceToTest {
					deploymentsToBeCleaned := []*pds.ModelsDeployment{}
					restoredDeployments := []*pds.ModelsDeployment{}
					wlDeploymentsToBeCleaned := []*v1.Deployment{}

					//clearing up the previous entries
					CleanMapEntries(pdsdeploymentsmd5Hash)
					CleanMapEntries(restoredDeploymentsmd5Hash)

					_, supported := backupSupportedDataServiceNameIDMap[ds.Name]
					if !supported {
						log.InfoD("Data service: %v doesn't support backup, skipping...", ds.Name)
						continue
					}
					stepLog = "Deploy and validate data service"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
						deploymentsToBeCleaned = append(deploymentsToBeCleaned, deployment)
						deps = append(deps, deployment)
						log.FailOnError(err, "Error while deploying data services")

						dsEntity = restoreBkp.DSEntity{
							Deployment: deployment,
						}
					})
					stepLog = "Running Workloads before taking backups"
					Step(stepLog, func() {
						ckSum, wlDep, err := dsTest.InsertDataAndReturnChecksum(deployment, wkloadParams)
						wlDeploymentsToBeCleaned = append(wlDeploymentsToBeCleaned, wlDep)
						log.FailOnError(err, "Error while Running workloads")
						log.Debugf("Checksum for the deployment %s is %s", *deployment.ClusterResourceName, ckSum)
						pdsdeploymentsmd5Hash[*deployment.ClusterResourceName] = ckSum

					})
					stepLog = "Perform adhoc backup and validate them"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
						err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
						log.FailOnError(err, "Failed while performing adhoc backup")
					})
					stepLog = "Perform restore for the backup jobs."
					Step(stepLog, func() {
						log.InfoD(stepLog)
						ctx, err := GetSourceClusterConfigPath()
						log.FailOnError(err, "failed while getting src cluster path")
						restoreTarget := tc.NewTargetCluster(ctx)
						restoreClient := restoreBkp.RestoreClient{
							TenantId:             tenantID,
							ProjectId:            projectID,
							Components:           components,
							Deployment:           deployment,
							RestoreTargetCluster: restoreTarget,
						}
						backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
						log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
						for _, backupJob := range backupJobs {
							log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
							restoredModel, err := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), params.InfraToTest.Namespace, dsEntity, true, true)
							log.FailOnError(err, "Failed during restore.")
							restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
							log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
							deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment)
							restoredDeployments = append(restoredDeployments, restoredDeployment)
							log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
						}
					})
					stepLog = "Validate md5hash for the restored deployments"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						for _, pdsDeployment := range restoredDeployments {
							err := dsTest.ValidateDataServiceDeployment(pdsDeployment, params.InfraToTest.Namespace)
							log.FailOnError(err, "Error while validating deployment before validating checksum")
							ckSum, wlDep, err := dsTest.ReadDataAndReturnChecksum(pdsDeployment, wkloadParams)
							wlDeploymentsToBeCleaned = append(wlDeploymentsToBeCleaned, wlDep)
							log.FailOnError(err, "Error while Running workloads")
							log.Debugf("Checksum for the deployment %s is %s", *pdsDeployment.ClusterResourceName, ckSum)
							restoredDeploymentsmd5Hash[*pdsDeployment.ClusterResourceName] = ckSum
						}

						dash.VerifyFatal(dsTest.ValidateDataMd5Hash(pdsdeploymentsmd5Hash, restoredDeploymentsmd5Hash),
							true, "Validate md5 hash after restore")
					})

					Step("Clean up workload deployments", func() {
						for _, wlDep := range wlDeploymentsToBeCleaned {
							err := k8sApps.DeleteDeployment(wlDep.Name, wlDep.Namespace)
							log.FailOnError(err, "Failed while deleting the workload deployment")
						}
					})

					Step("Delete Deployments, backUp targets and creds", func() {
						CleanupDeployments(deploymentsToBeCleaned)
					})

					steplog := "De-Register Target Cluster"
					Step(steplog, func() {
						log.InfoD(steplog)
						ctx := pdslib.GetAndExpectStringEnvVar("TARGET_KUBECONFIG")
						target := tc.NewTargetCluster(ctx)
						err = target.DeRegisterFromControlPlane()
						isRegistered = false
						log.FailOnError(err, "Error occurred while de-registering target cluster")
					})

					steplog = "Check and Register Target Cluster to ControlPlane"
					Step(steplog, func() {
						log.InfoD(steplog)
						err = targetCluster.RegisterClusterToControlPlane(params, tenantID, false)
						log.FailOnError(err, "Target Cluster Registeration failed")

						deploymentTarget, err := pdslib.ValidatePDSDeploymentTargetHealthStatus(deploymentTargetID, "healthy")
						log.FailOnError(err, "Error while getting deployment target status")
						dash.VerifyFatal(deploymentTarget.GetStatus(), "healthy", "Validating Deployment Target health status")
						isRegistered = true
					})
				}
			})
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		if !isRegistered {
			steplog := "Re-register Target Cluster to ControlPlane"
			Step(steplog, func() {
				log.InfoD(steplog)
				err = targetCluster.RegisterClusterToControlPlane(params, tenantID, false)
				log.FailOnError(err, "Target Cluster Registeration failed")

				deploymentTarget, err := pdslib.ValidatePDSDeploymentTargetHealthStatus(deploymentTargetID, "healthy")
				log.FailOnError(err, "Error while getting deployment target status")
				dash.VerifyFatal(deploymentTarget.GetStatus(), "healthy", "Validating Deployment Target health status")
			})
		}
	})
})
