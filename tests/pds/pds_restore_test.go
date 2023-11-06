package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	"github.com/portworx/torpedo/drivers/pds/dataservice"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	pdsbkp "github.com/portworx/torpedo/drivers/pds/pdsbackup"
	restoreBkp "github.com/portworx/torpedo/drivers/pds/pdsrestore"
	tc "github.com/portworx/torpedo/drivers/pds/targetcluster"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	v1 "k8s.io/api/apps/v1"
	"sync"
)

var (
	sourceTarget           *tc.TargetCluster
	restoreTargetCluster   *tc.TargetCluster
	bkpTarget              *pds.ModelsBackupTarget
	bkpTargets             []*pds.ModelsBackupTarget
	dsEntity               restoreBkp.DSEntity
	bkpJob                 *pds.ModelsBackupJobStatusResponse
	restoredDeployment     *pds.ModelsDeployment
	restoredDeployments    []*pds.ModelsDeployment
	deploymentsToBeCleaned []*pds.ModelsDeployment
)

const (
	targetName = "automation--"
)

var _ = Describe("{PerformRestoreToSameCluster}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestoreToSameCluster", "Perform multiple restore within same cluster.", pdsLabels, 0)
		credName := targetName + pdsbkp.RandString(8)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		awsBkpTargets = append(awsBkpTargets, bkpTarget)

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

	It("Perform multiple restore within same cluster", func() {
		var deps []*pds.ModelsDeployment
		pdsdeploymentsmd5Hash := make(map[string]string)
		restoredDeploymentsmd5Hash := make(map[string]string)
		stepLog := "Deploy data service and take adhoc backup."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
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

				Step("Delete Deployments", func() {
					CleanupDeployments(deploymentsToBeCleaned)
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{PerformRestoreToDifferentCluster}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestoreToDifferentCluster", "Perform multiple restore to different cluster.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		credName := targetName + pdsbkp.RandString(8)
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		awsBkpTargets = append(awsBkpTargets, bkpTarget)

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

	It("Perform multiple restore to different cluster", func() {
		var deps []*pds.ModelsDeployment
		pdsdeploymentsmd5Hash := make(map[string]string)
		restoredDeploymentsmd5Hash := make(map[string]string)
		stepLog := "Deploy data service and take adhoc backup."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {
				deploymentsToBeCleaned := []*pds.ModelsDeployment{}
				restoredDeployments := []*pds.ModelsDeployment{}
				wlDeploymentsToBeCleanedinSrc := []*v1.Deployment{}
				wlDeploymentsToBeCleanedinDest := []*v1.Deployment{}

				//clearing up the previous entries
				CleanMapEntries(pdsdeploymentsmd5Hash)
				CleanMapEntries(restoredDeploymentsmd5Hash)

				log.InfoD("setting source kubeconfig")
				err = SetSourceKubeConfig()
				log.FailOnError(err, "failed while setting set cluster path")
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
					wlDeploymentsToBeCleanedinSrc = append(wlDeploymentsToBeCleanedinSrc, wlDep)
					log.FailOnError(err, "Error while Running workloads")
					log.Debugf("Checksum for the deployment %s is %s", *deployment.ClusterResourceName, ckSum)
					pdsdeploymentsmd5Hash[*deployment.ClusterResourceName] = ckSum
				})

				defer func() {
					err := CleanupWorkloadDeployments(wlDeploymentsToBeCleanedinSrc, true)
					log.FailOnError(err, "Failed while deleting the workload deployment")
				}()

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
					dest_ctx, err := GetDestinationClusterConfigPath()
					log.FailOnError(err, "failed while getting dest cluster path")
					restoreTarget := tc.NewTargetCluster(dest_ctx)
					restoreClient := restoreBkp.RestoreClient{
						TenantId:             tenantID,
						ProjectId:            projectID,
						Components:           components,
						Deployment:           deployment,
						RestoreTargetCluster: restoreTarget,
					}
					backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
					log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
					log.InfoD("setting destination kubeconfig")
					err = SetDestinationKubeConfig()
					log.FailOnError(err, "failed while setting dest cluster path")
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
						ckSum, wlDep, err := dsTest.ReadDataAndReturnChecksum(pdsDeployment, wkloadParams)
						wlDeploymentsToBeCleanedinDest = append(wlDeploymentsToBeCleanedinDest, wlDep)
						log.FailOnError(err, "Error while Running workloads")
						log.Debugf("Checksum for the deployment %s is %s", *pdsDeployment.ClusterResourceName, ckSum)
						restoredDeploymentsmd5Hash[*pdsDeployment.ClusterResourceName] = ckSum
					}

					dash.VerifyFatal(dsTest.ValidateDataMd5Hash(pdsdeploymentsmd5Hash, restoredDeploymentsmd5Hash),
						true, "Validate md5 hash after restore")

					log.InfoD("Cleaning up workload deployments")
					for _, wlDep := range wlDeploymentsToBeCleanedinDest {
						err := k8sApps.DeleteDeployment(wlDep.Name, wlDep.Namespace)
						log.FailOnError(err, "Failed while deleting the workload deployment")
					}
				})
				Step("Delete Deployments", func() {
					CleanupDeployments(deploymentsToBeCleaned)
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{PerformRestoreFromMultipleBackupTargets}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestoreFromMultipleBackupTargets", "Perform multiple restore having backup present at different cloud object store.", pdsLabels, 0)
	})

	It("Perform multiple restore having backup present at different cloud object store.", func() {
		credName := targetName + pdsbkp.RandString(8)
		var deploymentsToBeCleaned []*pds.ModelsDeployment
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		stepLog := "Create AWS S3 Backup target."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			bkpTarget, err := bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
			log.FailOnError(err, "Failed to create AWS backup target.")
			log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
			bkpTargets = append(bkpTargets, bkpTarget)
		})
		stepLog = "Create GCP Backup target."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			bkpTarget, err := bkpClient.CreateGcpBackupCredsAndTarget(tenantID, fmt.Sprintf("%v-gcp", bkpTargetName))
			log.FailOnError(err, "Failed to create GCP backup target.")
			log.InfoD("GCP Backup target - %v created successfully", bkpTarget.GetName())
			bkpTargets = append(bkpTargets, bkpTarget)
		})
		stepLog = "Create Azure(blob) Backup target."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			bkpTarget, err := bkpClient.CreateAzureBackupCredsAndTarget(tenantID, fmt.Sprintf("%v-azure", bkpTargetName))
			log.FailOnError(err, "Failed to create Azure backup target.")
			log.InfoD("Azure backup target - %v created successfully", bkpTarget.GetName())
			bkpTargets = append(bkpTargets, bkpTarget)
		})
		stepLog = "Deploy data service and take adhoc backup, deleting the data service should not delete the backups."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {
				deploymentsToBeCleaned = []*pds.ModelsDeployment{}
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
					log.FailOnError(err, "Error while deploying data services")

					// TODO: Add workload generation

					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
				})
				stepLog = "Perform adhoc backup and validate them"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					for _, bkpTarget := range bkpTargets {
						log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
						err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "any")
						log.FailOnError(err, "Failed while performing adhoc backup")
					}
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
						log.InfoD("Restored successfully. Deployment- %v", restoredModel.GetClusterResourceName())
					}
				})

				Step("Delete Deployments", func() {
					CleanupDeployments(deploymentsToBeCleaned)
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{PerformSimultaneousRestoresSameDataService}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PerformSimultaneousRestoresSameDataService", "Perform multiple restore within same cluster.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		credName := targetName + pdsbkp.RandString(8)
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		awsBkpTargets = append(awsBkpTargets, bkpTarget)
	})

	It("Perform multiple restore within same cluster", func() {
		var deploymentsToBeCleaned []*pds.ModelsDeployment
		stepLog := "Deploy data service and take adhoc backup."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {
				deploymentsToBeCleaned = []*pds.ModelsDeployment{}
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
					log.FailOnError(err, "Error while deploying data services")

					// TODO: Add workload generation

					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
				})
				stepLog = "Perform adhoc backup and validate them"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup.")
				})
				stepLog = "Perform multiple restores for the backup jobs in parallel."
				Step(stepLog, func() {
					log.InfoD(stepLog)
					numberOfIterations := 3
					var wg sync.WaitGroup
					for i := 0; i < numberOfIterations; i++ {
						log.Infof("Triggering restore operation")
						wg.Add(1)
						go func() {
							defer wg.Done()
							defer GinkgoRecover()
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
								restoredModel, err := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), params.InfraToTest.Namespace, dsEntity, false, true)
								log.FailOnError(err, "Failed during restore.")
								restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
								log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
								deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment)
								log.InfoD("Restored as Deployment: %v successfully!", restoredModel.GetClusterResourceName())
							}
						}()
					}
					wg.Wait()
					log.Infof("Restoring multiple backup job succeeded.")
				})

				Step("Delete Deployments", func() {
					CleanupDeployments(deploymentsToBeCleaned)
				})
			}
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{PerformSimultaneousRestoresDifferentDataService}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PerformSimultaneousBackupRestoreForMultipleDeployments", "Perform multiple backup and restore simultaneously for different deployments.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		credName := targetName + pdsbkp.RandString(8)
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		awsBkpTargets = append(awsBkpTargets, bkpTarget)
	})

	It("Perform multiple restore within same cluster", func() {
		stepLog := "Deploy data service and take adhoc backup, "
		var deploymentDSentityMap = make(map[*pds.ModelsDeployment]restoreBkp.DSEntity)
		deploymentsToBeCleaned = []*pds.ModelsDeployment{}
		restoredDeployments = []*pds.ModelsDeployment{}
		Step(stepLog, func() {
			log.InfoD(stepLog)
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {
				_, supported := backupSupportedDataServiceNameIDMap[ds.Name]
				if !supported {
					log.InfoD("Data service: %v doesn't support backup, skipping...", ds.Name)
					continue
				}
				stepLog = "Deploy and validate data service"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					dsDeployment, _, _, err := DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")

					// TODO: Add workload generation

					dsEntity = restoreBkp.DSEntity{
						Deployment: dsDeployment,
					}
					log.Infof("Details DSObject- %v, Name - %v, DSEntity - %v", dsDeployment, dsDeployment.GetClusterResourceName(), dsEntity)
					deploymentDSentityMap[dsDeployment] = dsEntity
				})
			}
		})
		stepLog = "Perform adhoc backup and validate them for all the deployed data services."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for deployment := range deploymentDSentityMap {
				log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
				err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
				log.FailOnError(err, "Failed while performing adhoc backup")
			}
		})
		stepLog = "Perform multiple restores simultaneously."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			var wg sync.WaitGroup
			for deployment, dsEntity := range deploymentDSentityMap {
				deploymentsToBeCleaned = append(deploymentsToBeCleaned, deployment)
				wg.Add(1)
				log.Info("Triggering restore.")
				go func() {
					defer wg.Done()
					defer GinkgoRecover()
					log.Info("Fetching back up jobs")
					backupJobs, err := components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
					log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
					log.Info("Create restore client.")
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
					for _, backupJob := range backupJobs {
						log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						restoredModel, err := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), params.InfraToTest.Namespace, dsEntity, true, true)
						log.FailOnError(err, "Failed during restore.")
						restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
						log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
						restoredDeployments = append(restoredDeployments, restoredDeployment)
						log.InfoD("Restored successfully. Details: Deployment- %v", restoredModel.GetClusterResourceName())
					}
				}()
			}
			wg.Wait()
			log.Info("Simultaneous backups and restores succeeded.")
			deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployments...)
		})

		Step("Delete all the Deployments.", func() {
			log.Debugf("len of deployments to be cleaned %d", len(deploymentsToBeCleaned))
			for _, dep := range deploymentsToBeCleaned {
				log.Infof("deployment to be deleted %s", *dep.ClusterResourceName)
			}
			CleanupDeployments(deploymentsToBeCleaned)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{PerformRestoreAfterHelmUpgrade}", func() {
	var deps []*pds.ModelsDeployment
	pdsdeploymentsmd5Hash := make(map[string]string)
	restoredDeploymentsmd5Hash := make(map[string]string)
	var deploymentsToBeCleaned []*pds.ModelsDeployment
	var wlDeploymentsToBeCleaned []*v1.Deployment

	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestoreAfterHelmUpgrade", "Perform multiple restore within same cluster.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		credName := targetName + pdsbkp.RandString(8)
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		awsBkpTargets = append(awsBkpTargets, bkpTarget)

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

	It("Perform restore operation after helm upgrade", func() {
		steplog := "Install older helm verison"
		Step(steplog, func() {
			log.InfoD(steplog)
			ctx, err := GetSourceClusterConfigPath()
			log.FailOnError(err, "failed while getting src cluster path")
			target := tc.NewTargetCluster(ctx)
			isOldversion, err := target.IsLatestPDSHelm(params.PDSHelmVersions.PreviousHelmVersion)
			if !isOldversion {
				err = target.DeRegisterFromControlPlane()
				log.FailOnError(err, "Target Cluster DeRegisteration failed")

				err = targetCluster.RegisterClusterToControlPlane(params, tenantID, true)
				log.FailOnError(err, "Target Cluster Registeration failed")
			} else {
				log.InfoD("Target Cluster is with old pds helm version %s", params.PDSHelmVersions.PreviousHelmVersion)
			}
		})

		steplog = "Deploy and validate data service"
		Step(steplog, func() {
			log.InfoD(steplog)
			deps = DeployInANamespaceAndVerify(params.InfraToTest.Namespace)
		})
		defer func() {
			steplog = "Delete created dataservice deployments"
			Step(steplog, func() {
				log.InfoD(steplog)
				CleanupDeployments(deps)
				isDeploymentsDeleted = true
			})
		}()

		steplog = "Running Workloads before taking backups"
		Step(steplog, func() {
			for _, pdsDeployment := range deps {
				ckSum, wlDep, err := dsTest.InsertDataAndReturnChecksum(pdsDeployment, wkloadParams)
				wlDeploymentsToBeCleaned = append(wlDeploymentsToBeCleaned, wlDep)
				log.FailOnError(err, "Error while Running workloads")
				log.Debugf("Checksum for the deployment %s is %s", *pdsDeployment.ClusterResourceName, ckSum)
				pdsdeploymentsmd5Hash[*pdsDeployment.ClusterResourceName] = ckSum
			}
		})

		steplog = "Perform adhoc backup with older helm version and validate them"
		Step(steplog, func() {
			for _, pdsDeployment := range deps {
				log.InfoD(steplog)
				log.Infof("Deployment ID: %v, backup target ID: %v", pdsDeployment.GetId(), bkpTarget.GetId())
				err = bkpClient.TriggerAndValidateAdhocBackup(pdsDeployment.GetId(), bkpTarget.GetId(), "s3")
				log.FailOnError(err, "Failed while performing adhoc backup")
			}
		})

		steplog = "Upgrade target cluster to latest pds helm verison and validate ds deployments"
		Step(steplog, func() {
			log.InfoD(steplog)
			err = targetCluster.RegisterClusterToControlPlane(params, tenantID, false)
			log.FailOnError(err, "Target Cluster Registeration failed")
			for _, dep := range deps {
				err = dsTest.ValidateDataServiceDeployment(dep, namespace)
				log.FailOnError(err, "Error while validating data services")
				log.InfoD("Deployments pods are up and healthy")
			}
		})

		steplog = "Perform adhoc backup with latest helm version and validate them"
		Step(steplog, func() {
			for _, pdsDeployment := range deps {
				log.InfoD(steplog)
				log.Infof("Deployment ID: %v, backup target ID: %v", pdsDeployment.GetId(), bkpTarget.GetId())
				err = bkpClient.TriggerAndValidateAdhocBackup(pdsDeployment.GetId(), bkpTarget.GetId(), "s3")
				log.FailOnError(err, "Failed while performing adhoc backup")
			}
		})

		steplog = "Perform restore for the backup jobs."
		Step(steplog, func() {
			log.InfoD(steplog)
			ctx, err := GetSourceClusterConfigPath()
			log.FailOnError(err, "failed while getting src cluster path")
			restoreTarget := tc.NewTargetCluster(ctx)
			for _, pdsDeployment := range deps {
				restoreClient := restoreBkp.RestoreClient{
					TenantId:             tenantID,
					ProjectId:            projectID,
					Components:           components,
					Deployment:           pdsDeployment,
					RestoreTargetCluster: restoreTarget,
				}
				dsEntity = restoreBkp.DSEntity{
					Deployment: pdsDeployment,
				}
				backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, pdsDeployment.GetId())
				log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", pdsDeployment.GetClusterResourceName())
				for _, backupJob := range backupJobs {
					log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
					restoredModel, err := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), params.InfraToTest.Namespace, dsEntity, true, true)
					log.FailOnError(err, "Failed during restore.")

					restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
					log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
					deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment)
					log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
				}
			}
		})

		defer CleanupDeployments(deploymentsToBeCleaned)

		steplog = "Validate md5hash for the restored deployments"
		Step(steplog, func() {
			log.InfoD(steplog)
			for _, pdsDeployment := range deploymentsToBeCleaned {
				ckSum, wlDep, err := dsTest.ReadDataAndReturnChecksum(pdsDeployment, wkloadParams)
				wlDeploymentsToBeCleaned = append(wlDeploymentsToBeCleaned, wlDep)
				log.FailOnError(err, "Error while Running workloads")
				log.Debugf("Checksum for the deployment %s is %s", *pdsDeployment.ClusterResourceName, ckSum)
				restoredDeploymentsmd5Hash[*pdsDeployment.ClusterResourceName] = ckSum
			}

			defer func() {
				for _, wlDep := range wlDeploymentsToBeCleaned {
					err := k8sApps.DeleteDeployment(wlDep.Name, wlDep.Namespace)
					log.FailOnError(err, "Failed while deleting the workload deployment")
				}
			}()

			dash.VerifyFatal(dsTest.ValidateDataMd5Hash(pdsdeploymentsmd5Hash, restoredDeploymentsmd5Hash),
				true, "Validate md5 hash after restore")
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{PerformRestoreAfterPVCResize}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestoreAfterPVCResize", "Perform PVC Resize and restore within same cluster.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		credName := targetName + pdsbkp.RandString(8)
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		awsBkpTargets = append(awsBkpTargets, bkpTarget)
	})

	It("Perform PVC Resize and restore within same cluster", func() {
		var deploymentsToBeCleaned []*pds.ModelsDeployment
		var deployments = make(map[PDSDataService]*pds.ModelsDeployment)
		var depList []*pds.ModelsDeployment
		stepLog := "Deploy data service and take adhoc backup."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {
				deploymentsToBeCleaned = []*pds.ModelsDeployment{}
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
					log.FailOnError(err, "Error while deploying data services")
					deployments[ds] = deployment
					depList = append(depList, deployment)
					// TODO: Add workload generation

					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
				})
				stepLog = "Perform adhoc backup before PVC Resize"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup.")
				})

				stepLog = "Perform Restore before PVC Resize"
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
						log.InfoD("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						restoredModel, err := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), params.InfraToTest.Namespace, dsEntity, true, true)
						log.FailOnError(err, "Failed during restore.")
						restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
						log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
						deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment)
						log.InfoD("Restored successfully. Deployment- %v", restoredModel.GetClusterResourceName())
					}
				})
				stepLog = "Perform PVC Reisze by 1gb"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					log.FailOnError(err, "Unable to create scheduler context")
					_, err = IncreasePVCby1Gig(namespace, deployment, 1)
					log.FailOnError(err, "Failing while Increasing the PVC name...")
				})
				Step("Validate Deployments after PVC Resize", func() {
					for ds, deployment := range deployments {
						err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
						log.FailOnError(err, "Error while validating dataservices")
						log.InfoD("Data-service: %v is up and healthy", ds.Name)
					}
					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
				})
				stepLog = "Perform backup after PVC Resize"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup.")
				})
				stepLog = "Perform Restore after PVC Resize"
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
						log.InfoD("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						restoredModel, err := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), params.InfraToTest.Namespace, dsEntity, true, true)
						log.FailOnError(err, "Failed during restore.")
						restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
						log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
						deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment)
						log.InfoD("Restored successfully. Deployment- %v", restoredModel.GetClusterResourceName())
					}
				})
				Step("Delete Deployments", func() {
					CleanupDeployments(deploymentsToBeCleaned)
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{PerformSimultaneousBackupRestore}", func() {
	bkpTargetName = bkpTargetName + pdsbkp.RandString(8)
	JustBeforeEach(func() {
		StartTorpedoTest("PerformSimultaneousBackupRestore", "Perform multiple backup and restore simultaneously..", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		credName := targetName + pdsbkp.RandString(8)
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
	})

	It("Perform multiple restore within same cluster", func() {
		var deploymentsToBeCleaned []*pds.ModelsDeployment
		stepLog := "Deploy data service and take adhoc backup, deleting the data service should not delete the backups."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {
				deploymentsToBeCleaned = []*pds.ModelsDeployment{}
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
					log.FailOnError(err, "Error while deploying data services")

					// TODO: Add workload generation

					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
				})
				stepLog = "Perform multiple adhoc backup and validate them"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup")
				})
				var wg sync.WaitGroup
				stepLog = "Perform multiple backups and restores simultaneously."
				Step(stepLog, func() {
					log.InfoD(stepLog)
					numberOfIterations := 3
					log.Info("fetching back up jobs")
					backupJobs, err := components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
					log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())

					log.Info("Create restore client.")
					ctx, err := GetSourceClusterConfigPath()
					log.FailOnError(err, "failed while getting dest cluster path")
					restoreTarget := tc.NewTargetCluster(ctx)
					restoreClient := restoreBkp.RestoreClient{
						TenantId:             tenantID,
						ProjectId:            projectID,
						Components:           components,
						Deployment:           deployment,
						RestoreTargetCluster: restoreTarget,
					}

					log.Info("Triggering backup")
					wg.Add(numberOfIterations)
					for i := 0; i < numberOfIterations; i++ {
						go func() {
							defer wg.Done()
							defer GinkgoRecover()
							log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
							err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
							log.FailOnError(err, "Failed while performing adhoc backup")
						}()
					}
					wg.Add(numberOfIterations)
					for i := 0; i < numberOfIterations; i++ {
						log.Info("Triggering restore")
						go func() {
							defer wg.Done()
							defer GinkgoRecover()
							for _, backupJob := range backupJobs {
								log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
								restoredModel, err := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), params.InfraToTest.Namespace, dsEntity, true, true)
								log.FailOnError(err, "Failed during restore.")
								restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
								log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
								deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment)
								log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
							}
						}()
					}
				})

				wg.Wait()
				log.Info("Multiple adhoc backup and validation succeeded")

				Step("Delete Deployments", func() {
					CleanupDeployments(deploymentsToBeCleaned)
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{PerformRestoreAfterDataServiceVersionUpdate}", func() {
	bkpTargetName = bkpTargetName + pdsbkp.RandString(8)
	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestoreAfterDataServiceVersionUpdate", "Perform restore after ds version update", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", bkpTargetName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		ctx, err := GetSourceClusterConfigPath()
		sourceTarget = tc.NewTargetCluster(ctx)
		log.FailOnError(err, "failed while getting src cluster path")

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
	It("Update DataService and perform backup and restore", func() {
		var (
			deploymentsToClean            []*pds.ModelsDeployment
			restoredDep                   []*pds.ModelsDeployment
			versionUpdatedDsEntity        restoreBkp.DSEntity
			wlDeploymentsToBeCleanedinSrc []*v1.Deployment
			pdsdeploymentsmd5Hash         = make(map[string]string)
			restoreClient                 restoreBkp.RestoreClient
		)
		stepLog := "Deploy data service and take adhoc backup."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			namespace = params.InfraToTest.Namespace
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")

			for _, ds := range params.DataServiceToTest {
				//clearing up the previous entries
				deploymentsToClean = []*pds.ModelsDeployment{}
				wlDeploymentsToBeCleanedinSrc = []*v1.Deployment{}
				CleanMapEntries(pdsdeploymentsmd5Hash)

				_, supported := backupSupportedDataServiceNameIDMap[ds.Name]
				if !supported {
					log.InfoD("Data service: %v doesn't support backup, skipping...", ds.Name)
					continue
				}
				stepLog = "Deploy and validate data service"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					deployment, _, dataServiceVersionBuildMap, err = dsTest.TriggerDeployDataService(ds, namespace, tenantID, projectID, true,
						dataservice.TestParams{StorageTemplateId: storageTemplateID, DeploymentTargetId: deploymentTargetID, DnsZone: dnsZone, ServiceType: serviceType})
					log.FailOnError(err, "Error while deploying data services")
					deploymentsToClean = append(deploymentsToClean, deployment)

					err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
					log.FailOnError(err, "Error while validating data service deployment")

					dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
					log.FailOnError(err, "Error while getting resource template")
					dash.VerifyFatal(dataServiceDefaultResourceTemplateID != "", true, "Validating dataServiceDefaultResourceTemplateID")

					dataServiceDefaultAppConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
					log.FailOnError(err, "Error while getting app configuration template")
					dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")

					ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dataServiceVersionBuildMap)

					for version := range dataServiceVersionBuildMap {
						delete(dataServiceVersionBuildMap, version)
					}

					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}

					stepLog = "Running Workloads before taking backups"
					Step(stepLog, func() {
						ckSum, wlDep, err := dsTest.InsertDataAndReturnChecksum(deployment, wkloadParams)
						wlDeploymentsToBeCleaned := append(wlDeploymentsToBeCleanedinSrc, wlDep)
						log.FailOnError(err, "Error while Running workloads")
						log.Debugf("Checksum for the deployment %s is %s", *deployment.ClusterResourceName, ckSum)
						pdsdeploymentsmd5Hash[*deployment.ClusterResourceName] = ckSum
						wlDeploymentsToBeCleanedinSrc = append(wlDeploymentsToBeCleanedinSrc, wlDeploymentsToBeCleaned...)
					})

					stepLog = "Perform backup before updating data service version"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())

						err := bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
						log.FailOnError(err, "Failed while performing adhoc backup")

						// TODO: Restore for older versions are not supported,
						// once newer versions are released add restore for the same

						err = DeleteAllDsBackupEntities(deployment)
						log.FailOnError(err, "error while deleting backup job")
					})

					stepLog = "Update the data service version and perform backup and restore"
					Step(stepLog, func() {
						log.InfoD(stepLog)

						updatedDeployment, err := pdslib.UpdateDataServiceVerison(deployment.GetDataServiceId(), deployment.GetId(),
							dataServiceDefaultAppConfigID, int32(ds.Replicas), dataServiceDefaultResourceTemplateID, ds.Image, ds.Version)
						log.FailOnError(err, "Error occured while updating data service version")

						err = dsTest.ValidateDataServiceDeployment(updatedDeployment, namespace)
						log.FailOnError(err, "Error while validating data service deployment")

						resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
						log.FailOnError(err, "error on ValidateDataServiceVolumes method")

						_, _, dsVersionBuildMap, err := pdslib.GetVersionsImage(ds.Version, ds.Image, updatedDeployment.GetDataServiceId())
						log.FailOnError(err, "Error while fetching versions/image information")

						log.Debugf("Newly generated map %v", dsVersionBuildMap)

						for version, build := range dsVersionBuildMap {
							log.Debugf("New version :%s   build:%s", version, build)
						}

						ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dsVersionBuildMap)
						dash.VerifyFatal(config.Spec.Version, ds.Version+"-"+ds.Image, "validating ds build and version")

						versionUpdatedDsEntity = restoreBkp.DSEntity{
							Deployment: updatedDeployment,
						}

						stepLog = "Perform backup and restore after ds version update"
						Step(stepLog, func() {
							log.InfoD(stepLog)
							log.Infof("Deployment ID: %v, backup target ID: %v", updatedDeployment.GetId(), bkpTarget.GetId())

							// validate the health status of the deployment before taking backup
							err = dsTest.ValidateDataServiceDeployment(updatedDeployment, namespace)
							log.FailOnError(err, "Error while validating data service deployment")

							err := bkpClient.TriggerAndValidateAdhocBackup(updatedDeployment.GetId(), bkpTarget.GetId(), "s3")
							log.FailOnError(err, "Failed while performing adhoc backup")
							ctx, err := GetSourceClusterConfigPath()
							log.FailOnError(err, "failed while getting src cluster path")
							restoreTarget := tc.NewTargetCluster(ctx)
							restoreClient = restoreBkp.RestoreClient{
								TenantId:             tenantID,
								ProjectId:            projectID,
								Components:           components,
								Deployment:           updatedDeployment,
								RestoreTargetCluster: restoreTarget,
							}

							restoredDep = PerformRestore(restoreClient, versionUpdatedDsEntity, projectID, updatedDeployment)
							deploymentsToClean = append(deploymentsToClean, restoredDep...)

						})
						stepLog = "Validate md5hash for the restored deployments"
						Step(stepLog, func() {
							log.InfoD(stepLog)
							wlDeploymentsToBeCleaned := ValidateDataIntegrityPostRestore(restoredDep, pdsdeploymentsmd5Hash)
							wlDeploymentsToBeCleanedinSrc = append(wlDeploymentsToBeCleanedinSrc, wlDeploymentsToBeCleaned...)
						})
					})

					stepLog = "Clean up the workload deployments"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						for _, wlDep := range wlDeploymentsToBeCleanedinSrc {
							log.Debugf("Deleting workload deployment [%s]", wlDep.Name)
							err := k8sApps.DeleteDeployment(wlDep.Name, wlDep.Namespace)
							log.FailOnError(err, "Failed while deleting the workload deployment")
						}
					})
				})

				Step("Delete Deployments", func() {
					CleanupDeployments(deploymentsToClean)
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{PerformRestoreAfterDataServiceUpdate}", func() {
	bkpTargetName = bkpTargetName + pdsbkp.RandString(8)
	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestoreAfterDataServiceUpdate", "Perform restore after ds update", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", bkpTargetName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		ctx, err := GetSourceClusterConfigPath()
		sourceTarget = tc.NewTargetCluster(ctx)
		log.FailOnError(err, "failed while getting src cluster path")

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
	It("Update DataService and perform backup and restore", func() {
		var (
			deploymentsToClean                []*pds.ModelsDeployment
			restoredOriginalDep               []*pds.ModelsDeployment
			restoredDepPostResourceTempUpdate []*pds.ModelsDeployment
			originalDsEntity                  restoreBkp.DSEntity
			resourceTempUpdatedDsEntity       restoreBkp.DSEntity
			wlDeploymentsToBeCleanedinSrc     []*v1.Deployment
			pdsdeploymentsmd5Hash             = make(map[string]string)
			restoreClient                     restoreBkp.RestoreClient
		)
		stepLog := "Deploy data service and take adhoc backup."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			namespace = params.InfraToTest.Namespace
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")

			for _, ds := range params.DataServiceToTest {
				//clearing up the previous entries
				deploymentsToClean = []*pds.ModelsDeployment{}
				wlDeploymentsToBeCleanedinSrc = []*v1.Deployment{}
				CleanMapEntries(pdsdeploymentsmd5Hash)

				_, supported := backupSupportedDataServiceNameIDMap[ds.Name]
				if !supported {
					log.InfoD("Data service: %v doesn't support backup, skipping...", ds.Name)
					continue
				}
				stepLog = "Deploy and validate data service"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					deployment, _, _, err = DeployandValidateDataServices(ds, namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
					deploymentsToClean = append(deploymentsToClean, deployment)
					originalDsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}

					stepLog = "Running Workloads before taking backups"
					Step(stepLog, func() {
						ckSum, wlDep, err := dsTest.InsertDataAndReturnChecksum(deployment, wkloadParams)
						wlDeploymentsToBeCleaned := append(wlDeploymentsToBeCleanedinSrc, wlDep)
						log.FailOnError(err, "Error while Running workloads")
						log.Debugf("Checksum for the deployment %s is %s", *deployment.ClusterResourceName, ckSum)
						pdsdeploymentsmd5Hash[*deployment.ClusterResourceName] = ckSum
						wlDeploymentsToBeCleanedinSrc = append(wlDeploymentsToBeCleanedinSrc, wlDeploymentsToBeCleaned...)
					})

					stepLog = "Perform backup and restore of the original deployment"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
						err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
						log.FailOnError(err, "Failed while performing adhoc backup")
						ctx, err := GetSourceClusterConfigPath()
						log.FailOnError(err, "failed while getting src cluster path")
						restoreTarget := tc.NewTargetCluster(ctx)
						restoreClient = restoreBkp.RestoreClient{
							TenantId:             tenantID,
							ProjectId:            projectID,
							Components:           components,
							Deployment:           deployment,
							RestoreTargetCluster: restoreTarget,
						}

						restoredOriginalDep = PerformRestore(restoreClient, originalDsEntity, projectID, deployment)
						deploymentsToClean = append(deploymentsToClean, restoredOriginalDep...)

						//Delete the backupJob
						log.InfoD("Deleting the backup job")
						err = DeleteAllDsBackupEntities(deployment)
						log.FailOnError(err, "error while deleting backup job")
					})
					stepLog = "Validate md5hash for the restored deployments"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						wlDeploymentsToBeCleaned := ValidateDataIntegrityPostRestore(restoredOriginalDep, pdsdeploymentsmd5Hash)
						wlDeploymentsToBeCleanedinSrc = append(wlDeploymentsToBeCleanedinSrc, wlDeploymentsToBeCleaned...)
					})

					stepLog = "Update the app config, resource template and scale up the data service"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						// TODO: Update App Config Templates
						dataServiceDefaultAppConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
						log.FailOnError(err, "Error while getting app configuration template")
						dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

						controlPlane.UpdateResourceTemplateName("Medium")

						dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
						log.FailOnError(err, "Error while getting resource setting template")
						dash.VerifyFatal(dataServiceDefaultResourceTemplateID != "", true, "Validating dataServiceDefaultAppConfigID")

						updatedDeployment, err := dsTest.UpdateDataServices(deployment.GetId(),
							dataServiceDefaultAppConfigID, deployment.GetImageId(),
							int32(ds.ScaleReplicas), dataServiceDefaultResourceTemplateID, namespace)
						log.FailOnError(err, "Error while updating data services")

						err = dsTest.ValidateDataServiceDeployment(updatedDeployment, namespace)
						log.FailOnError(err, "Error while validating data service deployment")

						resourceTempUpdatedDsEntity = restoreBkp.DSEntity{
							Deployment: updatedDeployment,
						}

						stepLog = "Perform backup and restore of the resource template updated deployment "
						Step(stepLog, func() {
							log.InfoD(stepLog)
							log.Infof("Deployment ID: %v, backup target ID: %v", updatedDeployment.GetId(), bkpTarget.GetId())

							// validate the health status of the deployment before taking backup
							err = dsTest.ValidateDataServiceDeployment(updatedDeployment, namespace)
							log.FailOnError(err, "Error while validating data service deployment")

							err := bkpClient.TriggerAndValidateAdhocBackup(updatedDeployment.GetId(), bkpTarget.GetId(), "s3")
							log.FailOnError(err, "Failed while performing adhoc backup")
							ctx, err := GetSourceClusterConfigPath()
							log.FailOnError(err, "failed while getting src cluster path")
							restoreTarget := tc.NewTargetCluster(ctx)
							restoreClient = restoreBkp.RestoreClient{
								TenantId:             tenantID,
								ProjectId:            projectID,
								Components:           components,
								Deployment:           updatedDeployment,
								RestoreTargetCluster: restoreTarget,
							}

							restoredDepPostResourceTempUpdate = PerformRestore(restoreClient, resourceTempUpdatedDsEntity, projectID, updatedDeployment)
							deploymentsToClean = append(deploymentsToClean, restoredDepPostResourceTempUpdate...)

							//Delete the backupJob
							log.InfoD("Deleting the backup job")
							err = DeleteAllDsBackupEntities(updatedDeployment)
							log.FailOnError(err, "error while deleting backup job")
						})
						stepLog = "Validate md5hash for the restored deployments"
						Step(stepLog, func() {
							log.InfoD(stepLog)
							wlDeploymentsToBeCleaned := ValidateDataIntegrityPostRestore(restoredDepPostResourceTempUpdate, pdsdeploymentsmd5Hash)
							wlDeploymentsToBeCleanedinSrc = append(wlDeploymentsToBeCleanedinSrc, wlDeploymentsToBeCleaned...)
						})
					})

					stepLog = "Clean up the workload deployments"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						for _, wlDep := range wlDeploymentsToBeCleanedinSrc {
							log.Debugf("Deleting workload deployment [%s]", wlDep.Name)
							err := k8sApps.DeleteDeployment(wlDep.Name, wlDep.Namespace)
							log.FailOnError(err, "Failed while deleting the workload deployment")
						}
					})
				})

				Step("Delete Deployments", func() {
					CleanupDeployments(deploymentsToClean)
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{PerformSimultaneousBackupRestoreForMultipleDeployments}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PerformSimultaneousBackupRestoreForMultipleDeployments", "Perform multiple backup and restore simultaneously for different deployments.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		credName := targetName + pdsbkp.RandString(8)
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
	})

	It("Perform multiple restore within same cluster", func() {
		var deploymentDSentityMap = make(map[*pds.ModelsDeployment]restoreBkp.DSEntity)
		restoredDeployments = []*pds.ModelsDeployment{}
		deploymentsToBeCleaned = []*pds.ModelsDeployment{}
		stepLog := "Deploy data service and take adhoc backup, deleting the data service should not delete the backups."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {
				_, supported := backupSupportedDataServiceNameIDMap[ds.Name]
				if !supported {
					log.InfoD("Data service: %v doesn't support backup, skipping...", ds.Name)
					continue
				}
				stepLog = "Deploy and validate data service"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
					deploymentsToBeCleaned = append(deploymentsToBeCleaned, deployment)

					// TODO: Add workload generation

					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
					deploymentDSentityMap[deployment] = dsEntity
				})
			}
		})
		stepLog = "Perform multiple adhoc backup and validate them"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
			err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
			log.FailOnError(err, "Failed while performing adhoc backup")
		})
		stepLog = "Perform multiple backups and restores simultaneously."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			log.Debugf("len of deploymentDSentityMap %d", len(deploymentDSentityMap))
			for deployment, dsEntity := range deploymentDSentityMap {
				log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
				err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
				log.FailOnError(err, "Failed while performing adhoc backup")
				numberOfIterations := 3
				log.Info("Fetching back up jobs")
				backupJobs, err := components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
				log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())

				log.Info("Create restore client.")
				ctx, err := GetSourceClusterConfigPath()
				log.FailOnError(err, "failed while getting source cluster path")
				restoreTarget := tc.NewTargetCluster(ctx)
				restoreClient := restoreBkp.RestoreClient{
					TenantId:             tenantID,
					ProjectId:            projectID,
					Components:           components,
					Deployment:           deployment,
					RestoreTargetCluster: restoreTarget,
				}

				log.Info("Triggering backup")
				var wg sync.WaitGroup
				wg.Add(numberOfIterations)
				for i := 0; i < numberOfIterations; i++ {
					go func() {
						defer wg.Done()
						defer GinkgoRecover()
						log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
						err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
						log.FailOnError(err, "Failed while performing adhoc backup")
					}()
				}
				wg.Add(numberOfIterations)
				for i := 0; i < numberOfIterations; i++ {
					log.Info("Triggering restore")
					go func() {
						defer wg.Done()
						defer GinkgoRecover()
						for _, backupJob := range backupJobs {
							log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
							restoredModel, err := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), params.InfraToTest.Namespace, dsEntity, true, true)
							log.FailOnError(err, "Failed during restore.")
							restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
							log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
							restoredDeployments = append(restoredDeployments, restoredDeployment)
							log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
						}
					}()
				}
				wg.Wait()
				log.Info("Simultaneous backups and restores succeeded.")
			}
		})

		Step("Delete Deployments", func() {
			CleanupDeployments(restoredDeployments)
			CleanupDeployments(deploymentsToBeCleaned)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{ValidateTransitionalHealthStatus}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("ValidateTransitionalHealthStatus", "Perform restore within same cluster and validates the health status", pdsLabels, 0)
		credName := targetName + pdsbkp.RandString(8)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		awsBkpTargets = append(awsBkpTargets, bkpTarget)

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

	It("Perform restore within same cluster and validate the health status", func() {
		stepLog := "Deploy data service and take adhoc backup."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {
				deploymentsToBeCleaned := []*pds.ModelsDeployment{}

				_, supported := backupSupportedDataServiceNameIDMap[ds.Name]
				if !supported {
					log.InfoD("Data service: %v doesn't support backup, skipping...", ds.Name)
					continue
				}
				stepLog = "Deploy and validate data service"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					// TODO: Intermittent status needs to be validated after temporal changes are in place (DS-7001)
					deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					deploymentsToBeCleaned = append(deploymentsToBeCleaned, deployment)
					log.FailOnError(err, "Error while deploying data services")

					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
				})

				stepLog = "Perform adhoc backup and validate them"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup")
				})
				stepLog = "Perform restore for the backup jobs and validate the health status."
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
					// TODO: List backup jobs api needs to be replaced with get backup jobs
					backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
					log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
					for _, backupJob := range backupJobs {
						log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						// TODO: Intermittent status needs to be validated after temporal changes are in place (DS-7001)
						restoredModel, err := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), params.InfraToTest.Namespace, dsEntity, true, true)
						log.FailOnError(err, "Failed during restore.")
						restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
						log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
						deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment)
						log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
					}
				})
				stepLog = "ScaleUp Restored Deployments and validate the health status"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					var restoredDeployments = make(map[PDSDataService]*pds.ModelsDeployment)
					log.InfoD("Scaling up DataService %v and deployment name %v ", ds.Name, restoredDeployment.GetClusterResourceName())
					restoredDeployments[ds] = restoredDeployment
					// TODO: Intermittent status needs to be validated after temporal changes are in place (DS-7001)
					ScaleUpDeployments(tenantID, restoredDeployments)
				})

				Step("Delete Deployments", func() {
					// TODO: Intermittent status needs to be validated after temporal changes are in place (DS-7001)
					CleanupDeployments(deploymentsToBeCleaned)
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})
