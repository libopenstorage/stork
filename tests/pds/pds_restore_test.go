package tests

import (
	"fmt"
	"sync"

	. "github.com/onsi/ginkgo"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	pdsbkp "github.com/portworx/torpedo/drivers/pds/pdsbackup"
	restoreBkp "github.com/portworx/torpedo/drivers/pds/pdsrestore"
	tc "github.com/portworx/torpedo/drivers/pds/targetcluster"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	v1 "k8s.io/api/apps/v1"
)

var (
	restoreTargetCluster   *tc.TargetCluster
	bkpTarget              *pds.ModelsBackupTarget
	bkpTargets             []*pds.ModelsBackupTarget
	dsEntity               restoreBkp.DSEntity
	bkpJob                 *pds.ModelsBackupJobStatusResponse
	restoredDeployment     *pds.ModelsDeployment
	restoredDeployments    []*pds.ModelsDeployment
	deploymentsToBeCleaned []*pds.ModelsDeployment
	deploymentDSentityMap  = make(map[*pds.ModelsDeployment]restoreBkp.DSEntity)
)
var _ = Describe("{PerformRestoreToSameCluster}", func() {
	bkpTargetName = bkpTargetName + pdsbkp.RandString(8)
	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestoreToSameCluster", "Perform multiple restore within same cluster.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", bkpTargetName), deploymentTargetID)
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
		var deploymentsToBeCleaned []*pds.ModelsDeployment
		var wlDeploymentsToBeCleaned []*v1.Deployment
		var deps []*pds.ModelsDeployment
		pdsdeploymentsmd5Hash := make(map[string]string)
		restoredDeploymentsmd5Hash := make(map[string]string)
		stepLog := "Deploy data service and take adhoc backup."
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
					deploymentsToBeCleaned = append(deploymentsToBeCleaned, deployment)
					deps = append(deps, deployment)
					log.FailOnError(err, "Error while deploying data services")

					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
				})
				stepLog = "Running Workloads before taking backups"
				Step(stepLog, func() {
					for _, pdsDeployment := range deps {
						ckSum, wlDep, err := dsTest.InsertDataAndReturnChecksum(pdsDeployment, wkloadParams)
						wlDeploymentsToBeCleaned = append(wlDeploymentsToBeCleaned, wlDep)
						log.FailOnError(err, "Error while Running workloads")
						log.Debugf("Checksum for the deployment %s is %s", *pdsDeployment.ClusterResourceName, ckSum)
						pdsdeploymentsmd5Hash[*pdsDeployment.ClusterResourceName] = ckSum
					}
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
						log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
					}
				})
				stepLog = "Validate md5hash for the restored deployments"
				Step(stepLog, func() {
					log.InfoD(stepLog)
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
	bkpTargetName = bkpTargetName + pdsbkp.RandString(4)
	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestoreToDifferentCluster", "Perform multiple restore to different cluster.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v", bkpTargetName), deploymentTargetID)
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
		var deploymentsToBeCleaned []*pds.ModelsDeployment
		var wlDeploymentsToBeCleanedinSrc []*v1.Deployment
		var wlDeploymentsToBeCleanedinDest []*v1.Deployment
		var deps []*pds.ModelsDeployment
		pdsdeploymentsmd5Hash := make(map[string]string)
		restoredDeploymentsmd5Hash := make(map[string]string)
		stepLog := "Deploy data service and take adhoc backup."
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
					deploymentsToBeCleaned = append(deploymentsToBeCleaned, deployment)
					deps = append(deps, deployment)
					log.FailOnError(err, "Error while deploying data services")
					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
				})
				stepLog = "Running Workloads before taking backups"
				Step(stepLog, func() {
					for _, pdsDeployment := range deps {
						ckSum, wlDep, err := dsTest.InsertDataAndReturnChecksum(pdsDeployment, wkloadParams)
						wlDeploymentsToBeCleanedinSrc = append(wlDeploymentsToBeCleanedinSrc, wlDep)
						log.FailOnError(err, "Error while Running workloads")
						log.Debugf("Checksum for the deployment %s is %s", *pdsDeployment.ClusterResourceName, ckSum)
						pdsdeploymentsmd5Hash[*pdsDeployment.ClusterResourceName] = ckSum
					}
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
		bkpTargetName = "autom-" + pdsbkp.RandString(4)
		var deploymentsToBeCleaned []*pds.ModelsDeployment
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		stepLog := "Create AWS S3 Backup target."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			bkpTarget, err := bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", bkpTargetName), deploymentTargetID)
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
					ctx := pdslib.GetAndExpectStringEnvVar("PDS_RESTORE_TARGET_CLUSTER")
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
	bkpTargetName = bkpTargetName + pdsbkp.RandString(8)
	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestoreToSameCluster", "Perform multiple restore within same cluster.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", bkpTargetName), deploymentTargetID)
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
							ctx := pdslib.GetAndExpectStringEnvVar("PDS_RESTORE_TARGET_CLUSTER")
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
		err = bkpClient.AzureStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
		err = bkpClient.GCPStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{PerformSimultaneousRestoresDifferentDataService}", func() {
	bkpTargetName = bkpTargetName + pdsbkp.RandString(8)
	JustBeforeEach(func() {
		StartTorpedoTest("PerformSimultaneousBackupRestoreForMultipleDeployments", "Perform multiple backup and restore simultaneously for different deployments.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", bkpTargetName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		awsBkpTargets = append(awsBkpTargets, bkpTarget)
	})

	It("Perform multiple restore within same cluster", func() {
		stepLog := "Deploy data service and take adhoc backup, "
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
					ctx := pdslib.GetAndExpectStringEnvVar("PDS_RESTORE_TARGET_CLUSTER")
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
	bkpTargetName = bkpTargetName + pdsbkp.RandString(8)

	JustBeforeEach(func() {
		bkpTargetName = bkpTargetName + pdsbkp.RandString(8)
		StartTorpedoTest("PerformRestoreToSameCluster", "Perform multiple restore within same cluster.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", bkpTargetName), deploymentTargetID)
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
			ctx := pdslib.GetAndExpectStringEnvVar("TARGET_KUBECONFIG")
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
				for _, dep := range deps {
					_, err := pdslib.DeleteDeployment(*dep.Id)
					log.FailOnError(err, "error while deleting deployments")
				}
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
			ctx := pdslib.GetAndExpectStringEnvVar("PDS_RESTORE_TARGET_CLUSTER")
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
	bkpTargetName = bkpTargetName + pdsbkp.RandString(8)
	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestoreAfterPVCResize", "Perform PVC Resize and restore within same cluster.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", bkpTargetName), deploymentTargetID)
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
					ctx := pdslib.GetAndExpectStringEnvVar("PDS_RESTORE_TARGET_CLUSTER")
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
					ctx, err := Inst().Pds.CreateSchedulerContextForPDSApps(depList)
					log.FailOnError(err, "Unable to create scheduler context")
					err = IncreasePVCby1Gig(ctx)
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
					ctx := pdslib.GetAndExpectStringEnvVar("PDS_RESTORE_TARGET_CLUSTER")
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
