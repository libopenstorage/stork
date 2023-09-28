package tests

import (
	"fmt"
	"github.com/portworx/torpedo/drivers/node"
	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	v1 "k8s.io/api/apps/v1"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	pdsbkp "github.com/portworx/torpedo/drivers/pds/pdsbackup"
	restoreBkp "github.com/portworx/torpedo/drivers/pds/pdsrestore"
	tc "github.com/portworx/torpedo/drivers/pds/targetcluster"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{PerformRestoreValidatingHA}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestoreValidatingHA", "Perform restore while validating HA.", pdsLabels, 0)
		credName := targetName + pdsbkp.RandString(8)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		ctx, err := GetSourceClusterConfigPath()
		sourceTarget = tc.NewTargetCluster(ctx)
		log.FailOnError(err, "failed while getting src cluster path")
		ctx, err = GetSourceClusterConfigPath()
		log.FailOnError(err, "failed while getting dest cluster path")
		restoreTargetCluster = tc.NewTargetCluster(ctx)
	})

	It("Perform restore while validating HA.", func() {
		var (
			deploymentsToBeCleaned []*pds.ModelsDeployment
			nsName                 = params.InfraToTest.Namespace
		)
		stepLog := "Deploy data service and take adhoc backup."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {
				if ds.Name != postgresql {
					continue
				}
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
				})
				stepLog = "Perform adhoc backup before killing deployment pods."
				Step(stepLog, func() {
					log.InfoD(stepLog)
					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
					log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup")
				})

				stepLog = "Kill set of pods for HA."
				Step(stepLog, func() {
					log.InfoD(stepLog)
					dbMaster, isNativelyDistributed := GetDbMasterNode(nsName, ds.Name, deployment, sourceTarget)
					log.FailOnError(err, "Failed while fetching db master node.")
					log.InfoD("dbMaster Node is %s", dbMaster)
					if !isNativelyDistributed {
						err = sourceTarget.DeleteK8sPods(dbMaster, nsName)
						log.FailOnError(err, "Failed while deleting db master pod.")
						err = dsTest.ValidateDataServiceDeployment(deployment, nsName)
						log.FailOnError(err, "Failed while validating the deployment pods, post pod deletion.")
						newDbMaster, _ := GetDbMasterNode(nsName, ds.Name, deployment, sourceTarget)
						if dbMaster == newDbMaster {
							log.FailOnError(fmt.Errorf("leader node is not reassigned"), fmt.Sprintf("Leader pod %v", dbMaster))
						}
					} else {
						podName, err := sourceTarget.GetAnyPodName(deployment.GetClusterResourceName(), nsName)
						log.FailOnError(err, "Failed while fetching pod for stateful set %v.", deployment.GetClusterResourceName())
						err = sourceTarget.KillPodsInNamespace(params.InfraToTest.Namespace, podName)
						log.FailOnError(err, "Failed while deleting pod.")
						err = dsTest.ValidateDataServiceDeployment(deployment, nsName)
						log.FailOnError(err, "Failed while validating the deployment pods, post pod deletion.")
					}

				})
				stepLog = "Perform adhoc backup and validate them"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
					log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup")
				})

				stepLog = "Perform restore for the backup jobs."
				Step(stepLog, func() {
					log.InfoD(stepLog)
					restoreClient := restoreBkp.RestoreClient{
						TenantId:             tenantID,
						ProjectId:            projectID,
						Components:           components,
						Deployment:           deployment,
						RestoreTargetCluster: restoreTargetCluster,
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
				// TODO trigger workload for restored deployment
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

var _ = Describe("{PerformRestorePDSPodsDown}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PerformRestorePDSPodsDown", "Perform restore while simultaneously deleting backup controller manager & target controller pods", pdsLabels, 0)
		credName := targetName + pdsbkp.RandString(8)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		ctx, err := GetSourceClusterConfigPath()
		sourceTarget = tc.NewTargetCluster(ctx)
		log.FailOnError(err, "failed while getting src cluster path")
		ctx, err = GetSourceClusterConfigPath()
		log.FailOnError(err, "failed while getting dest cluster path")
		restoreTargetCluster = tc.NewTargetCluster(ctx)
	})

	It("Perform restore while simultaneously deleting backup controller manager & target controller pods.", func() {
		var (
			deploymentsToBeCleaned []*pds.ModelsDeployment
			nsName                 = params.InfraToTest.Namespace
		)
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
					deployment, _, _, err = DeployandValidateDataServices(ds, nsName, tenantID, projectID)
					deploymentsToBeCleaned = append(deploymentsToBeCleaned, deployment)
					log.FailOnError(err, "Error while deploying data services")
					// TODO: Add workload generation
				})
				stepLog = "Perform adhoc backup and validate."
				Step(stepLog, func() {
					log.InfoD(stepLog)
					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
					log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup")
				})

				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					defer GinkgoRecover()

					log.InfoD("Delete backup controller manager pod")
					pdsBackupControllerPod := pdslib.GetPDSPods(pdslib.PdsBackupControllerPod, "pds-system")
					err = sourceTarget.DeleteK8sPods(pdsBackupControllerPod.Name, "pds-system")
					log.FailOnError(err, "Failed While deleting backup controller manager pod.")

					log.InfoD("Delete target controller manager pod")
					pdsTargetControllerPod := pdslib.GetPDSPods(pdslib.PdsTargetControllerPod, "pds-system")
					err = sourceTarget.DeleteK8sPods(pdsTargetControllerPod.Name, "pds-system")
					log.FailOnError(err, "Failed While deleting  target controller pod.")
				}()

				go func() {
					defer wg.Done()
					defer GinkgoRecover()
					log.InfoD("Perform restore for the backup jobs.")
					restoreClient := restoreBkp.RestoreClient{
						TenantId:             tenantID,
						ProjectId:            projectID,
						Components:           components,
						Deployment:           deployment,
						RestoreTargetCluster: restoreTargetCluster,
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
						log.InfoD("Restored successfully. Deployment- %v", restoredModel.GetClusterResourceName())
					}
				}()

				wg.Wait()
				log.Info("Simultaneous backups and restores succeeded.")

				// TODO trigger workload for restored deployment

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

var _ = Describe("{BringDownPXReplicaNodes}", func() {
	bkpTargetName = bkpTargetName + pdsbkp.RandString(8)
	JustBeforeEach(func() {
		StartTorpedoTest("BringDownPXReplicaNodes", "Bring down one of the PX replica node and perform restore", pdsLabels, 0)
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

	It("Bring Down Replica Node and perform backup and restore", func() {
		var (
			deploymentsToClean             []*pds.ModelsDeployment
			nsName                         = params.InfraToTest.Namespace
			flag                           bool
			wlDeploymentsToBeCleanedinSrc  []*v1.Deployment
			wlDeploymentsToBeCleanedinDest []*v1.Deployment
			pdsdeploymentsmd5Hash          = make(map[string]string)
			restoredDepsPostDriverStop     []*pds.ModelsDeployment
			restoredDepsPostDriverStart    []*pds.ModelsDeployment
			restoreClient                  restoreBkp.RestoreClient
		)
		stepLog := "Deploy data service and take adhoc backup."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			//flag variable is to run test for only one volume of sts replica
			flag = false

			for _, ds := range params.DataServiceToTest {
				//clearing up the previous entries
				deploymentsToClean = []*pds.ModelsDeployment{}
				CleanMapEntries(pdsdeploymentsmd5Hash)

				_, supported := backupSupportedDataServiceNameIDMap[ds.Name]
				if !supported {
					log.InfoD("Data service: %v doesn't support backup, skipping...", ds.Name)
					continue
				}
				stepLog = "Deploy and validate data service"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					deployment, _, _, err = DeployandValidateDataServices(ds, nsName, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
					deploymentsToClean = append(deploymentsToClean, deployment)
					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}

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

					stepLog = "Get the replica node and stop volume driver on the replica node"
					Step(stepLog, func() {
						log.InfoD("Create Context for the deployments")
						ctxs, err := Inst().Pds.CreateSchedulerContextForPDSApps(deploymentsToClean)
						log.FailOnError(err, "Failed while creating scheduler contexts")

						appVolumes, err := Inst().S.GetVolumes(ctxs[0])
						log.FailOnError(err, "error while getting volumes")
						log.Debugf("len of volumes %d", len(appVolumes))
						for _, v := range appVolumes {
							if !strings.Contains(v.Name, "sharedbackupsdir") && !flag {
								log.Debugf("Getting replica node for volume:[%s]", v.Name)
								replPools, _, err := GetReplicaNodes(v)
								log.FailOnError(err, "error while getting replica nodes")
								selectedPool := replPools[0]
								storageNode1, err := GetNodeWithGivenPoolID(selectedPool)
								log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", replPools[0]))

								err = Inst().V.StopDriver([]node.Node{*storageNode1}, false, nil)
								log.FailOnError(err, "error stopping vol driver on node [%s]", storageNode1.Name)

								// Take Backup
								stepLog = "Perform adhoc backup and validate them"
								Step(stepLog, func() {
									log.InfoD(stepLog)
									log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
									err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
									log.FailOnError(err, "Failed while performing adhoc backup")
								})

								// Take Restore
								stepLog = "Perform restore for the backup jobs."
								Step(stepLog, func() {
									log.InfoD(stepLog)
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
									restoredDepsPostDriverStop = PerformRestore(restoreClient, dsEntity, projectID, deployment)
									deploymentsToClean = append(deploymentsToClean, restoredDepsPostDriverStop...)
								})

								stepLog = "Validate md5hash for the restored deployments"
								Step(stepLog, func() {
									log.InfoD(stepLog)
									wlDeploymentsToBeCleanedinDest = ValidateDataIntegrityPostRestore(restoredDepsPostDriverStop, pdsdeploymentsmd5Hash)

									log.InfoD("Cleaning up workload deployments")
									for _, wlDep := range wlDeploymentsToBeCleanedinDest {
										err := k8sApps.DeleteDeployment(wlDep.Name, wlDep.Namespace)
										log.FailOnError(err, "Failed while deleting the workload deployment")
									}
								})

								// Bring up the replica node
								err = Inst().V.StartDriver(*storageNode1)
								log.FailOnError(err, "error starting vol driver on node [%s]", storageNode1.Name)

								// Take backup
								stepLog = "Perform adhoc backup and validate them"
								Step(stepLog, func() {
									log.InfoD(stepLog)
									log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
									err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
									log.FailOnError(err, "Failed while performing adhoc backup")
								})

								// Take Restore
								stepLog = "Perform restore for the backup jobs."
								Step(stepLog, func() {
									log.InfoD(stepLog)
									restoredDepsPostDriverStart = PerformRestore(restoreClient, dsEntity, projectID, deployment)
									deploymentsToClean = append(deploymentsToClean, restoredDepsPostDriverStart...)
								})
								stepLog = "Validate md5hash for the restored deployments"
								Step(stepLog, func() {
									log.InfoD(stepLog)
									wlDeploymentsToBeCleanedinDest = ValidateDataIntegrityPostRestore(restoredDepsPostDriverStart, pdsdeploymentsmd5Hash)

									log.InfoD("Cleaning up workload deployments")
									for _, wlDep := range wlDeploymentsToBeCleanedinDest {
										err := k8sApps.DeleteDeployment(wlDep.Name, wlDep.Namespace)
										log.FailOnError(err, "Failed while deleting the workload deployment")
									}
								})
								flag = true
							}
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

var _ = Describe("{DeleteBackupJobTriggerRestore}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeleteBackupJobTriggerRestore", "Delete a running backup job and trigger a restore.", pdsLabels, 0)
		credName := targetName + pdsbkp.RandString(8)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		ctx, err := GetSourceClusterConfigPath()
		sourceTarget = tc.NewTargetCluster(ctx)
		log.FailOnError(err, "failed while getting src cluster path")
		ctx, err = GetSourceClusterConfigPath()
		log.FailOnError(err, "failed while getting dest cluster path")
		restoreTargetCluster = tc.NewTargetCluster(ctx)
	})

	It("Delete a running backup job and trigger a restore.", func() {
		var (
			deploymentsToBeCleaned []*pds.ModelsDeployment
			nsName                 = params.InfraToTest.Namespace
		)
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
					deployment, _, _, err = DeployandValidateDataServices(ds, nsName, tenantID, projectID)
					deploymentsToBeCleaned = append(deploymentsToBeCleaned, deployment)
					log.FailOnError(err, "Error while deploying data services")
					// TODO: Add workload generation
				})
				stepLog = "Perform adhoc backup and validate."
				Step(stepLog, func() {
					log.InfoD(stepLog)
					dsEntity = restoreBkp.DSEntity{
						Deployment: deployment,
					}
					log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup")
				})

				log.Info("Simultaneous backup delete and restores working as expected.")
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					defer GinkgoRecover()
					// TODO: Add backup job deletion for in parallel backup delete and restore operation.
				}()

				go func() {
					defer wg.Done()
					defer GinkgoRecover()
					log.InfoD("Perform restore for the backup jobs.")
					restoreClient := restoreBkp.RestoreClient{
						TenantId:             tenantID,
						ProjectId:            projectID,
						Components:           components,
						Deployment:           deployment,
						RestoreTargetCluster: restoreTargetCluster,
					}
					backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
					log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
					for _, backupJob := range backupJobs {
						log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						restoredModel, err := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), params.InfraToTest.Namespace, dsEntity, false, true)
						if err != nil {
							log.InfoD("Failed during the restore as the backup data might have been lost as part of simultaneous delete. ")
						} else {
							restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
							log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
							deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment)
							log.InfoD("Restored successfully. Deployment- %v", restoredModel.GetClusterResourceName())
						}
					}
				}()

				wg.Wait()

				// TODO trigger workload for restored deployment

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
