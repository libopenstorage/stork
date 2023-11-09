package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	"github.com/portworx/torpedo/drivers/pds/controlplane"
	pdsbkp "github.com/portworx/torpedo/drivers/pds/pdsbackup"
	restoreBkp "github.com/portworx/torpedo/drivers/pds/pdsrestore"
	tc "github.com/portworx/torpedo/drivers/pds/targetcluster"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	v1 "k8s.io/api/apps/v1"
	"math/rand"
	"strconv"
)

var _ = Describe("{ResizeStorageAndRestoreWithVariousFSandRepl}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("ResizeStorageAndRestoreWithVariousFSandRepl", "Perform PVC Resize and validate the updated vol in the storage config also perform restore of the ds", pdsLabels, 0)
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
			TableName:      "wltestingnew",
			Iterations:     params.LoadGen.Iterations,
			FailOnError:    params.LoadGen.FailOnError,
		}
	})

	It("Perform PVC Resize and validate the updated vol in the storage config", func() {

		var (
			resDeployments           []*pds.ModelsDeployment
			updatedDeployment        *pds.ModelsDeployment
			wlDeploymentsToBeCleaned []*v1.Deployment
			deploymentsToBeCleaned   []*pds.ModelsDeployment
			updatedDepList           []*pds.ModelsDeployment
			depList                  []*pds.ModelsDeployment
			stIDs                    []string
			resIds                   []string
			resConfigModelUpdated    *pds.ModelsResourceSettingsTemplate
			stConfigModelUpdated     *pds.ModelsStorageOptionsTemplate
			newResourceTemplateID    string
			newStorageTemplateID     string
			updatedPvcSize           uint64
		)
		pdsdeploymentsmd5Hash2 := make(map[string]string)
		restoredDeploymentsmd5Hash := make(map[string]string)
		restoredDeploymentsmd5Hash2 := make(map[string]string)
		stepLog := "Create Custom Templates , Deploy ds and Trigger Workload"
		Step(stepLog, func() {
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {
				for _, fs := range params.StorageConfigurations.FSType {
					for _, repl := range params.StorageConfigurations.ReplFactor {
						log.InfoD(stepLog)
						log.InfoD("The test will run with following storage/resource template configurations- FilesystemType is - [%v] and RepelFactor is- [%v] ", fs, repl)
						CleanMapEntries(restoredDeploymentsmd5Hash)
						CleanMapEntries(restoredDeploymentsmd5Hash2)
						CleanMapEntries(pdsdeploymentsmd5Hash2)
						stIDs, resIds = nil, nil
						depList, deploymentsToBeCleaned, restoredDeployments = []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}
						wlDeploymentsToBeCleaned = []*v1.Deployment{}
						_, supported := backupSupportedDataServiceNameIDMap[ds.Name]
						if !supported {
							log.InfoD("Data service: %v doesn't support backup, skipping...", ds.Name)
							continue
						}
						deployment, initialCapacity, resConfigModel, stConfigModel, appConfigID, _, pdsDeploymentHash, wkloadParamsold, err := DeployDSWithCustomTemplatesRunWorkloads(ds, tenantID, controlplane.Templates{
							CpuLimit:       params.StorageConfigurations.CpuLimit,
							CpuRequest:     params.StorageConfigurations.CpuRequest,
							MemoryLimit:    params.StorageConfigurations.MemoryLimit,
							MemoryRequest:  params.StorageConfigurations.MemoryRequest,
							StorageRequest: params.StorageConfigurations.StorageRequest,
							FsType:         fs,
							ReplFactor:     repl,
							Provisioner:    "pxd.portworx.com",
							Secure:         false,
							VolGroups:      false,
						})
						stIDs = append(stIDs, stConfigModel.GetId())
						resIds = append(resIds, resConfigModel.GetId())
						depList = append(depList, deployment)
						deploymentsToBeCleaned = append(deploymentsToBeCleaned, deployment)
						log.InfoD("Initial deployment ID- %v", deployment.GetId())
						dataserviceID, _ := dsTest.GetDataServiceID(ds.Name)
						stepLog = "Update the resource/storage template with increased storage size"
						Step(stepLog, func() {
							newTemplateName := "autoTemp-" + strconv.Itoa(rand.Int())
							updatedTemplateConfig := controlplane.Templates{
								CpuLimit:       *resConfigModel.CpuLimit,
								CpuRequest:     *resConfigModel.CpuRequest,
								DataServiceID:  dataserviceID,
								MemoryLimit:    *resConfigModel.MemoryLimit,
								MemoryRequest:  *resConfigModel.MemoryRequest,
								Name:           newTemplateName,
								StorageRequest: params.StorageConfigurations.NewStorageSize,
								FsType:         *stConfigModel.Fs,
								ReplFactor:     *stConfigModel.Repl,
								Provisioner:    *stConfigModel.Provisioner,
								Secure:         false,
								VolGroups:      false,
							}
							stConfigModelUpdated, resConfigModelUpdated, err = controlPlane.CreateCustomResourceTemplate(tenantID, updatedTemplateConfig)
							log.FailOnError(err, "Unable to update template")
							log.InfoD("Successfully updated the template with ID- %v", resConfigModelUpdated.GetId())
							newResourceTemplateID = resConfigModelUpdated.GetId()
							newStorageTemplateID = stConfigModelUpdated.GetId()
							stIDs = append(stIDs, newStorageTemplateID)
							resIds = append(resIds, newResourceTemplateID)
						})
						stepLog = "Apply updated template to the dataservice deployment"
						Step(stepLog, func() {
							log.InfoD(stepLog)
							if appConfigID == "" {
								appConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
								log.FailOnError(err, "Error while fetching AppConfigID")
							}
							updatedDeployment, err = dsTest.UpdateDataServices(deployment.GetId(),
								appConfigID, deployment.GetImageId(),
								int32(ds.Replicas), newResourceTemplateID, params.InfraToTest.Namespace)
							log.FailOnError(err, "Error while updating dataservices")
							Step("Validate Deployments after template update", func() {
								err = dsTest.ValidateDataServiceDeployment(updatedDeployment, namespace)
								log.FailOnError(err, "Error while validating dataservices")
								log.InfoD("Data-service: %v is up and healthy", ds.Name)
								updatedDepList = append(updatedDepList, updatedDeployment)
								updatedPvcSize, err = GetVolumeCapacityInGB(namespace, updatedDeployment)
								log.InfoD("Updated Storage Size is- %v", updatedPvcSize)
								dsEntity = restoreBkp.DSEntity{
									Deployment: updatedDeployment,
								}
							})
							stepLog = "Validate Workload is running after storage resize by creating new workload"
							Step(stepLog, func() {
								ckSum2, wlDep2, err := dsTest.InsertDataAndReturnChecksum(deployment, wkloadParams)
								log.FailOnError(err, "Error while Running workloads-%v", wlDep2)
								log.Debugf("Checksum for the deployment %s is %s", *deployment.ClusterResourceName, ckSum2)
								pdsdeploymentsmd5Hash2[*deployment.ClusterResourceName] = ckSum2
								wlDeploymentsToBeCleaned = append(wlDeploymentsToBeCleaned, wlDep2)
							})
							stepLog = "Verify storage size before and after storage resize - Verify at STS, PV,PVC level"
							Step(stepLog, func() {
								err := ValidateDepConfigPostStorageIncrease(ds, updatedDeployment, stConfigModelUpdated, resConfigModelUpdated, initialCapacity, updatedPvcSize)
								log.FailOnError(err, "Failed to validate DS Volume configuration Post Storage resize")
							})
						})
						stepLog = "Perform backup after PVC Resize"
						Step(stepLog, func() {
							log.InfoD(stepLog)
							log.Infof("Updated Deployment ID: %v, backup target ID: %v", updatedDeployment.GetId(), bkpTarget.GetId())
							err = bkpClient.TriggerAndValidateAdhocBackup(updatedDeployment.GetId(), bkpTarget.GetId(), "s3")
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
								Deployment:           updatedDeployment,
								RestoreTargetCluster: restoreTarget,
							}
							backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, updatedDeployment.GetId())
							log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", updatedDeployment.GetClusterResourceName())
							for _, backupJob := range backupJobs {
								log.InfoD("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
								restoredModel, err := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), params.InfraToTest.Namespace, dsEntity, true, true)
								log.FailOnError(err, "Failed during restore.")
								restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
								log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
								resDeployments = append(restoredDeployments, restoredDeployment)
								deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment)
								log.InfoD("Restored successfully. Deployment- %v", restoredModel.GetClusterResourceName())
							}
						})

						stepLog = "Validate md5hash for the restored deployments"
						Step(stepLog, func() {
							log.InfoD(stepLog)
							for _, pdsDeployment := range resDeployments {
								err := dsTest.ValidateDataServiceDeployment(pdsDeployment, params.InfraToTest.Namespace)
								log.FailOnError(err, "Error while validating deployment before validating checksum")
								ckSum, wlDep, err := dsTest.ReadDataAndReturnChecksum(pdsDeployment, wkloadParamsold)
								ckSum2, wlDep2, err := dsTest.ReadDataAndReturnChecksum(pdsDeployment, wkloadParams)
								wlDeploymentsToBeCleaned = append(wlDeploymentsToBeCleaned, wlDep, wlDep2)
								log.FailOnError(err, "Error while Running workloads")
								log.Debugf("Checksum for the deployment %s is %s", *pdsDeployment.ClusterResourceName, ckSum)
								log.Debugf("Checksum for the deployment2 %s is %s", *pdsDeployment.ClusterResourceName, ckSum2)
								restoredDeploymentsmd5Hash[*pdsDeployment.ClusterResourceName] = ckSum
								restoredDeploymentsmd5Hash2[*pdsDeployment.ClusterResourceName] = ckSum2
							}
							dash.VerifyFatal(dsTest.ValidateDataMd5Hash(pdsDeploymentHash, restoredDeploymentsmd5Hash),
								true, "Validate md5 hash1 after restore")
							dash.VerifyFatal(dsTest.ValidateDataMd5Hash(pdsdeploymentsmd5Hash2, restoredDeploymentsmd5Hash2),
								true, "Validate md5 hash2 after restore")
						})

						Step("Clean up workload deployments", func() {
							for _, wlDep := range wlDeploymentsToBeCleaned {
								err := k8sApps.DeleteDeployment(wlDep.Name, wlDep.Namespace)
								log.FailOnError(err, "Failed while deleting the workload deployment")
							}

						})

						Step("Delete Deployments", func() {
							CleanupDeployments(deploymentsToBeCleaned)
							err := controlPlane.CleanupCustomTemplates(stIDs, resIds)
							log.FailOnError(err, "Failed to delete custom templates")
						})
					}
				}
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		err := bkpClient.AWSStorageClient.DeleteBucket()
		log.FailOnError(err, "Failed while deleting the bucket")
	})
})

var _ = Describe("{ScaleUpDsPostStorageSizeIncreaseVariousRepl}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("ScaleUpDsPostStorageSizeIncreaseVariousRepl", "Scale up the DS and Perform PVC Resize, validate the updated vol in the storage config.", pdsLabels, 0)
		//Initializing the parameters required for workload generation
		wkloadParams = pdsdriver.LoadGenParams{
			LoadGenDepName: params.LoadGen.LoadGenDepName,
			Namespace:      params.InfraToTest.Namespace,
			NumOfRows:      params.LoadGen.NumOfRows,
			Timeout:        params.LoadGen.Timeout,
			Replicas:       params.LoadGen.Replicas,
			TableName:      "wltestingnew",
			Iterations:     params.LoadGen.Iterations,
			FailOnError:    params.LoadGen.FailOnError,
		}
	})

	It("Perform PVC Resize and validate the updated vol in the storage config", func() {

		var (
			updatedDeployment        *pds.ModelsDeployment
			updatedDeployment1       *pds.ModelsDeployment
			wlDeploymentsToBeCleaned []*v1.Deployment
			deploymentsToBeCleaned   []*pds.ModelsDeployment
			updatedDepList           []*pds.ModelsDeployment
			depList                  []*pds.ModelsDeployment
			updatedDepList1          []*pds.ModelsDeployment
			stIds                    []string
			resIds                   []string
			resConfigModelUpdated1   *pds.ModelsResourceSettingsTemplate
			stConfigModelUpdated1    *pds.ModelsStorageOptionsTemplate
			newResourceTemplateID1   string
			newStorageTemplateID1    string
			resConfigModelUpdated2   *pds.ModelsResourceSettingsTemplate
			stConfigModelUpdated2    *pds.ModelsStorageOptionsTemplate
			newResourceTemplateID2   string
			newStorageTemplateID2    string
			updatedPvcSize           uint64
			updatedPvcSize1          uint64
		)

		stepLog := "Create Custom Templates , Deploy ds and Trigger Workload"
		Step(stepLog, func() {
			pdsdeploymentsmd5Hash2 := make(map[string]string)
			for _, ds := range params.DataServiceToTest {
				for _, repl := range params.StorageConfigurations.ReplFactor {
					log.InfoD(stepLog)
					CleanMapEntries(pdsdeploymentsmd5Hash2)
					stIds, resIds = nil, nil
					depList, updatedDepList, updatedDepList1, deploymentsToBeCleaned, restoredDeployments = []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}
					wlDeploymentsToBeCleaned = []*v1.Deployment{}
					log.InfoD("The test will run with following storage/resource template configurations- FilesystemType is - [%v] and RepelFactor is- [%v] ", "xfs", repl)
					deployment, initialCapacity, resConfigModel, stConfigModel, appConfigID, _, _, _, err := DeployDSWithCustomTemplatesRunWorkloads(ds, tenantID, controlplane.Templates{
						CpuLimit:       params.StorageConfigurations.CpuLimit,
						CpuRequest:     params.StorageConfigurations.CpuRequest,
						MemoryLimit:    params.StorageConfigurations.MemoryLimit,
						MemoryRequest:  params.StorageConfigurations.MemoryRequest,
						StorageRequest: params.StorageConfigurations.StorageRequest,
						FsType:         "xfs",
						ReplFactor:     repl,
						Provisioner:    "pxd.portworx.com",
						Secure:         false,
						VolGroups:      false,
					})
					stIds = append(stIds, stConfigModel.GetId())
					resIds = append(resIds, resConfigModel.GetId())
					depList = append(depList, deployment)
					deploymentsToBeCleaned = append(deploymentsToBeCleaned, deployment)
					dataserviceID, _ := dsTest.GetDataServiceID(ds.Name)
					stepLog = "Check PVC for full condition based upto 90% full"
					stepLog = "Scale up the DS with increased storage size and Repl factor as 2 "
					Step(stepLog, func() {
						newTemplateName1 := "autoTemp-" + strconv.Itoa(rand.Int())
						updatedTemplateConfig1 := controlplane.Templates{
							CpuLimit:       *resConfigModel.CpuLimit,
							CpuRequest:     *resConfigModel.CpuRequest,
							DataServiceID:  dataserviceID,
							MemoryLimit:    *resConfigModel.MemoryLimit,
							MemoryRequest:  *resConfigModel.MemoryRequest,
							Name:           newTemplateName1,
							StorageRequest: params.StorageConfigurations.NewStorageSize,
							FsType:         *stConfigModel.Fs,
							ReplFactor:     *stConfigModel.Repl,
							Provisioner:    *stConfigModel.Provisioner,
							Secure:         false,
							VolGroups:      false,
						}
						stConfigModelUpdated1, resConfigModelUpdated1, err = controlPlane.CreateCustomResourceTemplate(tenantID, updatedTemplateConfig1)
						log.FailOnError(err, "Unable to update template")
						log.InfoD("Successfully updated the template with ID- %v and ReplicationFactor- %v", resConfigModelUpdated1.GetId(), updatedTemplateConfig1.ReplFactor)
						newResourceTemplateID1 = resConfigModelUpdated1.GetId()
						newStorageTemplateID1 = stConfigModelUpdated1.GetId()
						stIds = append(stIds, newStorageTemplateID1)
						resIds = append(resIds, newResourceTemplateID1)
					})
					stepLog = "Apply updated template to the dataservice deployment"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						if appConfigID == "" {
							appConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
							log.FailOnError(err, "Error while fetching AppConfigID")
						}
						updatedDeployment, err = dsTest.UpdateDataServices(deployment.GetId(),
							appConfigID, deployment.GetImageId(),
							int32(ds.ScaleReplicas), newResourceTemplateID1, params.InfraToTest.Namespace)
						log.FailOnError(err, "Error while updating dataservices")
						Step("Validate Deployments after template update", func() {
							err = dsTest.ValidateDataServiceDeployment(updatedDeployment, namespace)
							log.FailOnError(err, "Error while validating dataservices")
							log.InfoD("Data-service: %v is up and healthy", ds.Name)
							updatedDepList = append(updatedDepList, updatedDeployment)
							updatedPvcSize, err = GetVolumeCapacityInGB(namespace, updatedDeployment)
							log.InfoD("Updated Storage Size is- %v", updatedPvcSize)
						})
						stepLog = "Validate Workload is running after storage resize by creating new workload"
						Step(stepLog, func() {

							ckSum2, wlDep2, err := dsTest.InsertDataAndReturnChecksum(deployment, wkloadParams)
							log.FailOnError(err, "Error while Running workloads-%v", wlDep2)
							log.Debugf("Checksum for the deployment %s is %s", *deployment.ClusterResourceName, ckSum2)
							pdsdeploymentsmd5Hash2[*deployment.ClusterResourceName] = ckSum2
							wlDeploymentsToBeCleaned = append(wlDeploymentsToBeCleaned, wlDep2)
						})
						stepLog = "Verify storage size before and after storage resize - Verify at STS, PV,PVC level"
						Step(stepLog, func() {
							err := ValidateDepConfigPostStorageIncrease(ds, updatedDeployment, stConfigModelUpdated1, resConfigModelUpdated1, initialCapacity, updatedPvcSize)
							log.FailOnError(err, "Failed to validate DS Volume configuration Post Storage resize")
						})
					})
					stepLog = "Increase the storage size again after Scale-UP"
					Step(stepLog, func() {
						newTemplateName2 := "autoTemp-" + strconv.Itoa(rand.Int())
						updatedTemplateConfig2 := controlplane.Templates{
							CpuLimit:       *resConfigModelUpdated1.CpuLimit,
							CpuRequest:     *resConfigModelUpdated1.CpuRequest,
							DataServiceID:  dataserviceID,
							MemoryLimit:    *resConfigModelUpdated1.MemoryLimit,
							MemoryRequest:  *resConfigModelUpdated1.MemoryRequest,
							Name:           newTemplateName2,
							StorageRequest: params.StorageConfigurations.NewStorageSize,
							FsType:         *stConfigModel.Fs,
							ReplFactor:     *stConfigModel.Repl,
							Provisioner:    *stConfigModelUpdated1.Provisioner,
							Secure:         false,
							VolGroups:      false,
						}
						stConfigModelUpdated2, resConfigModelUpdated2, err = controlPlane.CreateCustomResourceTemplate(tenantID, updatedTemplateConfig2)
						log.FailOnError(err, "Unable to update template")
						log.InfoD("Successfully updated the template with ID- %v and ReplicationFactor- %v", resConfigModelUpdated1.GetId(), updatedTemplateConfig2.ReplFactor)
						newResourceTemplateID2 = resConfigModelUpdated2.GetId()
						newStorageTemplateID2 = stConfigModelUpdated2.GetId()
						stIds = append(stIds, newStorageTemplateID2)
						resIds = append(resIds, newResourceTemplateID2)
					})
					stepLog = "Apply updated template to the dataservice deployment"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						if appConfigID == "" {
							appConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
							log.FailOnError(err, "Error while fetching AppConfigID")
						}
						updatedDeployment1, err = dsTest.UpdateDataServices(deployment.GetId(),
							appConfigID, deployment.GetImageId(),
							int32(ds.ScaleReplicas), newResourceTemplateID2, params.InfraToTest.Namespace)
						log.FailOnError(err, "Error while updating dataservices")
						Step("Validate Deployments after template update", func() {
							err = dsTest.ValidateDataServiceDeployment(updatedDeployment1, namespace)
							log.FailOnError(err, "Error while validating dataservices")
							log.InfoD("Data-service: %v is up and healthy", ds.Name)
							updatedDepList1 = append(updatedDepList1, updatedDeployment1)
							updatedPvcSize1, err = GetVolumeCapacityInGB(namespace, updatedDeployment)
							log.InfoD("Updated Storage Size is- %v", updatedPvcSize1)
						})
						stepLog = "Verify storage size before and after storage resize - Verify at STS, PV,PVC level"
						Step(stepLog, func() {
							err := ValidateDepConfigPostStorageIncrease(ds, updatedDeployment, stConfigModelUpdated2, resConfigModelUpdated2, updatedPvcSize, updatedPvcSize1)
							log.FailOnError(err, "Failed to validate DS Volume configuration Post Storage resize")
						})
					})
					Step("Clean up workload deployments", func() {
						for _, wlDep := range wlDeploymentsToBeCleaned {
							err := k8sApps.DeleteDeployment(wlDep.Name, wlDep.Namespace)
							log.FailOnError(err, "Failed while deleting the workload deployment")
						}
					})

					Step("Delete Deployments", func() {
						CleanupDeployments(deploymentsToBeCleaned)
						err := controlPlane.CleanupCustomTemplates(stIds, resIds)
						log.FailOnError(err, "Failed to delete custom templates")
					})
				}
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{PerformStorageResizeBy1Gb100TimesAllDs}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PerformStorageResizeBy1Gb100TimesAllDs", "Perform PVC Resize by 1GB for 100 times in a loop and validate the updated vol in the storage config.", pdsLabels, 0)
	})

	It("Perform PVC Resize and validate the updated vol in the storage config", func() {

		var (
			updatedDeployment     *pds.ModelsDeployment
			resConfigModelUpdated *pds.ModelsResourceSettingsTemplate
			stConfigModelUpdated  *pds.ModelsStorageOptionsTemplate
			stIds                 []string
			resIds                []string
			tempstIds             []string
			tempresIds            []string
			newResourceTemplateID string
			newStorageTemplateID  string
			updatedPvcSize        uint64
		)
		stepLog := "Create Custom Templates , Deploy ds and Trigger Workload"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, ds := range params.DataServiceToTest {
				stIds, resIds = nil, nil
				wlDeploymentsToBeCleaned := []*v1.Deployment{}
				deploymentsToBeCleaned := []*pds.ModelsDeployment{}
				deployment, initialCapacity, resConfigModel, stConfigModel, appConfigID, wlDep, _, _, err := DeployDSWithCustomTemplatesRunWorkloads(ds, tenantID, controlplane.Templates{
					CpuLimit:       params.StorageConfigurations.CpuLimit,
					CpuRequest:     params.StorageConfigurations.CpuRequest,
					MemoryLimit:    params.StorageConfigurations.MemoryLimit,
					MemoryRequest:  params.StorageConfigurations.MemoryRequest,
					StorageRequest: params.StorageConfigurations.StorageRequest,
					FsType:         "xfs",
					ReplFactor:     2,
					Provisioner:    "pxd.portworx.com",
					Secure:         false,
					VolGroups:      false,
				})
				stIds = append(stIds, stConfigModel.GetId())
				resIds = append(resIds, resConfigModel.GetId())
				deploymentsToBeCleaned = append(deploymentsToBeCleaned, deployment)
				wlDeploymentsToBeCleaned = append(wlDeploymentsToBeCleaned, wlDep)
				log.InfoD("workload deployment is- %v", wlDep.Name)
				stepLog = "Check PVC for full condition based upto 90% full"
				storageSizeCounter := 0
				for i := 2; i <= params.StorageConfigurations.Iterations; i++ {
					tempstIds, tempresIds = nil, nil
					dataserviceID, _ := dsTest.GetDataServiceID(ds.Name)
					log.InfoD("The test is executing for the [%v] iteration", i)
					storageSizeCounter = i
					log.InfoD("Current Storage Size is- %v", strconv.Itoa(storageSizeCounter-1)+"G")
					storageSize := fmt.Sprint(storageSizeCounter, "G")
					log.InfoD("StorageSize calculated for Update is- %v", storageSize)
					stepLog = "Update the resource/storage template with increased storage size"
					Step(stepLog, func() {
						newTemplateName := "autoTemp-" + strconv.Itoa(rand.Int())
						updatedTemplateConfig := controlplane.Templates{
							CpuLimit:       *resConfigModel.CpuLimit,
							CpuRequest:     *resConfigModel.CpuRequest,
							DataServiceID:  dataserviceID,
							MemoryLimit:    *resConfigModel.MemoryLimit,
							MemoryRequest:  *resConfigModel.MemoryRequest,
							Name:           newTemplateName,
							StorageRequest: storageSize,
							FsType:         *stConfigModel.Fs,
							ReplFactor:     *stConfigModel.Repl,
							Provisioner:    *stConfigModel.Provisioner,
							Secure:         false,
							VolGroups:      false,
						}
						stConfigModelUpdated, resConfigModelUpdated, err = controlPlane.CreateCustomResourceTemplate(tenantID, updatedTemplateConfig)
						log.FailOnError(err, "Unable to update template")
						log.InfoD("Successfully updated the template with ID- %v", resConfigModelUpdated.GetId())
						newResourceTemplateID = resConfigModelUpdated.GetId()
						newStorageTemplateID = stConfigModelUpdated.GetId()
						tempstIds = append(tempstIds, newStorageTemplateID)
						tempresIds = append(tempresIds, newResourceTemplateID)
					})
					stepLog = "Apply updated template to the dataservice deployment"
					Step(stepLog, func() {
						if appConfigID == "" {
							appConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
							log.FailOnError(err, "Error while fetching AppConfigID")
						}
						updatedDeployment, err = dsTest.UpdateDataServices(deployment.GetId(),
							appConfigID, deployment.GetImageId(),
							int32(ds.Replicas), newResourceTemplateID, params.InfraToTest.Namespace)
						log.FailOnError(err, "Error while updating dataservices")
						Step("Validate Deployments after template update", func() {
							err = dsTest.ValidateDataServiceDeployment(updatedDeployment, namespace)
							log.FailOnError(err, "Error while validating dataservices")
							log.InfoD("Data-service: %v is up and healthy", ds.Name)
							updatedPvcSize, err = GetVolumeCapacityInGB(namespace, updatedDeployment)
							log.InfoD("Updated Storage Size is- %v", updatedPvcSize)
						})
						stepLog = "Verify storage size before and after storage resize - Verify at STS, PV,PVC level"
						Step(stepLog, func() {
							err := ValidateDepConfigPostStorageIncrease(ds, updatedDeployment, stConfigModelUpdated, resConfigModelUpdated, initialCapacity, updatedPvcSize)
							log.FailOnError(err, "Failed to validate DS Volume configuration Post Storage resize")
						})
						//ToDo: Re-run  Workload to check if I/O is running post update
						//ToDo: Perform backup restores on updated dep
					})
					log.InfoD("Cleanup up custom templates created by iterations")
					err := controlPlane.CleanupCustomTemplates(tempstIds, tempresIds)
					log.FailOnError(err, "Failed to delete custom templates")
				}
				Step("Clean up workload deployments", func() {
					log.Debugf("Deleting Workload Deployment %v in the namespace %v ", wlDep.Name, wlDep.Namespace)
					err := k8sApps.DeleteDeployment(wlDep.Name, params.InfraToTest.Namespace)
					log.FailOnError(err, "Failed while deleting the workload deployment")
				})
				Step("Delete Deployments", func() {
					CleanupDeployments(deploymentsToBeCleaned)
					err := controlPlane.CleanupCustomTemplates(stIds, resIds)
					log.FailOnError(err, "Failed to delete custom templates")
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

// DeployDSWithCustomTemplatesRunWorkloads Deploy dataservice with custom templates and run workloads on them
func DeployDSWithCustomTemplatesRunWorkloads(ds PDSDataService, tenantId string, templates controlplane.Templates) (*pds.ModelsDeployment, uint64, *pds.ModelsResourceSettingsTemplate, *pds.ModelsStorageOptionsTemplate, string, *v1.Deployment, map[string]string, pdsdriver.LoadGenParams, error) {
	var (
		dsVersions             = make(map[string]map[string][]string)
		depList                []*pds.ModelsDeployment
		initialCapacity        uint64
		dataServiceAppConfigID string
		workloadDep            *v1.Deployment
		stConfigModel          *pds.ModelsStorageOptionsTemplate
		resConfigModel         *pds.ModelsResourceSettingsTemplate
	)
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

	pdsdeploymentsmd5Hash1 := make(map[string]string)
	cusTempName := "autoTemp-" + strconv.Itoa(rand.Int())

	dataserviceID, _ := dsTest.GetDataServiceID(ds.Name)
	stConfigModel, resConfigModel, err = controlPlane.CreateCustomResourceTemplate(tenantId, controlplane.Templates{
		CpuLimit:       templates.CpuLimit,
		CpuRequest:     templates.CpuRequest,
		DataServiceID:  dataserviceID,
		MemoryLimit:    templates.MemoryLimit,
		MemoryRequest:  templates.MemoryRequest,
		Name:           cusTempName,
		StorageRequest: templates.StorageRequest,
		FsType:         templates.FsType,
		ReplFactor:     templates.ReplFactor,
		Provisioner:    templates.Provisioner,
		Secure:         templates.Secure,
		VolGroups:      templates.VolGroups})
	log.FailOnError(err, "Unable to create custom templates")
	customStorageTemplateID := stConfigModel.GetId()
	log.InfoD("created storageTemplateName is- %v and ID fetched is- %v ", *stConfigModel.Name, customStorageTemplateID)
	customResourceTemplateID := resConfigModel.GetId()
	log.InfoD("created resourceTemplateName is- %v and ID fetched is- %v ", *resConfigModel.Name, customResourceTemplateID)

	dataServiceAppConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
	log.FailOnError(err, "error while getting app configuration template")
	log.InfoD("ds App config ID is- %v ", dataServiceAppConfigID)

	deploymentsToBeCleaned = []*pds.ModelsDeployment{}
	isDeploymentsDeleted = false
	log.InfoD("Starting to deploy DataService- %v", ds.Name)
	deployment, _, dataServiceVersionBuildMap, err = dsTest.DeployDS(ds.Name, projectID,
		deploymentTargetID,
		dnsZone,
		deploymentName,
		namespaceID,
		dataServiceAppConfigID,
		int32(ds.Replicas),
		serviceType,
		customResourceTemplateID,
		customStorageTemplateID,
		ds.Version,
		ds.Image,
		namespace)
	log.FailOnError(err, "Error while deploying data services")
	err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
	log.FailOnError(err, "Error while validating dataservices")
	log.InfoD("Data-service: %v is up and healthy", ds.Name)
	depList = append(depList, deployment)
	dsVersions[ds.Name] = dataServiceVersionBuildMap
	log.FailOnError(err, "Error while deploying data services")
	initialCapacity, err = GetVolumeCapacityInGB(namespace, deployment)
	log.FailOnError(err, "Error while fetching pvc size for the ds")
	log.InfoD("Initial volume storage size is : %v", initialCapacity)
	dsEntity = restoreBkp.DSEntity{
		Deployment: deployment,
	}
	CleanMapEntries(pdsdeploymentsmd5Hash1)
	ckSum, wlDep, err := dsTest.InsertDataAndReturnChecksum(deployment, wkloadParams)
	workloadDep = wlDep
	log.FailOnError(err, "Error while Running workloads")
	log.Debugf("Checksum for the deployment %s is %s", *deployment.ClusterResourceName, ckSum)
	pdsdeploymentsmd5Hash1[*deployment.ClusterResourceName] = ckSum
	return deployment, initialCapacity, resConfigModel, stConfigModel, dataServiceAppConfigID, workloadDep, pdsdeploymentsmd5Hash1, wkloadParams, nil
}
