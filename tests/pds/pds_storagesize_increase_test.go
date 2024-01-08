package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	"github.com/portworx/torpedo/drivers/pds/controlplane"
	_ "github.com/portworx/torpedo/drivers/pds/pdsbackup"
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
			increasedStorageSize     uint64
			beforeResizePodAge       float64
		)
		pdsdeploymentsmd5HashUpdated := make(map[string]string)
		restoredDeploymentsmd5Hash := make(map[string]string)
		restoredDeploymentsmd5HashUpdated := make(map[string]string)
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
						CleanMapEntries(restoredDeploymentsmd5HashUpdated)
						CleanMapEntries(pdsdeploymentsmd5HashUpdated)
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
						beforeResizePodAge, err = GetPodAge(deployment, params.InfraToTest.Namespace)
						log.FailOnError(err, "unable to get pods AGE before Storage resize")
						log.InfoD("Pods Age before storage resize is- [%v]Min", beforeResizePodAge)
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
								increasedStorageSize, err = GetVolumeCapacityInGB(namespace, updatedDeployment)
								log.InfoD("Increased Storage Size is- %v", increasedStorageSize)
								dsEntity = restoreBkp.DSEntity{
									Deployment: updatedDeployment,
								}
							})
							stepLog = "Validate Workload is running after storage resize by creating new workload"
							Step(stepLog, func() {
								ckSum2, wlDep2, err := dsTest.InsertDataAndReturnChecksum(deployment, wkloadParams)
								log.FailOnError(err, "Error while Running workloads-%v", wlDep2)
								log.Debugf("Checksum for the deployment %s is %s", *deployment.ClusterResourceName, ckSum2)
								pdsdeploymentsmd5HashUpdated[*deployment.ClusterResourceName] = ckSum2
								wlDeploymentsToBeCleaned = append(wlDeploymentsToBeCleaned, wlDep2)
							})
							stepLog = "Verify storage size before and after storage resize - Verify at STS, PV,PVC level"
							Step(stepLog, func() {
								stIncrease := validateStorageIncrease{
									UpdatedDeployment:    updatedDeployment,
									StConfigUpdated:      stConfigModelUpdated,
									ResConfigUpdated:     resConfigModelUpdated,
									InitialCapacity:      initialCapacity,
									IncreasedStorageSize: increasedStorageSize,
									BeforeResizePodAge:   beforeResizePodAge,
								}
								err := ValidateDepConfigPostStorageIncrease(ds, &stIncrease)
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
								log.Debugf("Checksum for the updated-deployment %s is %s", *pdsDeployment.ClusterResourceName, ckSum2)
								restoredDeploymentsmd5Hash[*pdsDeployment.ClusterResourceName] = ckSum
								restoredDeploymentsmd5HashUpdated[*pdsDeployment.ClusterResourceName] = ckSum2
							}
							dash.VerifyFatal(dsTest.ValidateDataMd5Hash(pdsDeploymentHash, restoredDeploymentsmd5Hash),
								true, "Validate md5 hash1 after restore")
							dash.VerifyFatal(dsTest.ValidateDataMd5Hash(pdsdeploymentsmd5HashUpdated, restoredDeploymentsmd5HashUpdated),
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
			updatedDeployment            *pds.ModelsDeployment
			scaledUpdatedDeployment      *pds.ModelsDeployment
			wlDeploymentsToBeCleaned     []*v1.Deployment
			deploymentsToBeCleaned       []*pds.ModelsDeployment
			updatedDepList               []*pds.ModelsDeployment
			depList                      []*pds.ModelsDeployment
			scaledUpdatedDepList         []*pds.ModelsDeployment
			stIds                        []string
			resIds                       []string
			resConfigModelUpdated        *pds.ModelsResourceSettingsTemplate
			stConfigModelUpdated         *pds.ModelsStorageOptionsTemplate
			newResourceTemplateID        string
			newStorageTemplateID         string
			scaledUpdatedResConfigModel  *pds.ModelsResourceSettingsTemplate
			scaledUpdatedstConfigModel   *pds.ModelsStorageOptionsTemplate
			scaledNewResourceTemplateID  string
			scaledNewStorageTemplateID   string
			increasedPvcSize             uint64
			increasedPvcSizeAfterScaleUp uint64
			beforeResizePodAge           float64
		)

		stepLog := "Create Custom Templates , Deploy ds and Trigger Workload"
		Step(stepLog, func() {
			pdsdeploymentsmd5HashAfterResize := make(map[string]string)
			for _, ds := range params.DataServiceToTest {
				for _, repl := range params.StorageConfigurations.ReplFactor {
					log.InfoD(stepLog)
					CleanMapEntries(pdsdeploymentsmd5HashAfterResize)
					stIds, resIds = nil, nil
					depList, updatedDepList, scaledUpdatedDepList, deploymentsToBeCleaned, restoredDeployments = []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}
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
					beforeResizePodAge, err = GetPodAge(deployment, params.InfraToTest.Namespace)
					log.FailOnError(err, "unable to get pods AGE before Storage resize")
					log.InfoD("Pods Age before storage resize is- [%v]Min", beforeResizePodAge)

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
						stConfigModelUpdated, resConfigModelUpdated, err = controlPlane.CreateCustomResourceTemplate(tenantID, updatedTemplateConfig1)
						log.FailOnError(err, "Unable to update template")
						log.InfoD("Successfully updated the template with ID- %v and ReplicationFactor- %v", resConfigModelUpdated.GetId(), updatedTemplateConfig1.ReplFactor)
						newResourceTemplateID = resConfigModelUpdated.GetId()
						newStorageTemplateID = stConfigModelUpdated.GetId()
						stIds = append(stIds, newStorageTemplateID)
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
							int32(ds.ScaleReplicas), newResourceTemplateID, params.InfraToTest.Namespace)
						log.FailOnError(err, "Error while updating dataservices")
						Step("Validate Deployments after template update", func() {
							err = dsTest.ValidateDataServiceDeployment(updatedDeployment, namespace)
							log.FailOnError(err, "Error while validating dataservices")
							log.InfoD("Data-service: %v is up and healthy", ds.Name)
							updatedDepList = append(updatedDepList, updatedDeployment)
							increasedPvcSize, err = GetVolumeCapacityInGB(namespace, updatedDeployment)
							log.InfoD("Increased Storage Size is- %v", increasedPvcSize)
						})
						stepLog = "Validate Workload is running after storage resize by creating new workload"
						Step(stepLog, func() {

							ckSum2, wlDep2, err := dsTest.InsertDataAndReturnChecksum(deployment, wkloadParams)
							log.FailOnError(err, "Error while Running workloads-%v", wlDep2)
							log.Debugf("Checksum for the deployment %s is %s", *deployment.ClusterResourceName, ckSum2)
							pdsdeploymentsmd5HashAfterResize[*deployment.ClusterResourceName] = ckSum2
							wlDeploymentsToBeCleaned = append(wlDeploymentsToBeCleaned, wlDep2)
						})
						stepLog = "Verify storage size before and after storage resize - Verify at STS, PV,PVC level"
						Step(stepLog, func() {
							stIncrease := validateStorageIncrease{
								UpdatedDeployment:    updatedDeployment,
								StConfigUpdated:      stConfigModelUpdated,
								ResConfigUpdated:     resConfigModelUpdated,
								InitialCapacity:      initialCapacity,
								IncreasedStorageSize: increasedPvcSize,
								BeforeResizePodAge:   beforeResizePodAge,
							}
							err := ValidateDepConfigPostStorageIncrease(ds, &stIncrease)
							log.FailOnError(err, "Failed to validate DS Volume configuration Post Storage resize")
						})
					})
					stepLog = "Increase the storage size again after Scale-UP"
					Step(stepLog, func() {
						newTemplateName2 := "autoTemp-" + strconv.Itoa(rand.Int())
						currStorageSize := *resConfigModelUpdated.StorageRequest
						newStorageSize, _ := strconv.Atoi(currStorageSize[:len(currStorageSize)-1])
						updatedStorage := strconv.Itoa(newStorageSize+10) + "G"
						updatedTemplateConfig2 := controlplane.Templates{
							CpuLimit:       *resConfigModelUpdated.CpuLimit,
							CpuRequest:     *resConfigModelUpdated.CpuRequest,
							DataServiceID:  dataserviceID,
							MemoryLimit:    *resConfigModelUpdated.MemoryLimit,
							MemoryRequest:  *resConfigModelUpdated.MemoryRequest,
							Name:           newTemplateName2,
							StorageRequest: updatedStorage,
							FsType:         *stConfigModel.Fs,
							ReplFactor:     *stConfigModel.Repl,
							Provisioner:    *stConfigModelUpdated.Provisioner,
							Secure:         false,
							VolGroups:      false,
						}
						scaledUpdatedstConfigModel, scaledUpdatedResConfigModel, err = controlPlane.CreateCustomResourceTemplate(tenantID, updatedTemplateConfig2)
						log.FailOnError(err, "Unable to update template")
						log.InfoD("Successfully updated the template with ID- %v and ReplicationFactor- %v", resConfigModelUpdated.GetId(), updatedTemplateConfig2.ReplFactor)
						scaledNewResourceTemplateID = scaledUpdatedResConfigModel.GetId()
						scaledNewStorageTemplateID = scaledUpdatedstConfigModel.GetId()
						stIds = append(stIds, scaledNewStorageTemplateID)
						resIds = append(resIds, scaledNewResourceTemplateID)
					})
					stepLog = "Apply updated template to the dataservice deployment"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						if appConfigID == "" {
							appConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
							log.FailOnError(err, "Error while fetching AppConfigID")
						}
						scaledUpdatedDeployment, err = dsTest.UpdateDataServices(updatedDeployment.GetId(),
							appConfigID, updatedDeployment.GetImageId(),
							int32(ds.ScaleReplicas), scaledNewResourceTemplateID, params.InfraToTest.Namespace)
						log.FailOnError(err, "Error while updating dataservices")
						Step("Validate Deployments after template update", func() {
							err = dsTest.ValidateDataServiceDeployment(scaledUpdatedDeployment, namespace)
							log.FailOnError(err, "Error while validating dataservices")
							log.InfoD("Data-service: %v is up and healthy", ds.Name)
							scaledUpdatedDepList = append(scaledUpdatedDepList, scaledUpdatedDeployment)
							increasedPvcSizeAfterScaleUp, err = GetVolumeCapacityInGB(namespace, scaledUpdatedDeployment)
							log.InfoD("Increased Storage Size after scale-up is- %v", increasedPvcSizeAfterScaleUp)
						})
						stepLog = "Verify storage size before and after storage resize - Verify at STS, PV,PVC level"
						Step(stepLog, func() {
							stIncrease := validateStorageIncrease{
								UpdatedDeployment:    scaledUpdatedDeployment,
								StConfigUpdated:      scaledUpdatedstConfigModel,
								ResConfigUpdated:     scaledUpdatedResConfigModel,
								InitialCapacity:      increasedPvcSize,
								IncreasedStorageSize: increasedPvcSizeAfterScaleUp,
								BeforeResizePodAge:   beforeResizePodAge,
							}
							err := ValidateDepConfigPostStorageIncrease(ds, &stIncrease)
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
			increasedPvcSize      uint64
			beforeResizePodAge    float64
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
				beforeResizePodAge, err = GetPodAge(deployment, params.InfraToTest.Namespace)
				log.FailOnError(err, "unable to get pods AGE before Storage resize")
				log.InfoD("Pods Age before storage resize is- [%v]Min", beforeResizePodAge)
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
						beforeResizePodAge, err = GetPodAge(deployment, params.InfraToTest.Namespace)
						log.FailOnError(err, "unable to get pods AGE before Storage resize")
						log.InfoD("Pods Age before storage resize is- [%v]Min", beforeResizePodAge)
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
							increasedPvcSize, err = GetVolumeCapacityInGB(namespace, updatedDeployment)
							log.InfoD("Increased Storage Size is- %v", increasedPvcSize)
						})
						stepLog = "Verify storage size before and after storage resize - Verify at STS, PV,PVC level"
						Step(stepLog, func() {
							stIncrease := validateStorageIncrease{
								UpdatedDeployment:    updatedDeployment,
								StConfigUpdated:      stConfigModelUpdated,
								ResConfigUpdated:     resConfigModelUpdated,
								InitialCapacity:      initialCapacity,
								IncreasedStorageSize: increasedPvcSize,
								BeforeResizePodAge:   beforeResizePodAge,
							}
							err := ValidateDepConfigPostStorageIncrease(ds, &stIncrease)
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

	pdsdeploymentsmd5Hash := make(map[string]string)
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
		namespace,
		ds.DataServiceEnabledTLS)
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
	CleanMapEntries(pdsdeploymentsmd5Hash)
	ckSum, wlDep, err := dsTest.InsertDataAndReturnChecksum(deployment, wkloadParams)
	workloadDep = wlDep
	log.FailOnError(err, "Error while Running workloads")
	log.Debugf("Checksum for the deployment %s is %s", *deployment.ClusterResourceName, ckSum)
	pdsdeploymentsmd5Hash[*deployment.ClusterResourceName] = ckSum
	return deployment, initialCapacity, resConfigModel, stConfigModel, dataServiceAppConfigID, workloadDep, pdsdeploymentsmd5Hash, wkloadParams, nil
}
