package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	pdsbkp "github.com/portworx/torpedo/drivers/pds/pdsbackup"
	restoreBkp "github.com/portworx/torpedo/drivers/pds/pdsrestore"
	tc "github.com/portworx/torpedo/drivers/pds/targetcluster"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{ServiceIdentityNsLevel}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("ServiceIdentityForNsLevel", "Create and Update Service Identity with N namespaces with different roles ", pdsLabels, 0)
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

	It("Deploy Dataservices", func() {
		var (
			deploymentsToBeCleaned []*pds.ModelsDeployment
			deployments            = make(map[PDSDataService]*pds.ModelsDeployment)
			resDeployments         = make(map[PDSDataService]*pds.ModelsDeployment)
			depList                []*pds.ModelsDeployment
			deps                   []*pds.ModelsDeployment
			dsVersions             = make(map[string]map[string][]string)
			nsRoles                []pds.ModelsBinding
			iamRolesToBeCleaned    []string
			siToBeCleaned          []string
			binding1               pds.ModelsBinding
			binding2               pds.ModelsBinding
			nsID1                  []string
			nsID2                  []string
			serviceIdentityID      string
		)

		Step("Deploy Data Services", func() {
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {
				_, supported := backupSupportedDataServiceNameIDMap[ds.Name]
				if !supported {
					log.InfoD("Data service: %v doesn't support backup, skipping...", ds.Name)
					continue
				}
				ns1, err := pdslib.CreatePdsLabeledNamespaces()
				log.FailOnError(err, "Error while creating namespace")
				log.InfoD("Successfully created namespace with PDS Label %v ", ns1)
				ns1Id1, err := targetCluster.GetnameSpaceID(ns1, deploymentTargetID)
				nsID1 = append(nsID1, ns1Id1)
				log.FailOnError(err, "Error while fetching namespaceID")
				log.InfoD("NamespaceID1 fetched is %v ", nsID1)
				ns1RoleName := "namespace-admin"

				ns2, err := pdslib.CreatePdsLabeledNamespaces()
				log.FailOnError(err, "Error while creating namespace")
				log.InfoD("Successfully created namespace with PDS Label %v ", ns2)
				ns2Id2, err := targetCluster.GetnameSpaceID(ns2, deploymentTargetID)
				nsID2 = append(nsID2, ns2Id2)
				log.FailOnError(err, "Error while fetching namespaceID")
				log.InfoD("NamespaceID2 fetched is %v ", nsID2)
				ns2RoleName := "namespace-reader"

				binding1.ResourceIds = nsID1
				binding1.RoleName = &ns1RoleName

				binding2.ResourceIds = nsID2
				binding2.RoleName = &ns2RoleName

				nsRoles = append(nsRoles, binding1, binding2)

				resTempId, appConfigId, err := dsWithRbac.GetDataServiceDeploymentTemplateIDS(tenantID, ds)
				log.FailOnError(err, "Error while fetching template and app-config ids")
				actorId, iamId, err := pdslib.CreateSiAndIamRoleBindings(accountID, nsRoles)
				log.FailOnError(err, "Error while creating and fetching IAM Roles")
				log.InfoD("Successfully created ServiceIdentity- %v and IAM Roles- %v ", actorId, iamId)
				serviceIdentityID = actorId

				log.FailOnError(err, "Error while fetching template and app-config ids")
				dsId, err := dsTest.GetDataServiceID(ds.Name)
				versionId, imageID, err := dsWithRbac.GetDSImageVersionToBeDeployed(false, ds, dsId)

				customParams.SetParamsForServiceIdentityTest(params, true)
				log.InfoD("Successfully updated Infra params for Si test")

				isDeploymentsDeleted = false
				deployment, _, dataServiceVersionBuildMap, err = DeployandValidateDataServicesWithSiAndTls(ds, ns1, ns1Id1, projectID, resTempId, appConfigId, versionId, imageID, dsId)
				log.FailOnError(err, "Error while deploying data services")
				deploymentsToBeCleaned = append(deploymentsToBeCleaned, deployment)
				log.FailOnError(err, "Error while deploying data services")
				deployments[ds] = deployment
				deps = append(deps, deployment)
				depList = append(depList, deployment)

				dsEntity = restoreBkp.DSEntity{
					Deployment: deployment,
				}
				dsVersions[ds.Name] = dataServiceVersionBuildMap

				//ToDo : Add workload generation for deps with RBAC roles on ns1

				Step("Perform adhoc backup and validate them", func() {

					log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup")
				})

				Step("Perform restore for the backup jobs to diff namespace", func() {

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
						restoredModel, _ := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), ns2, dsEntity, false, false)
						log.InfoD("Failed to trigger restore for - %v", restoredModel.Name)
						log.InfoD("Restore is failed as expected.")
						deploymentsToBeCleaned = append(deploymentsToBeCleaned)
					}
				})
				//ToDo: Testing is blocked from here because of bug-
				Step("Update IAM Role with ns2 as namespace-admin role", func() {
					ns2RoleName = "namespace-admin"
					binding2.RoleName = &ns2RoleName
					nsRoles = append(nsRoles, binding1, binding2)
					log.InfoD("Starting to update the IAM Roles for ns2")
					_, err := components.IamRoleBindings.UpdateIamRoleBindings(accountID, serviceIdentityID, nsRoles)
					log.FailOnError(err, "Failed while updating IAM Roles for ns2")

				})

				Step("Perform restore again for the backup jobs to ns2 with admin role", func() {
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
						restoredModel, _ := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), ns2, dsEntity, false, true)
						log.FailOnError(err, "Failed during restore.")
						restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
						resDeployments[ds] = restoredDeployment
						log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
						deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment)
						log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
					}
				})

				Step("Scale up the restored deployments on ns2", func() {
					log.InfoD("Starting to scale up the restore deployment")
					for ds, resDep := range resDeployments {
						log.InfoD("Scaling up DataService %v ", ds.Name)

						dataServiceDefaultAppConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
						log.FailOnError(err, "Error while getting app configuration template")
						dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

						dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
						log.FailOnError(err, "Error while getting resource setting template")
						dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

						updatedDeployment, err := pdslib.UpdateDataServices(deployment.GetId(),
							dataServiceDefaultAppConfigID, deployment.GetImageId(),
							int32(ds.ScaleReplicas), dataServiceDefaultResourceTemplateID, ns2)
						log.FailOnError(err, "Error while updating dataservices")

						err = dsTest.ValidateDataServiceDeployment(updatedDeployment, ns2)
						log.FailOnError(err, "Error while validating data service deployment")

						_, _, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, *resDep.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, ns2)
						log.FailOnError(err, "error on ValidateDataServiceVolumes method")
						dash.VerifyFatal(int32(ds.ScaleReplicas), config.Spec.Nodes, "Validating replicas after scaling up of dataservice")
					}
				})
				//ToDo : Add workload generation for restored-deps with RBAC roles on ns2

				Step("Delete Deployments", func() {
					CleanupDeployments(deploymentsToBeCleaned)
					CleanupServiceIdentitiesAndIamRoles(siToBeCleaned, iamRolesToBeCleaned, actorId)
					customParams.SetParamsForServiceIdentityTest(params, false)
				})

			}

		})
		JustAfterEach(func() {
			defer EndTorpedoTest()
		})
	})
})
