package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	pdsbkp "github.com/portworx/torpedo/drivers/pds/pdsbackup"
	restoreBkp "github.com/portworx/torpedo/drivers/pds/pdsrestore"
	tc "github.com/portworx/torpedo/drivers/pds/targetcluster"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	"math/rand"
	"strconv"
)

var _ = Describe("{ServiceIdentityNsLevel}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("ServiceIdentityNsLevel", "Create and Update Service Identity with N namespaces with different roles ", pdsLabels, 0)
		credName := targetName + pdsbkp.RandString(8)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		awsBkpTargets = append(awsBkpTargets, bkpTarget)

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
			pdsRestoreNsName       string
		)

		Step("Deploy Data Services", func() {
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {
				log.InfoD("Cleaning up previous entries in the list")
				nsRoles = nil
				nsID2, nsID2, iamRolesToBeCleaned, siToBeCleaned = nil, nil, nil, nil
				deps, depList, deploymentsToBeCleaned = []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}
				resDeployments = make(map[PDSDataService]*pds.ModelsDeployment)

				_, supported := backupSupportedDataServiceNameIDMap[ds.Name]
				if !supported {
					log.InfoD("Data service: %v doesn't support backup, skipping...", ds.Name)
					continue
				}
				ns1, _, err := targetCluster.CreatePDSNamespace("ns1-" + strconv.Itoa(rand.Int()))
				log.FailOnError(err, "Error while creating namespace")
				log.InfoD("Successfully created namespace with PDS Label %v ", ns1)
				ns1Id1, err := targetCluster.GetnameSpaceID(ns1.Name, deploymentTargetID)
				nsID1 = append(nsID1, ns1Id1)
				log.FailOnError(err, "Error while fetching namespaceID")
				log.InfoD("NamespaceID1 fetched is %v ", nsID1)
				ns1RoleName := "namespace-admin"

				ns2, _, err := targetCluster.CreatePDSNamespace("ns2-" + strconv.Itoa(rand.Int()))
				log.FailOnError(err, "Error while creating namespace")
				log.InfoD("Successfully created namespace with PDS Label %v ", ns2)
				ns2Id2, err := targetCluster.GetnameSpaceID(ns2.Name, deploymentTargetID)
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

				components.ServiceIdentity.GenerateServiceTokenAndSetAuthContext(actorId)
				customParams.SetParamsForServiceIdentityTest(params, true)
				log.InfoD("Successfully updated Infra params for Si test")

				isDeploymentsDeleted = false
				deployment, _, dataServiceVersionBuildMap, err = DeployandValidateDataServicesWithSiAndTls(ds, ns1.Name, ns1Id1, projectID, resTempId, appConfigId, versionId, imageID, dsId, false)
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
					// ListBackupJobsBelongToDeployment will be changed after BUG: DS-6679 will be fixed
					customParams.SetParamsForServiceIdentityTest(params, false)
					backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
					log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
					pdsRestoreTargetClusterID, err := targetCluster.GetDeploymentTargetID(clusterID, tenantID)

					for _, backupJob := range backupJobs {
						log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						pdsRestoreNsName, _, err = restoreClient.GetNameSpaceNameToRestore(backupJob.GetId(), pdsRestoreTargetClusterID, ns2.Name, false)
						log.FailOnError(err, "unable to fetch namespace id to restore")
						components.ServiceIdentity.GenerateServiceTokenAndSetAuthContext(actorId)
						customParams.SetParamsForServiceIdentityTest(params, true)
						_, err = restoreClient.RestoreDataServiceWithRbac(pdsRestoreTargetClusterID, backupJob.GetId(), pdsRestoreNsName, dsEntity, ns2Id2, false)
						dash.VerifyFatal(err != nil, true, "Restore is failed as expected")

					}
				})

				Step("Update IAM Role with ns2 as namespace-admin role", func() {
					nsRoles = nil
					customParams.SetParamsForServiceIdentityTest(params, false)
					newns2RoleName := "namespace-admin"
					binding2.ResourceIds = nsID2
					binding2.RoleName = &newns2RoleName
					nsRoles := append(nsRoles, binding1, binding2)
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
					pdsRestoreTargetClusterID, err := targetCluster.GetDeploymentTargetID(clusterID, tenantID)

					for _, backupJob := range backupJobs {
						log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						log.FailOnError(err, "unable to fetch namespace id to restore")
						components.ServiceIdentity.GenerateServiceTokenAndSetAuthContext(actorId)
						customParams.SetParamsForServiceIdentityTest(params, true)
						restoredModel, _ := restoreClient.RestoreDataServiceWithRbac(pdsRestoreTargetClusterID, backupJob.GetId(), ns2.Name, dsEntity, ns2Id2, false)
						log.FailOnError(err, "Failed during restore.")

						restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
						customParams.SetParamsForServiceIdentityTest(params, false)
						dsTest.ValidateDataServiceDeployment(restoredDeployment, ns2.Name)
						resDeployments[ds] = restoredDeployment
						log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
						deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment)
						log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
					}
				})

				Step("Scale up the restored deployments on ns2", func() {
					log.InfoD("Starting to scale up the restore deployment")
					for rds, resDep := range resDeployments {
						customParams.SetParamsForServiceIdentityTest(params, false)
						log.InfoD("Scaling up DataService %s ", *resDep.Name)
						dataServiceDefaultAppConfigID, err = controlPlane.GetAppConfTemplate(tenantID, rds.Name)
						log.FailOnError(err, "Error while getting app configuration template")
						dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

						dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, rds.Name)
						log.FailOnError(err, "Error while getting resource setting template")
						dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

						components.ServiceIdentity.GenerateServiceTokenAndSetAuthContext(actorId)
						customParams.SetParamsForServiceIdentityTest(params, true)

						log.InfoD("Update deploymnets params are- resDepId- %v, dataServiceDefaultAppConfigID- %v ,resDep.GetImageId()- %v ,int32(3)- %v ,dataServiceDefaultResourceTemplateID- %v", *resDep.Id,
							dataServiceDefaultAppConfigID, resDep.GetImageId(),
							int32(ds.ScaleReplicas), dataServiceDefaultResourceTemplateID)
						updatedDeployment, err := dsTest.UpdateDataServices(*resDep.Id,
							dataServiceDefaultAppConfigID, resDep.GetImageId(),
							int32(ds.ScaleReplicas), dataServiceDefaultResourceTemplateID, ns2.Name)
						log.FailOnError(err, "Error while updating dataservices")

						err = dsTest.ValidateDataServiceDeployment(updatedDeployment, ns2.Name)
						log.FailOnError(err, "Error while validating data service deployment")

						customParams.SetParamsForServiceIdentityTest(params, false)
						_, _, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, *resDep.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, ns2.Name)
						log.FailOnError(err, "error on ValidateDataServiceVolumes method")
						dash.VerifyFatal(int32(ds.ScaleReplicas), config.Spec.Nodes, "Validating replicas after scaling up of dataservice")
					}
				})
				//ToDo : Add workload generation for restored-deps with RBAC roles on ns2

				Step("Delete Deployments", func() {
					CleanupDeployments(deploymentsToBeCleaned)
					CleanupServiceIdentitiesAndIamRoles(siToBeCleaned, iamRolesToBeCleaned, actorId)
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

var _ = Describe("{ServiceIdentityTargetClusterLevel}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("ServiceIdentityTargetClusterLevel", "Create and Update Service Identity with 2 namespaces on different clusters and perform cross-cluster restore", pdsLabels, 0)
		credName := targetName + pdsbkp.RandString(8)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target1.")
		log.InfoD("AWS S3 target1 - %v created successfully", bkpTarget.GetName())
		awsBkpTargets = append(awsBkpTargets, bkpTarget)

	})

	It("Deploy Dataservices", func() {
		var (
			deploymentsToBeCleaned         []*pds.ModelsDeployment
			restoredDeploymentsToBeCleaned []*pds.ModelsDeployment
			deployments                    = make(map[PDSDataService]*pds.ModelsDeployment)
			resDeployments                 = make(map[PDSDataService]*pds.ModelsDeployment)
			depList                        []*pds.ModelsDeployment
			deps                           []*pds.ModelsDeployment
			dsVersions                     = make(map[string]map[string][]string)
			nsRolesSrc                     []pds.ModelsBinding
			nsRolesDesti                   []pds.ModelsBinding
			iamRolesToBeCleanedinSrc       []string
			siToBeCleanedinSrc             []string
			iamRolesToBeCleanedinDest      []string
			siToBeCleanedinDest            []string
			binding1                       pds.ModelsBinding
			binding2                       pds.ModelsBinding
			nsID1                          []string
			nsID2                          []string
			serviceIdentityIDSrc           string
			serviceIdentityIDDesti         string
			destinationTargetID            string
			pdsRestoreNsName               string
		)

		Step("Deploy Data Services", func() {
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			log.FailOnError(err, "Failed to fetch destination Target cluster ID")
			for _, ds := range params.DataServiceToTest {
				log.InfoD("Cleaning up previous entries to the lists")
				nsRolesSrc, nsRolesDesti = nil, nil
				nsID2, nsID2, iamRolesToBeCleanedinSrc, iamRolesToBeCleanedinDest, siToBeCleanedinDest, siToBeCleanedinSrc = nil, nil, nil, nil, nil, nil
				deps, depList, deploymentsToBeCleaned, restoredDeploymentsToBeCleaned = []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}
				resDeployments = make(map[PDSDataService]*pds.ModelsDeployment)
				deployments = make(map[PDSDataService]*pds.ModelsDeployment)

				_, supported := backupSupportedDataServiceNameIDMap[ds.Name]
				if !supported {
					log.InfoD("Data service: %v doesn't support backup, skipping...", ds.Name)
					continue
				}
				log.InfoD("Create namespace ns1 and assign admin role to it on targetCluster 1")

				log.FailOnError(err, "failed while setting set cluster path")

				srcNS, _, err := targetCluster.CreatePDSNamespace("ns1-" + strconv.Itoa(rand.Int()))
				namespaceName := srcNS.Name
				log.FailOnError(err, "Error while creating namespace")
				log.InfoD("Successfully created source namespace with PDS Label %v ", srcNS)
				ns1Id1, err := targetCluster.GetnameSpaceID(namespaceName, deploymentTargetID)
				nsID1 = append(nsID1, ns1Id1)
				log.FailOnError(err, "Error while fetching namespaceID")
				log.InfoD("srcNs fetched is %v ", nsID1)
				ns1RoleName := "namespace-admin"

				binding1.ResourceIds = nsID1
				binding1.RoleName = &ns1RoleName

				nsRolesSrc = append(nsRolesSrc, binding1)

				actorId1, iamId1, err := pdslib.CreateSiAndIamRoleBindings(accountID, nsRolesSrc)
				log.FailOnError(err, "Error while creating and fetching IAM Roles")
				log.InfoD("Successfully created ServiceIdentity- %v and IAM Roles- %v ", actorId1, iamId1)
				serviceIdentityIDSrc = actorId1

				siToBeCleanedinSrc = append(siToBeCleanedinSrc, actorId1)
				iamRolesToBeCleanedinSrc = append(iamRolesToBeCleanedinSrc, iamId1)

				log.InfoD("Create namespace ns2 and assign admin role to it on targetCluster 2")
				log.InfoD("setting source kubeconfig")
				err = SetDestinationKubeConfig()
				log.FailOnError(err, "failed while setting set cluster path")
				destinationConfigPath, err := GetDestinationClusterConfigPath()
				destinationClusterID, _ := targetCluster.GetClusterIDFromKubePath(destinationConfigPath)
				destinationTargetID, err = targetCluster.GetDeploymentTargetID(destinationClusterID, tenantID)
				log.InfoD("Destination cluster Target ID is- %v", destinationTargetID)
				nsDesti, _, err := targetCluster.CreatePDSNamespace(namespaceName)
				log.FailOnError(err, "Error while creating namespace")
				log.InfoD("Successfully created namespace with PDS Label %v ", nsDesti)
				ns2Id2, err := targetCluster.GetnameSpaceID(namespaceName, destinationTargetID)
				nsID2 = append(nsID2, ns2Id2)
				log.FailOnError(err, "Error while fetching namespaceID")
				log.InfoD("nsDesti fetched is %v ", nsID2)
				ns2RoleName := "namespace-admin"

				binding2.ResourceIds = nsID2
				binding2.RoleName = &ns2RoleName

				nsRolesDesti = append(nsRolesDesti, binding2)

				actorId2, iamId2, err := pdslib.CreateSiAndIamRoleBindings(accountID, nsRolesDesti)
				log.FailOnError(err, "Error while creating and fetching IAM Roles")
				log.InfoD("Successfully created ServiceIdentity- %v and IAM Roles- %v ", actorId2, iamId2)
				serviceIdentityIDDesti = actorId2

				siToBeCleanedinDest = append(siToBeCleanedinDest, actorId2)
				iamRolesToBeCleanedinDest = append(iamRolesToBeCleanedinDest, iamId2)

				log.InfoD("setting source kubeconfig")
				err = SetSourceKubeConfig()
				log.FailOnError(err, "failed while setting set cluster path")
				resTempId, appConfigId, err := dsWithRbac.GetDataServiceDeploymentTemplateIDS(tenantID, ds)
				log.FailOnError(err, "Error while fetching template and app-config ids")

				log.FailOnError(err, "Error while fetching template and app-config ids")
				dsId, err := dsTest.GetDataServiceID(ds.Name)
				versionId, imageID, err := dsWithRbac.GetDSImageVersionToBeDeployed(false, ds, dsId)

				components.ServiceIdentity.GenerateServiceTokenAndSetAuthContext(actorId1)
				customParams.SetParamsForServiceIdentityTest(params, true)
				log.InfoD("Successfully updated Infra params for Si test")

				isDeploymentsDeleted = false
				deployment, _, dataServiceVersionBuildMap, err = DeployandValidateDataServicesWithSiAndTls(ds, namespaceName, ns1Id1, projectID, resTempId, appConfigId, versionId, imageID, dsId, false)
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

				Step("Perform cross restore with S1 on ns2 of cluster2", func() {
					log.FailOnError(err, "failed while getting dest cluster path")
					restoreTarget := tc.NewTargetCluster(destinationConfigPath)
					restoreClient := restoreBkp.RestoreClient{
						TenantId:             tenantID,
						ProjectId:            projectID,
						Components:           components,
						Deployment:           deployment,
						RestoreTargetCluster: restoreTarget,
					}
					// ListBackupJobsBelongToDeployment will be changed after BUG: DS-6679 will be fixed
					customParams.SetParamsForServiceIdentityTest(params, false)
					backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
					log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
					err = SetDestinationKubeConfig()
					for _, backupJob := range backupJobs {
						log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						pdsRestoreNsName, _, err = restoreClient.GetNameSpaceNameToRestore(backupJob.GetId(), destinationTargetID, namespaceName, true)
						log.FailOnError(err, "unable to fetch namespace id to restore")
						components.ServiceIdentity.GenerateServiceTokenAndSetAuthContext(actorId1)
						customParams.SetParamsForServiceIdentityTest(params, true)
						_, err = restoreClient.RestoreDataServiceWithRbac(destinationTargetID, backupJob.GetId(), pdsRestoreNsName, dsEntity, ns2Id2, true)
						dash.VerifyFatal(err != nil, true, "Restore is failed as expected")

					}
				})

				Step("Update IAM2 with ns1 of cluster1 as reader role", func() {
					nsRolesDesti = nil
					var newBinding pds.ModelsBinding
					customParams.SetParamsForServiceIdentityTest(params, false)
					newns1RoleName := "namespace-reader"
					newBinding.ResourceIds = nsID1
					newBinding.RoleName = &newns1RoleName
					nsRolesDesti = append(nsRolesDesti, newBinding, binding2)
					log.InfoD("Starting to update the IAM Roles for ns2")
					resp, err := components.IamRoleBindings.UpdateIamRoleBindings(accountID, serviceIdentityIDDesti, nsRolesDesti)
					log.InfoD("Updated IAM role Binding is- %v", resp.AccessPolicy)
					log.FailOnError(err, "Failed while updating IAM Roles for ns2")
				})

				Step("Restore ds on ns2 of cluster2 to ns2 using updated ServiceIdentity2 (with ns1 as reader) token", func() {
					log.FailOnError(err, "failed while getting dest cluster path")
					restoreTarget := tc.NewTargetCluster(destinationConfigPath)
					restoreClient := restoreBkp.RestoreClient{
						TenantId:             tenantID,
						ProjectId:            projectID,
						Components:           components,
						Deployment:           deployment,
						RestoreTargetCluster: restoreTarget,
					}
					// ListBackupJobsBelongToDeployment will be changed after BUG: DS-6679 will be fixed

					backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
					log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
					err = SetDestinationKubeConfig()
					for _, backupJob := range backupJobs {
						log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						pdsRestoreNsName, _, err = restoreClient.GetNameSpaceNameToRestore(backupJob.GetId(), destinationTargetID, namespaceName, true)
						log.FailOnError(err, "unable to fetch namespace id to restore")
						components.ServiceIdentity.GenerateServiceTokenAndSetAuthContext(actorId2)
						customParams.SetParamsForServiceIdentityTest(params, true)
						_, err = restoreClient.RestoreDataServiceWithRbac(destinationTargetID, backupJob.GetId(), pdsRestoreNsName, dsEntity, ns2Id2, false)
						dash.VerifyFatal(err != nil, true, "Restore is failed as expected")

					}
				})
				Step("Update IAM2 with ns1 of cluster1 as admin role", func() {
					nsRolesDesti = nil
					var newBinding2 pds.ModelsBinding
					customParams.SetParamsForServiceIdentityTest(params, false)
					newns1RoleName := "namespace-admin"
					newBinding2.ResourceIds = nsID1
					newBinding2.RoleName = &newns1RoleName
					nsRolesDesti = append(nsRolesDesti, newBinding2, binding2)
					log.InfoD("Starting to update the IAM Roles for ns2")
					_, err := components.IamRoleBindings.UpdateIamRoleBindings(accountID, serviceIdentityIDDesti, nsRolesDesti)
					log.FailOnError(err, "Failed while updating IAM Roles for ns2")
				})
				Step("Restore ds on ns2 of cluster2 to ns2 using updated ServiceIdentity2 (with ns1 as admin) token", func() {
					log.FailOnError(err, "failed while getting dest cluster path")
					restoreTarget := tc.NewTargetCluster(destinationConfigPath)
					restoreClient := restoreBkp.RestoreClient{
						TenantId:             tenantID,
						ProjectId:            projectID,
						Components:           components,
						Deployment:           deployment,
						RestoreTargetCluster: restoreTarget,
					}
					// ListBackupJobsBelongToDeployment will be changed after BUG: DS-6679 will be fixed

					backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
					log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
					for _, backupJob := range backupJobs {
						log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						pdsRestoreNsName, _, err = restoreClient.GetNameSpaceNameToRestore(backupJob.GetId(), destinationTargetID, namespaceName, true)
						log.FailOnError(err, "unable to fetch namespace id to restore")
						components.ServiceIdentity.GenerateServiceTokenAndSetAuthContext(actorId2)
						customParams.SetParamsForServiceIdentityTest(params, true)
						restoredModel, _ := restoreClient.RestoreDataServiceWithRbac(destinationTargetID, backupJob.GetId(), pdsRestoreNsName, dsEntity, ns2Id2, false)
						log.FailOnError(err, "Failed during restore.")
						restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
						customParams.SetParamsForServiceIdentityTest(params, false)
						dsTest.ValidateDataServiceDeployment(restoredDeployment, pdsRestoreNsName)
						resDeployments[ds] = restoredDeployment
						log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
						restoredDeploymentsToBeCleaned = append(restoredDeploymentsToBeCleaned, restoredDeployment)
						log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
					}
				})

				//ToDo : Add workload generation for restored-deps with RBAC roles on ns2 of cluster2
				Step("Perform adhoc backup of restored deployment and validate them", func() {
					customParams.SetParamsForServiceIdentityTest(params, false)
					log.Infof("Deployment ID: %v, backup target ID: %v", restoredDeployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(restoredDeployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup")
				})

				Step("Update IAM1 with ns1 of cluster2 as admin role", func() {
					err = SetSourceKubeConfig()
					nsRolesSrc = nil
					var newBinding1 pds.ModelsBinding
					newns1RoleName := "namespace-admin"

					newBinding1.ResourceIds = nsID2
					newBinding1.RoleName = &newns1RoleName
					nsRolesSrc = append(nsRolesSrc, newBinding1, binding1)
					log.InfoD("Starting to update the IAM Roles for ns2")
					_, err := components.IamRoleBindings.UpdateIamRoleBindings(accountID, serviceIdentityIDSrc, nsRolesSrc)
					log.FailOnError(err, "Failed while updating IAM Roles for ns2")
				})

				Step("Restore ds on ns2 of cluster2 to ns2 using updated ServiceIdentity1 (with ns2 as admin) token", func() {
					log.FailOnError(err, "failed while getting dest cluster path")
					restoreTarget := tc.NewTargetCluster(destinationConfigPath)
					restoreClient := restoreBkp.RestoreClient{
						TenantId:             tenantID,
						ProjectId:            projectID,
						Components:           components,
						Deployment:           deployment,
						RestoreTargetCluster: restoreTarget,
					}
					// ListBackupJobsBelongToDeployment will be changed after BUG: DS-6679 will be fixed

					backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
					log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
					err = SetDestinationKubeConfig()
					for _, backupJob := range backupJobs {
						log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						pdsRestoreNsName, _, err = restoreClient.GetNameSpaceNameToRestore(backupJob.GetId(), destinationTargetID, namespaceName, true)
						log.FailOnError(err, "unable to fetch namespace id to restore")
						components.ServiceIdentity.GenerateServiceTokenAndSetAuthContext(actorId2)
						customParams.SetParamsForServiceIdentityTest(params, true)
						restoredModel, _ := restoreClient.RestoreDataServiceWithRbac(destinationTargetID, backupJob.GetId(), pdsRestoreNsName, dsEntity, ns2Id2, false)
						log.FailOnError(err, "Failed during restore.")
						restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
						customParams.SetParamsForServiceIdentityTest(params, false)
						dsTest.ValidateDataServiceDeployment(restoredDeployment, pdsRestoreNsName)
						resDeployments[ds] = restoredDeployment
						log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
						restoredDeploymentsToBeCleaned = append(restoredDeploymentsToBeCleaned, restoredDeployment)
						log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
					}
				})

				Step("Delete Deployments", func() {
					CleanupDeployments(restoredDeploymentsToBeCleaned)
					CleanupServiceIdentitiesAndIamRoles(siToBeCleanedinDest, iamRolesToBeCleanedinDest, actorId2)
					err = SetSourceKubeConfig()
					CleanupDeployments(deploymentsToBeCleaned)
					CleanupServiceIdentitiesAndIamRoles(siToBeCleanedinSrc, iamRolesToBeCleanedinSrc, actorId1)
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

var _ = Describe("{ServiceIdentitySiDLevel}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("ServiceIdentitySiDLevel", "Create and Update N Service Identities with N namespaces with different roles ", pdsLabels, 0)
		credName := targetName + pdsbkp.RandString(8)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bkpTarget, err = bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", credName), deploymentTargetID)
		log.FailOnError(err, "Failed to create S3 backup target.")
		log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
		awsBkpTargets = append(awsBkpTargets, bkpTarget)

	})

	It("Deploy Dataservices", func() {
		var (
			deploymentsToBeCleaned []*pds.ModelsDeployment
			deployments            = make(map[PDSDataService]*pds.ModelsDeployment)
			resDeployments         = make(map[PDSDataService]*pds.ModelsDeployment)
			depList                []*pds.ModelsDeployment
			deps                   []*pds.ModelsDeployment
			dsVersions             = make(map[string]map[string][]string)
			nsAdminRoles           []pds.ModelsBinding
			nsReaderRoles          []pds.ModelsBinding
			iamRolesToBeCleaned    []string
			siToBeCleaned          []string
			bindingReader          pds.ModelsBinding
			bindingAdmin           pds.ModelsBinding
			nsID1                  []string
			nsID2                  []string
			resDepNamespaceID      []string
			pdsRestoreNsName       string
			pdsRestoreNsId         string
			serviceIdentityID2     string
		)

		Step("Deploy Data Services", func() {
			backupSupportedDataServiceNameIDMap, err = bkpClient.GetAllBackupSupportedDataServices()
			log.FailOnError(err, "Error while fetching the backup supported ds.")
			for _, ds := range params.DataServiceToTest {

				log.InfoD("Cleaning up previous entries to the lists")
				nsAdminRoles, nsReaderRoles = nil, nil
				nsID2, nsID2, iamRolesToBeCleaned, siToBeCleaned, resDepNamespaceID = nil, nil, nil, nil, nil
				deps, depList, deploymentsToBeCleaned = []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}, []*pds.ModelsDeployment{}
				resDeployments = make(map[PDSDataService]*pds.ModelsDeployment)
				deployments = make(map[PDSDataService]*pds.ModelsDeployment)

				_, supported := backupSupportedDataServiceNameIDMap[ds.Name]
				if !supported {
					log.InfoD("Data service: %v doesn't support backup, skipping...", ds.Name)
					continue
				}
				ns1, _, err := targetCluster.CreatePDSNamespace("ns1-" + strconv.Itoa(rand.Int()))
				log.FailOnError(err, "Error while creating namespace")
				log.InfoD("Successfully created namespace with PDS Label %v ", ns1)
				ns1Id1, err := targetCluster.GetnameSpaceID(ns1.Name, deploymentTargetID)
				nsID1 = append(nsID1, ns1Id1)
				log.FailOnError(err, "Error while fetching namespaceID")
				log.InfoD("NamespaceID1 fetched is %v ", nsID1)
				nsAdminRole := "namespace-admin"
				nsReaderRole := "namespace-reader"

				bindingAdmin.ResourceIds = nsID1
				bindingAdmin.RoleName = &nsAdminRole

				bindingReader.ResourceIds = nsID2
				bindingReader.RoleName = &nsReaderRole

				nsAdminRoles = append(nsAdminRoles, bindingAdmin)
				nsReaderRoles = append(nsReaderRoles, bindingReader)

				actorId1, iamId1, err := pdslib.CreateSiAndIamRoleBindings(accountID, nsAdminRoles)
				log.FailOnError(err, "Error while creating and fetching IAM Roles")
				log.InfoD("Successfully created ServiceIdentity- %v and IAM Roles- %v ", actorId1, iamId1)

				actorId2, iamId2, err := pdslib.CreateSiAndIamRoleBindings(accountID, nsReaderRoles)
				log.FailOnError(err, "Error while creating and fetching IAM Roles")
				log.InfoD("Successfully created ServiceIdentity- %v and IAM Roles- %v ", actorId2, iamId2)
				serviceIdentityID2 = actorId2
				siToBeCleaned = append(siToBeCleaned, serviceIdentityID2)
				iamRolesToBeCleaned = append(iamRolesToBeCleaned, iamId2)

				resTempId, appConfigId, err := dsWithRbac.GetDataServiceDeploymentTemplateIDS(tenantID, ds)
				log.FailOnError(err, "Error while fetching template and app-config ids")
				dsId, err := dsTest.GetDataServiceID(ds.Name)
				versionId, imageID, err := dsWithRbac.GetDSImageVersionToBeDeployed(false, ds, dsId)

				components.ServiceIdentity.GenerateServiceTokenAndSetAuthContext(actorId1)
				customParams.SetParamsForServiceIdentityTest(params, true)
				log.InfoD("Successfully updated Infra params for Si test")

				isDeploymentsDeleted = false
				deployment, _, dataServiceVersionBuildMap, err = DeployandValidateDataServicesWithSiAndTls(ds, ns1.Name, ns1Id1, projectID, resTempId, appConfigId, versionId, imageID, dsId, false)
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

				Step("Delete ServiceIdentity 1", func() {
					customParams.SetParamsForServiceIdentityTest(params, false)
					_, err := components.ServiceIdentity.DeleteServiceIdentity(actorId1)
					log.FailOnError(err, "Failed to delete service identity 1")
				})
				Step("Scale up the deployments with general token", func() {
					log.InfoD("Starting to scale up the restore deployment")
					for ds, deployment := range deployments {
						log.InfoD("Scaling up DataService %v ", &deployment.Name)
						dataServiceDefaultAppConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
						log.FailOnError(err, "Error while getting app configuration template")
						dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

						dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
						log.FailOnError(err, "Error while getting resource setting template")
						dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

						updatedDeployment, err := dsTest.UpdateDataServices(deployment.GetId(),
							dataServiceDefaultAppConfigID, deployment.GetImageId(),
							int32(ds.ScaleReplicas), dataServiceDefaultResourceTemplateID, ns1.Name)
						log.FailOnError(err, "Error while updating dataservices")

						err = dsTest.ValidateDataServiceDeployment(updatedDeployment, ns1.Name)
						log.FailOnError(err, "Error while validating data service deployment")

						_, _, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, *deployment.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, ns1.Name)
						log.FailOnError(err, "error on ValidateDataServiceVolumes method")
						dash.VerifyFatal(int32(ds.ScaleReplicas), config.Spec.Nodes, "Validating replicas after scaling up of dataservice")
					}

				})

				Step("Perform adhoc backup and validate them", func() {

					log.Infof("Deployment ID: %v, backup target ID: %v", deployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup")
				})

				Step("Perform restore for the backup jobs to diff namespace with general token", func() {
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
					// ListBackupJobsBelongToDeployment will be changed after BUG: DS-6679 will be fixed

					backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
					log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
					for _, backupJob := range backupJobs {
						log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						pdsRestoreNsName, pdsRestoreNsId, err = restoreClient.GetNameSpaceNameToRestore(backupJob.GetId(), deploymentTargetID, ns1.Name, false)
						log.FailOnError(err, "unable to fetch namespace id to restore")
						resDepNamespaceID = append(resDepNamespaceID, pdsRestoreNsId)
						components.ServiceIdentity.GenerateServiceTokenAndSetAuthContext(actorId2)
						restoredModel, _ := restoreClient.RestoreDataServiceWithRbac(deploymentTargetID, backupJob.GetId(), pdsRestoreNsName, dsEntity, pdsRestoreNsId, false)
						log.FailOnError(err, "Failed during restore.")
						restoredDeployment, err = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
						customParams.SetParamsForServiceIdentityTest(params, false)
						dsTest.ValidateDataServiceDeployment(restoredDeployment, pdsRestoreNsName)
						resDeployments[ds] = restoredDeployment
						log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
						deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment)
						log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
					}
				})

				Step("Update IAM2 with newly created restore namespace", func() {
					nsReaderRoles = nil
					var (
						newBinding1 pds.ModelsBinding
						newBinding2 pds.ModelsBinding
					)
					newRoleName := "namespace-admin"
					newBinding1.ResourceIds = nsID2
					newBinding1.RoleName = &newRoleName
					newBinding2.ResourceIds = resDepNamespaceID
					newBinding2.RoleName = &newRoleName

					nsReaderRoles := append(nsReaderRoles, newBinding1, newBinding2)
					log.InfoD("Starting to update the IAM Roles for ns2")
					_, err := components.IamRoleBindings.UpdateIamRoleBindings(accountID, serviceIdentityID2, nsReaderRoles)
					log.FailOnError(err, "Failed while updating IAM Roles for ns2")
				})

				Step("Scale up the restored deployments with updated IAM2 Sitoken", func() {
					log.InfoD("Starting to scale up the restore deployment")
					for rds, resDep := range resDeployments {
						log.InfoD("Scaling up DataService %v ", rds.Name)
						dataServiceDefaultAppConfigID, err = controlPlane.GetAppConfTemplate(tenantID, rds.Name)
						log.FailOnError(err, "Error while getting app configuration template")
						dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

						dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, rds.Name)
						log.FailOnError(err, "Error while getting resource setting template")
						dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

						components.ServiceIdentity.GenerateServiceTokenAndSetAuthContext(actorId2)
						customParams.SetParamsForServiceIdentityTest(params, true)

						updatedDeployment, err := dsTest.UpdateDataServices(resDep.GetId(),
							dataServiceDefaultAppConfigID, deployment.GetImageId(),
							int32(ds.ScaleReplicas), dataServiceDefaultResourceTemplateID, pdsRestoreNsName)
						log.FailOnError(err, "Error while updating dataservices")
						customParams.SetParamsForServiceIdentityTest(params, false)
						log.InfoD("PDS RESTORE NAMESPACE IS- %v", pdsRestoreNsName)
						err = dsTest.ValidateDataServiceDeployment(updatedDeployment, pdsRestoreNsName)
						log.FailOnError(err, "Error while validating data service deployment")
						_, _, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, *resDep.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, pdsRestoreNsName)
						log.FailOnError(err, "error on ValidateDataServiceVolumes method")
						dash.VerifyFatal(int32(ds.ScaleReplicas), config.Spec.Nodes, "Validating replicas after scaling up of dataservice")
					}

				})

				//ToDo: Run workloads on restored dep

				Step("Perform adhoc backup of the restored Deployment and validate them", func() {

					customParams.SetParamsForServiceIdentityTest(params, true)
					log.Infof("Deployment ID: %v, backup target ID: %v", restoredDeployment.GetId(), bkpTarget.GetId())
					err = bkpClient.TriggerAndValidateAdhocBackup(restoredDeployment.GetId(), bkpTarget.GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup")
				})

				Step("Perform restore again for the backup jobs to same ns with new admin role token", func() {
					customParams.SetParamsForServiceIdentityTest(params, false)
					ctx, err := GetSourceClusterConfigPath()
					log.FailOnError(err, "failed while getting src cluster path")
					restoreTarget := tc.NewTargetCluster(ctx)
					restoreClient := restoreBkp.RestoreClient{
						TenantId:             tenantID,
						ProjectId:            projectID,
						Components:           components,
						Deployment:           restoredDeployment,
						RestoreTargetCluster: restoreTarget,
					}
					backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, restoredDeployment.GetId())
					log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", restoredDeployment.GetClusterResourceName())
					for _, backupJob := range backupJobs {
						log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
						log.FailOnError(err, "unable to fetch namespace id to restore")
						customParams.SetParamsForServiceIdentityTest(params, true)
						restoredModel, _ := restoreClient.RestoreDataServiceWithRbac(deploymentTargetID, backupJob.GetId(), pdsRestoreNsName, dsEntity, resDepNamespaceID[0], false)
						log.FailOnError(err, "Failed during restore.")
						restoredDeployment2, err := restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
						customParams.SetParamsForServiceIdentityTest(params, false)
						dsTest.ValidateDataServiceDeployment(restoredDeployment2, pdsRestoreNsName)
						resDeployments[ds] = restoredDeployment2
						log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
						deploymentsToBeCleaned = append(deploymentsToBeCleaned, restoredDeployment2)
						log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
					}
				})
				Step("Delete Deployments", func() {
					CleanupDeployments(deploymentsToBeCleaned)
					CleanupServiceIdentitiesAndIamRoles(siToBeCleaned, iamRolesToBeCleaned, actorId2)
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
