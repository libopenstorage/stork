package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	pdsbkp "github.com/portworx/torpedo/drivers/pds/pdsbackup"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	"net/http"
	"strings"
	"time"
)

var (
	bkpClient                                     *pdsbkp.BackupClient
	awsBkpTargets, azureBkpTargets, gcpBkpTargets []*pds.ModelsBackupTarget
	bkpTargetName                                 = "automation--"
)

var _ = Describe("{ValidateBackupTargetsOnSupportedObjectStores}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("ValidateBackupTargetsOnSupportedObjectStores", "Validate backup targets for all supported object stores.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bucketName = strings.ToLower("pds-automation-" + pdsbkp.RandString(5))
	})

	It("Add all supported Object stores as backup target for data services", func() {
		stepLog := "Create AWS S3 Backup target."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			bkpTarget, err := bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", bkpTargetName), bucketName, deploymentTargetID)
			log.FailOnError(err, "Failed to create AWS backup target.")
			log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
			awsBkpTargets = append(awsBkpTargets, bkpTarget)
		})
		stepLog = "Create GCP Backup target."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			bkpTarget, err := bkpClient.CreateGcpBackupCredsAndTarget(tenantID, fmt.Sprintf("%v-gcp", bkpTargetName))
			log.FailOnError(err, "Failed to create GCP backup target.")
			log.InfoD("GCP Backup target - %v created successfully", bkpTarget.GetName())
			gcpBkpTargets = append(gcpBkpTargets, bkpTarget)
		})
		stepLog = "Create Azure(blob) Backup target."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			bkpTarget, err := bkpClient.CreateAzureBackupCredsAndTarget(tenantID, fmt.Sprintf("%v-azure", bkpTargetName))
			log.FailOnError(err, "Failed to create Azure backup target.")
			log.InfoD("Azure backup target - %v created successfully", bkpTarget.GetName())
			azureBkpTargets = append(azureBkpTargets, bkpTarget)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		deleteAllBkpTargets()
	})
})

var _ = Describe("{DeleteDataServiceAndValidateBackupAtObjectStore}", func() {
	bkpTargetName = bkpTargetName + pdsbkp.RandString(8)
	JustBeforeEach(func() {
		StartTorpedoTest("DeleteDataServiceAndValidateBackupAtObjectStore", "Delete the PDS data service should not delete the backups in backend", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
		bucketName = strings.ToLower("pds-automation-" + pdsbkp.RandString(5))
	})

	It("Delete the PDS data service should not delete the backups in backend", func() {
		stepLog := "Create backup target."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			bkpTarget, err := bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", bkpTargetName), bucketName, deploymentTargetID)
			log.FailOnError(err, "Failed to create S3 backup target.")
			log.InfoD("AWS S3 target - %v created successfully", bkpTarget.GetName())
			awsBkpTargets = append(awsBkpTargets, bkpTarget)
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
				currentTimestamp := time.Now().UTC()
				stepLog = "Deploy and validate data service"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
				})
				// TODO: Add defer for cleaning up the deployment to handle failure
				stepLog = "Perform adhoc backup and validate them"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					log.Infof("Deployment ID: %v, backuptargetID: %v", deployment.GetId(), awsBkpTargets[0].GetId())
					err := bkpClient.TriggerAndValidateAdhocBackup(deployment.GetId(), awsBkpTargets[0].GetId(), "s3")
					log.FailOnError(err, "Failed while performing adhoc backup")
				})
				Step("Delete Deployments", func() {
					log.InfoD("Deleting Deployment %v ", *deployment.ClusterResourceName)
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
					log.InfoD("Getting all PV and associated PVCs and deleting them")
					err = pdslib.DeletePvandPVCs(*deployment.ClusterResourceName, false)
					log.FailOnError(err, "Error while deleting PV and PVCs")
				})
				Step("Validate post deleting up the data service, no backup data got been deleted from the object store.", func() {
					folders, err := bkpClient.AWSStorageClient.ListFolders(currentTimestamp)
					log.FailOnError(err, "Failed while fetching the folders ")
					if len(folders) < 1 {
						log.FailOnError(fmt.Errorf("post deleting the data service, backup lost"), "no content found in the S3 bucket")
					}
					for _, folder := range folders {
						log.Infof("Found the folder: %v (default encrypted).", folder)
					}
				})
			}
		})
	})
	JustAfterEach(func() {
		err := bkpClient.AWSStorageClient.DeleteBucket(bucketName)
		log.FailOnError(err, "Failed while deleting the bucket")
		EndTorpedoTest()
	})
})

func deleteAllBkpTargets() {
	log.InfoD("Delete all the backup targets.")
	for _, bkptarget := range awsBkpTargets {
		err = bkpClient.DeleteAwsS3BackupCredsAndTarget(bkptarget.GetId())
		log.FailOnError(err, "Failed while deleting the Aws backup target")
	}
	for _, bkptarget := range azureBkpTargets {
		err = bkpClient.DeleteAzureBackupCredsAndTarget(bkptarget.GetId())
		log.FailOnError(err, "Failed while deleting the Azure backup target")
	}
	for _, bkptarget := range gcpBkpTargets {
		err = bkpClient.DeleteGoogleBackupCredsAndTarget(bkptarget.GetId())
		log.FailOnError(err, "Failed while deleting the GCP backup target")
	}
}
