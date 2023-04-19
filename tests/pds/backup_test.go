package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsbkp "github.com/portworx/torpedo/drivers/pds/pdsbackup"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

const (
	bkpTargetName = "bkptarget-automation"
)

var (
	bkpClient                                     *pdsbkp.BackupClient
	awsBkpTargets, azureBkpTargets, gcpBkpTargets []*pds.ModelsBackupTarget
)

var _ = Describe("{ValidateBackupTargetsOnSupportedObjectStores}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("ValidateBackupTargetsOnSupportedObjectStores", "Validate backup targets for all supported object stores.", pdsLabels, 0)
		bkpClient, err = pdsbkp.InitializePdsBackup()
		log.FailOnError(err, "Failed to initialize backup for pds.")
	})

	It("Add all supported Object stores as backup target for data services", func() {
		stepLog := "Create AWS S3 Backup target."
		Step(stepLog, func() {
			log.InfoD(stepLog)
			bkpTarget, err := bkpClient.CreateAwsS3BackupCredsAndTarget(tenantID, fmt.Sprintf("%v-aws", bkpTargetName))
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
