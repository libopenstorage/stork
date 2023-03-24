package tests

import (
	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

// This testcase verifies Px Backup upgrade
var _ = Describe("{UpgradePxBackup}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("UpgradePxBackup", "Upgrading backup", nil, 0)
	})
	It("Upgrade Px Backup", func() {
		Step("Upgrade Px Backup", func() {
			log.InfoD("Upgrade Px Backup to version %s", latestPxBackupVersion)
			err := UpgradePxBackup(latestPxBackupVersion)
			dash.VerifyFatal(err, nil, "Verifying Px Backup upgrade completion")
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(make([]*scheduler.Context, 0))
		log.Infof("No cleanup required for this testcase")
	})
})
