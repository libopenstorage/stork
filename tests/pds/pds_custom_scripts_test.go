package tests

import (
	. "github.com/onsi/ginkgo/v2"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{DeleteAppConfigTemplates}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeleteAppConfigTemplates", "Deletes the app config templates ",
			pdsLabels, 0)
	})
	It("Delete App Config Templates", func() {
		stepLog := "Delete App Config Templates of given prefix"
		log.InfoD(stepLog)
		Step(stepLog, func() {
			appTemplatePrefix := params.CleanUpParams.AppTemplatePrefix
			err := controlPlane.DeleteAppConfigTemplates(tenantID, appTemplatePrefix)
			log.FailOnError(err, "error occured while deleting app config templates")
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{DeleteStorageConfigTemplates}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeleteStorageConfigTemplates", "Deletes the storage config templates ",
			pdsLabels, 0)
	})
	It("Delete Storage Config Templates", func() {
		stepLog := "Delete Storage Config Templates of given prefix"
		log.InfoD(stepLog)
		Step(stepLog, func() {
			storageTemplatePrefix := params.CleanUpParams.StorageTemplatePrefix
			err := controlPlane.DeleteStorageConfigTemplates(tenantID, storageTemplatePrefix)
			log.FailOnError(err, "error occured while deleting app config templates")
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{DeleteResourceSettingsTemplates}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeleteResourceSettingsTemplates",
			"Deletes the resource settings templates ", pdsLabels, 0)
	})
	It("Delete Resource Setting Templates", func() {
		stepLog := "Delete Resource Setting Templates of given prefix"
		log.InfoD(stepLog)
		Step(stepLog, func() {
			resourceTemplatePrefix := params.CleanUpParams.ResourceTemplatePrefix
			err := controlPlane.DeleteResourceSettingTemplates(tenantID, resourceTemplatePrefix)
			log.FailOnError(err, "error occured while deleting app config templates")
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{DeleteBackUpTargets}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeleteBackUpTargets",
			"Deletes the automation created backuptargets", pdsLabels, 0)
	})
	It("Deletes the automation created backuptargets", func() {
		stepLog := "Delete backup Targets"
		Step(stepLog, func() {
			err := CleanUpBackUpTargets(projectID, "automation--", "s3")
			log.FailOnError(err, "error occured while deleting backup target")
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{DeleteDeploymentTargets}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeleteUnhealthyDeploymentTargets",
			"Deletes the unhealthy deployment target clusters", pdsLabels, 0)
	})
	It("Delete Unhealthy Target Cluster from control plane", func() {
		stepLog := "Delete Unhealthy Deployment Targets"
		Step(stepLog, func() {
			err := pdslib.DeleteDeploymentTargets(projectID)
			log.FailOnError(err, "error occured while deleting deployment target")
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{DeleteBackUpTargetsAndCreds}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeleteBackUpTargetsAndCreds",
			"Deletes bkp targets and creds", pdsLabels, 0)
	})
	It("Delete Backup targets and credentials", func() {
		stepLog := "Delete Backup targets and credentials"
		Step(stepLog, func() {
			log.Debugf("deployment target %s", deploymentTargetID)
			err := pdslib.DeleteBackUpTargetsAndCredsBelongsToDeploymetTarget("", projectID, false)
			log.FailOnError(err, "error occured while deleting bkp targets and creds")
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})
