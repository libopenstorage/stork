package tests

import (
	. "github.com/onsi/ginkgo"
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

var _ = Describe("{DeleteUnhealthyDeploymentTargets}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeleteUnhealthyDeploymentTargets",
			"Deletes the unhealthy deployment target clusters", pdsLabels, 0)
	})
	It("Delete Unhealthy Target Cluster from control plane", func() {
		stepLog := "Delete Unhealthy Deployment Targets"
		Step(stepLog, func() {
			err := targetCluster.DeleteDeploymentTargets(tenantID)
			log.FailOnError(err, "error occured while deleting deployment target")
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})
