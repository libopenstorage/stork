package main

import (
	"github.com/gin-gonic/gin"
	"github.com/portworx/torpedo/apiServer/taas/utils"
	"log"
)

// We will define all API calls here.
// Once Gin Server starts, it will initialise all APIs it contains.
// Future work : To have segregated APIs based on need -> We will have to create multiple main calls for initialising.
func main() {
	router := gin.Default()
	router.DELETE("taas/deletens/:namespace", utils.DeleteNS)
	router.POST("taas/createns", utils.CreateNS)
	router.POST("taas/inittorpedo", utils.InitializeDrivers)
	router.GET("taas/getnodes", utils.GetNodes)
	router.POST("taas/rebootnode/:nodename", utils.RebootNode)
	router.GET("taas/storagenodes", utils.GetStorageNodes)
	router.GET("taas/storagelessnodes", utils.GetStorageLessNodes)
	router.POST("taas/collectsupport", utils.CollectSupport)
	router.POST("taas/scheduleapps", utils.ScheduleAppsAndValidate)
	router.POST("taas/deploypxagent", utils.ExecuteHelmCmd)
	router.GET("taas/getclusterid/:namespace", utils.GetNamespaceID)
	router.GET("taas/getclusternodestatus", utils.GetNodeStatus)
	router.POST("taas/runhelmcmd", utils.ExecuteHelmCmd)
	router.GET("taas/pxversion", utils.GetPxVersion)
	router.GET("taas/ispxinstalled", utils.IsPxInstalled)
	router.GET("taas/getpxctloutput", utils.GetPxctlStatusOutput)
	router.GET("taas/getkubevirtvmsbyns", utils.GetVMsInNamespaces)
	router.GET("taas/getkubevirtvmsbynslabels", utils.GetVMsWithNamespaceLabels)
	router.POST("taas/namespaces/addLabel", utils.AddNSLabel)
	router.POST("taas/stork/upgrade", utils.UpgradeStork)
	router.DELETE("taas/deletepod", utils.DeletePod)
	router.GET("taas/getpxbackupnamespace", utils.GetPxBackupNamespace)
	router.POST("taas/createvolumesnapshotclass", utils.CreateVolumeSnapshotClass)
	log.Fatal(router.Run(":8080"))
}
