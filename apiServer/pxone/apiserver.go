package main

import (
	"github.com/gin-gonic/gin"
	"github.com/portworx/torpedo/apiServer/pxone/utils"
	"log"
)

// We will define all API calls here.
// Once Gin Server starts, it will initialise all APIs it contains.
// Future work : To have segregated APIs based on need -> We will have to create multiple main calls for initialising.
func main() {
	router := gin.Default()
	router.DELETE("pxone/deletens/:namespace", utils.DeleteNS)
	router.POST("pxone/createns", utils.CreateNS)
	router.POST("pxone/inittorpedo", utils.InitializeDrivers)
	router.GET("pxone/getnodes", utils.GetNodes)
	router.POST("pxone/rebootnode/:nodename", utils.RebootNode)
	router.GET("pxone/storagenodes", utils.GetStorageNodes)
	router.GET("pxone/storagelessnodes", utils.GetStorageLessNodes)
	router.POST("pxone/collectsupport", utils.CollectSupport)
	router.POST("pxone/scheduleapps/:appName", utils.ScheduleAppsAndValidate)
	router.POST("pxone/deploypxagent", utils.ExecuteHelmCmd)
	log.Fatal(router.Run(":8080"))
}
