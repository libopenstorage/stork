package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/portworx/torpedo/drivers/pds/lib"
	"log"
	"net/http"
)

func DeleteNamespace(ctx *gin.Context) {
	ns := ctx.Param("namespace")
	err := lib.DeleteK8sNamespace(ns)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	} else {
		ctx.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Namespace %s Deleted", ns)})
	}
}

func CreateNS(c *gin.Context) {
	ns, err := lib.CreateTempNS(6)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Failed to Create Namespace",
			"error":   err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"message":   "Namespace created successfully",
		"namespace": ns,
	})
}

func main() {
	router := gin.Default()
	router.DELETE("pxone/deletens/:namespace", DeleteNamespace)
	router.POST("pxone/createns", CreateNS)
	log.Fatal(router.Run(":8080"))
}
