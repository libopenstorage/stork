package utils

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/portworx/torpedo/drivers/pds/lib"
	"net/http"
	"os/exec"
)

// DeleteNS : This API Call will delete a given namespace
func DeleteNS(c *gin.Context) {
	ns := c.Param("namespace")
	err := lib.DeleteK8sNamespace(ns)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	} else {
		c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Namespace %s Deleted", ns)})
	}
}

// CreateNS : This API call will create a namespace in the cluster
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

// ExecuteHelmCmd : Execute the copied Helm Command
func ExecuteHelmCmd(c *gin.Context) {
	var payload HelmPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(400, gin.H{
			"error": err.Error(),
		})
		return
	}
	cmd := exec.Command("sh", "-c", payload.Command)
	_, err := cmd.CombinedOutput()
	if err != nil {
		c.JSON(500, gin.H{
			"error":   "Command execution failed",
			"details": err.Error(),
		})
		return
	}
	c.JSON(200, gin.H{
		"message": "Command received and executed successfully",
	})
}
