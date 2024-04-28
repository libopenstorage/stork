package utils

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/portworx/torpedo/drivers/pds/lib"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/tests"
	v1 "k8s.io/api/core/v1"
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

func GetNamespaceID(c *gin.Context) {
	ns := c.Param("namespace")
	namespace, err := k8sCore.GetNamespace(ns)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	clusterID := string(namespace.GetObjectMeta().GetUID())
	c.JSON(http.StatusOK, gin.H{
		"clusterID": clusterID,
	})
}

func GetNodeStatus(c *gin.Context) {
	nodes, err := k8sCore.GetNodes()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	totalNodes := 0
	healthyNodes := 0
	unhealthyNodes := 0
	degradedNodes := 0
	for _, node := range nodes.Items {
		if _, isMaster := node.Labels["node-role.kubernetes.io/master"]; isMaster {
			continue
		}
		if _, isControlPlane := node.Labels["node-role.kubernetes.io/control-plane"]; isControlPlane {
			continue
		}
		totalNodes += 1
		for _, condition := range node.Status.Conditions {
			if condition.Type == v1.NodeReady {
				if condition.Status == v1.ConditionTrue {
					healthyNodes++
				} else if condition.Status == v1.ConditionFalse {
					unhealthyNodes++
				} else {
					degradedNodes++
				}
			}
		}
	}
	fmt.Printf("Total Nodes: %d\n", totalNodes)
	fmt.Printf("Healthy Nodes: %d\n", healthyNodes)
	fmt.Printf("Unhealthy Nodes: %d\n", unhealthyNodes)
	fmt.Printf("Degraded Nodes: %d\n", degradedNodes)
	c.JSON(http.StatusOK, gin.H{
		"TotalNodes":     totalNodes,
		"HealthyNodes":   healthyNodes,
		"UnhealthyNodes": unhealthyNodes,
		"DegradedNodes":  degradedNodes,
	})
}

// CreateVolumeSnapshotClass creates volume snapshot class
func CreateVolumeSnapshotClass(c *gin.Context) {
	log.Infof("Creating volume snapshot class")
	var createVolumeSnapshotClassRequest struct {
		VolumeSnapshotClassName      string `json:"volumeSnapshotClassName"`
		Provisioner                  string `json:"provisioner"`
		IsDefaultVolumeSnapshotClass bool   `json:"isDefaultVolumeSnapshotClass"`
		DeletePolicy                 string `json:"deletePolicy"`
	}
	if !checkTorpedoInit(c) {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error in init": fmt.Errorf("error in InitInstance()"),
		})
		return
	}
	if err := c.BindJSON(&createVolumeSnapshotClassRequest); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error for request": err.Error()})
		return
	}
	if len(createVolumeSnapshotClassRequest.VolumeSnapshotClassName) == 0 || len(createVolumeSnapshotClassRequest.Provisioner) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "volumesnapshotclass name  or provisioner cannot be empty", "vsc name": createVolumeSnapshotClassRequest.VolumeSnapshotClassName, "provisioner": createVolumeSnapshotClassRequest.Provisioner})
		return
	}
	_, err := tests.Inst().S.CreateVolumeSnapshotClasses(createVolumeSnapshotClassRequest.VolumeSnapshotClassName, createVolumeSnapshotClassRequest.Provisioner, createVolumeSnapshotClassRequest.IsDefaultVolumeSnapshotClass, createVolumeSnapshotClassRequest.DeletePolicy)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("VolumeSnapshotClass %v with provisioner %s and setting it as default: %v created successfully", createVolumeSnapshotClassRequest.VolumeSnapshotClassName, createVolumeSnapshotClassRequest.Provisioner, createVolumeSnapshotClassRequest.IsDefaultVolumeSnapshotClass)})
}
