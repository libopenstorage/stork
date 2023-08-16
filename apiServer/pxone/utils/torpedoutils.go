package utils

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/tests"
	"math/rand"
	"net/http"
)

var (
	IsTorpedoInitDone bool                 // flag to check if Drivers init is done
	context           []*scheduler.Context // Application context will be maintained globally for now
)

// This method checks if test has done InitInstance once or not. If not, we will try to do it.
func checkTorpedoInit(c *gin.Context) bool {
	if !IsTorpedoInitDone {
		InitializeDrivers(c)
		if !IsTorpedoInitDone {
			c.JSON(http.StatusInternalServerError, gin.H{
				"message": "Torpedo Init failed",
			})
			return false
		}
	}
	return true
}

// InitializeDrivers : This API Call will init all Torpedo Drivers. This needs to be run as ginkgo test
// as multiple ginkgo and gomega dependencies are being called in InitInstance()
func InitializeDrivers(c *gin.Context) {
	tests.ParseFlags()
	tests.InitInstance()
	IsTorpedoInitDone = true
}

// GetNodes : This API will return list of all worker nodes in the Cluster
func GetNodes(c *gin.Context) {
	if !checkTorpedoInit(c) {
		return
	}
	nodes := node.GetWorkerNodes()
	c.JSON(http.StatusOK, gin.H{
		"message": "Nodes are: ",
		"nodes":   nodes,
	})
}

// RebootNode - API Call to Reboot node :
// pxone/rebootnode/all - Reboots all nodes in cluster
// pxone/rebootnode/random - Reboots a randomly selected node
// pxone/rebootnode/<nodename> - Reboots a node with specific node name
func RebootNode(c *gin.Context) {
	if !checkTorpedoInit(c) {
		return
	}
	nodes := node.GetWorkerNodes()
	nodename := c.Param("nodename")
	if nodename == "all" {
		for _, n := range nodes {
			err := tests.Inst().N.RebootNode(n, node.RebootNodeOpts{
				Force: true,
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         defaultCommandTimeout,
					TimeBeforeRetry: defaultCommandRetry,
				},
			})
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
		}
		c.JSON(http.StatusOK, gin.H{"message": "All Nodes successfully rebooted"})
		return
	} else if nodename == "random" {
		randomNode := nodes[rand.Intn(len(nodes))]
		err := tests.Inst().N.RebootNode(randomNode, node.RebootNodeOpts{
			Force: true,
			ConnectionOpts: node.ConnectionOpts{
				Timeout:         defaultCommandTimeout,
				TimeBeforeRetry: defaultCommandRetry,
			},
		})
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		} else {
			c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Randomly selected node %s successfully rebooted", randomNode.Name)})
		}
	} else {
		for _, n := range nodes {
			if n.Name == nodename {
				err := tests.Inst().N.RebootNode(n, node.RebootNodeOpts{
					Force: true,
					ConnectionOpts: node.ConnectionOpts{
						Timeout:         defaultCommandTimeout,
						TimeBeforeRetry: defaultCommandRetry,
					},
				})
				if err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				} else {
					c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Node with name %s successfully rebooted", n.Name)})
				}
				return
			}
		}
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Node with name %s not found", nodename)})
	}
}

// GetStorageNodes : Returns all Storage Node objects in the cluster
func GetStorageNodes(c *gin.Context) {
	if !checkTorpedoInit(c) {
		return
	}
	nodes, err := tests.GetStorageNodes()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"nodes": nodes,
		})
	}
}

// GetStorageLessNodes : Returns all Storage less node objects in the cluster
func GetStorageLessNodes(c *gin.Context) {
	if !checkTorpedoInit(c) {
		return
	}
	nodes := node.GetStorageLessNodes()
	c.JSON(http.StatusOK, gin.H{"nodes": nodes})
}

// CollectSupport : This API collects the support bundle
func CollectSupport(c *gin.Context) {
	if !checkTorpedoInit(c) {
		return
	}
	tests.CollectSupport()
	c.JSON(http.StatusOK, gin.H{"message": "Collection of support bundle done from Torpedo End"})
}

// ScheduleAppsAndValidate : This API schedules multiple applications on the cluster and validates them
// context is created as a global context to be accessed later in further tests
func ScheduleAppsAndValidate(c *gin.Context) {
	if !checkTorpedoInit(c) {
		return
	}
	context = tests.ScheduleApplications(testName)
	tests.ValidateApplications(context)
	c.JSON(http.StatusOK, gin.H{
		"message": "Apps Created and Validated successfully",
	})
}
