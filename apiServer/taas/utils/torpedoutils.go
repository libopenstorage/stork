package utils

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/tests"
	"math/rand"
	"net/http"
	"regexp"
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
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Errorf("Error happened while doing InitInstance()"),
		})
		return
	}
	nodes := node.GetWorkerNodes()
	c.JSON(http.StatusOK, gin.H{
		"message": "Nodes are: ",
		"nodes":   nodes,
	})
}

// RebootNode - API Call to Reboot node :
// taas/rebootnode/all - Reboots all nodes in cluster
// taas/rebootnode/random - Reboots a randomly selected node
// taas/rebootnode/<nodename> - Reboots a node with specific node name
func RebootNode(c *gin.Context) {
	if !checkTorpedoInit(c) {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Errorf("Error happened while doing InitInstance()"),
		})
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
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Errorf("Error happened while doing InitInstance()"),
		})
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
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Errorf("Error happened while doing InitInstance()"),
		})
		return
	}
	nodes := node.GetStorageLessNodes()
	c.JSON(http.StatusOK, gin.H{"nodes": nodes})
}

// CollectSupport : This API collects the support bundle
func CollectSupport(c *gin.Context) {
	if !checkTorpedoInit(c) {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Errorf("Error happened while doing InitInstance()"),
		})
		return
	}
	tests.CollectSupport()
	c.JSON(http.StatusOK, gin.H{"message": "Collection of support bundle done from Torpedo End"})
}

// ScheduleAppsAndValidate : This API schedules multiple applications on the cluster and validates them
// context is created as a global context to be accessed later in further tests
func ScheduleAppsAndValidate(c *gin.Context) {
	if !checkTorpedoInit(c) {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Errorf("Error happened while doing InitInstance()"),
		})
		return
	}
	appToRun := c.Param("appName")
	tests.Inst().AppList = []string{appToRun}
	context = tests.ScheduleApplications(testName)
	for _, ctx := range context {
		tests.ValidateContext(ctx, &errChan)
	}
	for err := range errChan {
		errors = append(errors, err)
	}
	errStrings := make([]string, 0)
	for _, err := range errors {
		if err != nil {
			errStrings = append(errStrings, err.Error())
		}
	}
	if len(errStrings) > 0 {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": errStrings,
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"message": "Apps Created and Validated successfully",
		})
	}
}

// GetPxVersion This function returns the current Px Version in the Target Cluster
func GetPxVersion(c *gin.Context) {
	if !checkTorpedoInit(c) {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Errorf("Error happened while doing InitInstance()"),
		})
		return
	}
	version, err := tests.Inst().V.GetDriverVersion()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err,
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"version": version,
		})
	}
}

// IsPxInstalled This funtion returns true if Px is Installed
func IsPxInstalled(c *gin.Context) {
	if !checkTorpedoInit(c) {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Errorf("Error happened while doing InitInstance()"),
		})
		return
	}
	nodes := node.GetWorkerNodes()
	var pxinstalled bool
	var err error
	for _, n := range nodes {
		pxinstalled, err = tests.Inst().V.IsDriverInstalled(n)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err,
			})
			return
		}
		if !pxinstalled {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Px is not Installed",
				"node":  n.Name,
			})
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"status": "Px installed on all nodes successfully",
	})
}

// GetPxctlStatusOutput This function is used to return all elements in pxctl status output
func GetPxctlStatusOutput(c *gin.Context) {
	if !checkTorpedoInit(c) {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Errorf("Error happened while doing InitInstance()"),
		})
		return
	}
	cmd := "status"
	nodes := node.GetWorkerNodes()
	out, err := tests.Inst().V.GetPxctlCmdOutput(nodes[0], cmd)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err,
		})
	} else {
		var status PxctlStatus
		// Extract Status
		re := regexp.MustCompile(`Status: (.+)`)
		match := re.FindStringSubmatch(out)
		if len(match) > 0 {
			status.Status = match[1]
		}
		// Extract Node ID
		re = regexp.MustCompile(`Node ID: (.+)`)
		match = re.FindStringSubmatch(out)
		if len(match) > 0 {
			status.NodeID = match[1]
		}
		// Extract IP
		re = regexp.MustCompile(`IP: (.+)`)
		match = re.FindStringSubmatch(out)
		if len(match) > 0 {
			status.IP = match[1]
		}
		// Extract Cluster ID
		re = regexp.MustCompile(`Cluster ID: (.+)`)
		match = re.FindStringSubmatch(out)
		if len(match) > 0 {
			status.ClusterID = match[1]
		}
		// Extract Cluster UUID
		re = regexp.MustCompile(`Cluster UUID: (.+)`)
		match = re.FindStringSubmatch(out)
		if len(match) > 0 {
			status.ClusterUUID = match[1]
		}
		// Extract Total Used
		re = regexp.MustCompile(`Total Used\s+: (.+)`)
		match = re.FindStringSubmatch(out)
		if len(match) > 0 {
			status.TotalUsed = match[1]
		}
		// Extract Total Capacity
		re = regexp.MustCompile(`Total Capacity\s+: (.+)`)
		match = re.FindStringSubmatch(out)
		if len(match) > 0 {
			status.TotalCapacity = match[1]
		}
		c.JSON(http.StatusOK, gin.H{
			"output": status,
		})
	}
}
