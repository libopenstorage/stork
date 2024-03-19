package tests

import (
	"fmt"
	"github.com/portworx/torpedo/drivers/scheduler/iks"
	"net/url"
	"strings"
	"time"

	"go.uber.org/multierr"

	"github.com/hashicorp/go-version"
	oputil "github.com/libopenstorage/operator/pkg/util/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/aks"
	"github.com/portworx/torpedo/drivers/scheduler/eks"
	"github.com/portworx/torpedo/drivers/scheduler/gke"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	v1 "k8s.io/api/core/v1"
)

var (
	OpVer24_1_0, _ = version.NewVersion("24.1.0-")
)
var _ = Describe("{UpgradeCluster}", func() {
	var contexts []*scheduler.Context

	JustBeforeEach(func() {
		tags := map[string]string{
			"upgradeCluster": "true",
		}
		StartTorpedoTest("UpgradeCluster", "Upgrade cluster test", tags, 0)
	})
	It("upgrade scheduler and ensure everything is running fine", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradecluster-%d", i))...)
		}

		ValidateApplications(contexts)

		var versions []string
		if len(Inst().SchedUpgradeHops) > 0 {
			versions = strings.Split(Inst().SchedUpgradeHops, ",")
		}
		Expect(versions).NotTo(BeEmpty())

		// TODO: Commenting out below changes as it doesn't work for most distros upgrades, see PTX-22409
		/*var mError error
		if Inst().S.String() != aks.SchedName {
			stopSignal := make(chan struct{})
			go getClusterNodesInfo(stopSignal, &mError)

			defer func() {
				close(stopSignal)
			}()
		}*/

		for _, version := range versions {
			Step(fmt.Sprintf("start [%s] scheduler upgrade to version [%s]", Inst().S.String(), version), func() {
				stopSignal := make(chan struct{})

				var mError error
				opver, err := oputil.GetPxOperatorVersion()
				if err == nil && opver.GreaterThanOrEqual(OpVer24_1_0) {
					go doPDBValidation(stopSignal, &mError)
					defer func() {
						close(stopSignal)
					}()
				}

				err = Inst().S.UpgradeScheduler(version)
				dash.VerifyFatal(mError, nil, "validation of PDB of px-storage during cluster upgrade successful")
				dash.VerifyFatal(err, nil, fmt.Sprintf("verify [%s] upgrade to [%s] is successful", Inst().S.String(), version))

				// Sleep needed for AKS cluster upgrades
				if Inst().S.String() == aks.SchedName {
					log.Warnf("Warning! This is [%s] scheduler, during Node Pool upgrades, AKS creates extra node, this node then becomes PX node. "+
						"After the Node Pool upgrade is complete, AKS deletes this extra node, but PX Storage object still around for about ~20-30 mins. "+
						"Recommended config is that you deploy PX with 6 nodes in 3 zones and set MaxStorageNodesPerZone to 2, "+
						"so when extra AKS node gets created, PX gets deployed as Storageless node, otherwise if PX gets deployed as Storage node, "+
						"PX storage objects will never be deleted and validation might fail!", Inst().S.String())
					log.Infof("Sleeping for 30 minutes to let the cluster stabilize after the upgrade..")
					time.Sleep(30 * time.Minute)
				}

				// Sleep needed for GKE cluster upgrades
				if Inst().S.String() == gke.SchedName {
					log.Warnf("This is [%s] scheduler, during Node Pool upgrades, GKE creates an extra node. "+
						"After the Node Pool upgrade is complete, GKE deletes this extra node, but it takes some time.", Inst().S.String())
					log.Infof("Sleeping for 10 minutes to let the cluster stabilize after the upgrade..")
					time.Sleep(10 * time.Minute)
				}

				// Sleep needed for EKS cluster upgrades
				if Inst().S.String() == eks.SchedName {
					log.Warnf("This is [%s] scheduler, during Node Group upgrades, EKS creates an extra node. "+
						"After the Node Group upgrade is complete, EKS deletes this extra node, but it takes some time.", Inst().S.String())
					log.Infof("Sleeping for 30 minutes to let the cluster stabilize after the upgrade..")
					time.Sleep(30 * time.Minute)
				}

				// Sleep needed for IKS cluster upgrades
				if Inst().S.String() == iks.SchedName {
					log.Warnf("This is [%s] scheduler, during Worker Pool upgrades, IKS replaces all worker nodes. "+
						"The replacement might affect cluster capacity temporarily, requiring time for stabilization.", Inst().S.String())
					log.Infof("Sleeping for 30 minutes to let the cluster stabilize after the upgrade..")
					time.Sleep(30 * time.Minute)
				}

				printK8sCluterInfo()
			})

			Step("validate storage components", func() {
				urlToParse := fmt.Sprintf("%s/%s", Inst().StorageDriverUpgradeEndpointURL, Inst().StorageDriverUpgradeEndpointVersion)
				u, err := url.Parse(urlToParse)
				log.FailOnError(err, fmt.Sprintf("error parsing PX version the url [%s]", urlToParse))
				err = Inst().V.ValidateDriver(u.String(), true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("verify volume driver after upgrade to %s", version))

				// Printing cluster node info after the upgrade
				printK8sCluterInfo()
			})

			// TODO: This currently doesn't work for most distros and commenting out this change, see PTX-22409
			/*if Inst().S.String() != aks.SchedName {
				dash.VerifyFatal(mError, nil, "validate no parallel upgrade of nodes")
			}*/

			Step("update node drive endpoints", func() {
				// Update NodeRegistry, this is needed as node names and IDs might change after upgrade
				err = Inst().S.RefreshNodeRegistry()
				log.FailOnError(err, "Refresh Node Registry failed")

				// Refresh Driver Endpoints
				err = Inst().V.RefreshDriverEndpoints()
				log.FailOnError(err, "Refresh Driver Endpoints failed")

				// Printing pxctl status after the upgrade
				PrintPxctlStatus()
			})

			Step("validate all apps after upgrade", func() {
				ValidateApplications(contexts)
			})
		}

		Step("destroy apps", func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

func getClusterNodesInfo(stopSignal <-chan struct{}, mError *error) {
	stNodes := node.GetStorageNodes()

	nodeSchedulableStatus := make(map[string]string)
	stNodeNames := make(map[string]bool)

	for _, stNode := range stNodes {
		stNodeNames[stNode.Name] = true
	}

	//Handling case where we have storageless node as kvdb node with dedicated kvdb device attached.
	kvdbNodes, _ := GetAllKvdbNodes()
	for _, kvdbNode := range kvdbNodes {
		sNode, err := node.GetNodeDetailsByNodeID(kvdbNode.ID)
		if err == nil {
			stNodeNames[sNode.Name] = true
		} else {
			log.Errorf("got error while getting with id [%s]", kvdbNode.ID)
		}
	}

	log.Infof("stnodes are %#v", stNodeNames)
	itr := 1
	for {
		log.Infof("K8s node validation. iteration: #%d", itr)
		select {
		// if it gets any value from stopSignal channel, it will exit
		case <-stopSignal:
			log.Infof("Exiting node validations routine")
			return
		default:
			nodeList, err := core.Instance().GetNodes()
			if err != nil {
				log.Errorf("Got error : %s", err.Error())
				*mError = err
				return
			}

			nodeNotReadyeCount := 0
			for _, k8sNode := range nodeList.Items {
				for _, status := range k8sNode.Status.Conditions {
					if status.Type == v1.NodeReady {
						nodeSchedulableStatus[k8sNode.Name] = string(status.Status)
						if status.Status != v1.ConditionTrue && stNodeNames[k8sNode.Name] {
							nodeNotReadyeCount += 1
						}
						break
					}
				}

			}
			if nodeNotReadyeCount > 1 {
				err = fmt.Errorf("multiple  nodes are Unschedulable at same time,"+
					"node status:%#v", nodeSchedulableStatus)
				log.Errorf("Got error : %s", err.Error())
				log.Infof("Node Details: %#v", nodeList.Items)
				output, err := Inst().N.RunCommand(stNodes[0], "pxctl status", node.ConnectionOpts{
					IgnoreError:     false,
					TimeBeforeRetry: defaultRetryInterval,
					Timeout:         defaultTimeout,
					Sudo:            true,
				})
				if err != nil {
					log.Errorf("failed to get pxctl status, Err: %v", err)
				}
				log.Infof(output)
				*mError = err
				return
			}
		}
		itr++
		time.Sleep(30 * time.Second)
	}
}

// printK8sClusterInfo prints info about K8s cluster nodes
func printK8sCluterInfo() {
	log.Info("Get cluster info..")
	t := func() (interface{}, bool, error) {
		nodeList, err := core.Instance().GetNodes()
		if err != nil {
			return "", true, fmt.Errorf("failed to get nodes, Err %v", err)
		}
		if len(nodeList.Items) > 0 {
			for _, node := range nodeList.Items {
				nodeType := "Worker"
				if core.Instance().IsNodeMaster(node) {
					nodeType = "Master"
				}
				log.Infof(
					"Node Name: %s, Node Type: %s, Kernel Version: %s, Kubernetes Version: %s, OS: %s, Container Runtime: %s",
					node.Name, nodeType,
					node.Status.NodeInfo.KernelVersion, node.Status.NodeInfo.KubeletVersion, node.Status.NodeInfo.OSImage,
					node.Status.NodeInfo.ContainerRuntimeVersion)
			}
			return "", false, nil
		}
		return "", false, fmt.Errorf("no nodes were found in the cluster")
	}
	if _, err := task.DoRetryWithTimeout(t, 1*time.Minute, 5*time.Second); err != nil {
		log.Warnf("failed to get k8s cluster info, Err: %v", err)
	}
}
func doPDBValidation(stopSignal <-chan struct{}, mError *error) {
	pdbValue, allowedDisruptions := GetPDBValue()
	isClusterParallelyUpgraded := false
	nodes, err := Inst().V.GetDriverNodes()
	if err != nil {
		*mError = multierr.Append(*mError, err)
		return
	}
	totalNodes := len(nodes)
	itr := 1
	for {
		log.Infof("PDB validation iteration: #%d", itr)
		select {
		case <-stopSignal:
			if allowedDisruptions > 1 && !isClusterParallelyUpgraded {
				err := fmt.Errorf("Cluster is not parallely upgraded")
				*mError = multierr.Append(*mError, err)
				log.Warnf("Cluster not parallely upgraded as expected")
			}
			log.Infof("Exiting PDB validation routine")
			return
		default:
			errorChan := make(chan error, 50)
			ValidatePDB(pdbValue, allowedDisruptions, totalNodes, &isClusterParallelyUpgraded, &errorChan)
			for err := range errorChan {
				*mError = multierr.Append(*mError, err)
			}
			if *mError != nil {
				return
			}
			itr++
			time.Sleep(10 * time.Second)
		}
	}

}
