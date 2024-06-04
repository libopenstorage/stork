package tests

import (
	"fmt"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/iks"
	"github.com/portworx/torpedo/drivers/scheduler/oke"
	"net/url"
	"strings"
	"time"

	oputil "github.com/libopenstorage/operator/pkg/util/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/aks"
	"github.com/portworx/torpedo/drivers/scheduler/eks"
	"github.com/portworx/torpedo/drivers/scheduler/gke"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
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

		preUpgradeNodeDisksMap := make(map[string]map[string]int)
		printDisks := func(nodeDiskMap map[string]map[string]int) {
			stNodes := node.GetStorageNodes()
			for _, stNode := range stNodes {
				nodeDiskMap[stNode.Name] = make(map[string]int)
				drvNode, err := Inst().V.GetDriverNode(&stNode)
				if err != nil {
					log.Errorf("error getting driver node [%s]: [%v]", stNode.Name, err)
				} else {
					log.Infof("Node [%s] has disks: [%s]", stNode.Name, drvNode.Disks)
					for _, disk := range drvNode.Disks {
						nodeDiskMap[stNode.Name][disk.Medium.String()] = nodeDiskMap[stNode.Name][disk.Medium.String()] + 1
					}
				}
			}
		}

		printDisks(preUpgradeNodeDisksMap)

		for _, version := range versions {
			Step(fmt.Sprintf("start [%s] scheduler upgrade to version [%s]", Inst().S.String(), version), func() {
				stopSignal := make(chan struct{})

				var mError error
				opver, err := oputil.GetPxOperatorVersion()
				if err == nil && opver.GreaterThanOrEqual(PDBValidationMinOpVersion) {
					go DoPDBValidation(stopSignal, &mError)
					defer func() {
						close(stopSignal)
					}()
				} else {
					log.Warnf("PDB validation skipped. Current Px-Operator version: [%s], minimum required: [%s]. Error: [%v].", opver, PDBValidationMinOpVersion, err)
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

				// Sleep needed for IKS/OKE cluster upgrades
				if Inst().S.String() == iks.SchedName || Inst().S.String() == oke.SchedName {
					log.Warnf("This is [%s] scheduler, during Worker Pool upgrades, %s replaces all worker nodes. "+
						"The replacement might affect cluster capacity temporarily, requiring time for stabilization.", Inst().S.String(), strings.ToUpper(Inst().S.String()))
					log.Infof("Sleeping for 30 minutes to let the cluster stabilize after the upgrade..")
					time.Sleep(30 * time.Minute)
				}

				PrintK8sClusterInfo()
			})

			Step("update node drive endpoints", func() {
				// Update NodeRegistry, this is needed as node names and IDs might change after upgrade
				err = Inst().S.RefreshNodeRegistry()
				log.FailOnError(err, "Refresh Node Registry failed")

				// Refresh Driver Endpoints
				err = Inst().V.RefreshDriverEndpoints()
				log.FailOnError(err, "Refresh Driver Endpoints failed")

				// Printing pxctl status after the upgrade
				PrintPxctlStatus()
				postUpgradeNodeDisksMap := make(map[string]map[string]int)
				printDisks(postUpgradeNodeDisksMap)
				for nodeName, disks := range preUpgradeNodeDisksMap {
					if _, ok := postUpgradeNodeDisksMap[nodeName]; !ok {
						log.FailOnError(fmt.Errorf("node [%s] is not present in post upgrade node disks map", nodeName), "Node is not present in post upgrade node disks map")

					}
					for diskType, diskCount := range disks {
						if postUpgradeNodeDisksMap[nodeName][diskType] != diskCount {
							log.Errorf("Node [%s] disk type [%s] count mismatch. Pre upgrade: [%d], Post upgrade: [%d]", nodeName, diskType, preUpgradeNodeDisksMap[nodeName][diskType], diskCount)
							dash.VerifySafely(preUpgradeNodeDisksMap[nodeName][diskType], diskCount, fmt.Sprintf("Verify Node [%s] disk type [%s] count matched.", nodeName, diskType))
						}
					}
				}
			})

			Step("validate storage components", func() {
				urlToParse := fmt.Sprintf("%s/%s", Inst().StorageDriverUpgradeEndpointURL, Inst().StorageDriverUpgradeEndpointVersion)
				u, err := url.Parse(urlToParse)
				log.FailOnError(err, fmt.Sprintf("error parsing PX version the url [%s]", urlToParse))
				err = Inst().V.ValidateDriver(u.String(), true)
				if err != nil {
					PrintPxctlStatus()
				}
				// Printing cluster node info after the upgrade
				PrintK8sClusterInfo()
				dash.VerifyFatal(err, nil, fmt.Sprintf("verify volume driver after upgrade to %s", version))

			})

			// TODO: This currently doesn't work for most distros and commenting out this change, see PTX-22409
			/*if Inst().S.String() != aks.SchedName {
				dash.VerifyFatal(mError, nil, "validate no parallel upgrade of nodes")
			}*/

			Step("validate all apps after upgrade", func() {
				ValidateApplications(contexts)
			})
			PerformSystemCheck()
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