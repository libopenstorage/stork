package tests

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/pkg/sched"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/sirupsen/logrus"
	appsapi "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storageapi "k8s.io/api/storage/v1"

	"github.com/portworx/torpedo/drivers/backup"
	// import aks driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/aks"
	// import backup driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/backup/portworx"
	// import aws driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/aws"
	// import gke driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/gke"
	// import vsphere driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/vsphere"

	// import ssh driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/scheduler"

	// import scheduler drivers to invoke it's init
	_ "github.com/portworx/torpedo/drivers/scheduler/dcos"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"

	// import scheduler drivers to invoke it's init
	_ "github.com/portworx/torpedo/drivers/scheduler/openshift"
	_ "github.com/portworx/torpedo/drivers/scheduler/rke"
	"github.com/portworx/torpedo/drivers/volume"

	// import portworx driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/volume/portworx"
	// import gce driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/volume/gce"
	// import aws driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/volume/aws"
	// import azure driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/volume/azure"
	// import generic csi driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/volume/generic_csi"
	"github.com/portworx/torpedo/pkg/log"

	yaml "gopkg.in/yaml.v2"
)

const (
	// SkipClusterScopedObjects describes option for skipping deletion of cluster wide objects
	SkipClusterScopedObjects = "skipClusterScopedObjects"
)

const (
	// defaultSpecsRoot specifies the default location of the base specs directory in the Torpedo container
	defaultSpecsRoot                     = "/specs"
	schedulerCliFlag                     = "scheduler"
	nodeDriverCliFlag                    = "node-driver"
	storageDriverCliFlag                 = "storage-driver"
	backupCliFlag                        = "backup-driver"
	specDirCliFlag                       = "spec-dir"
	appListCliFlag                       = "app-list"
	logLocationCliFlag                   = "log-location"
	logLevelCliFlag                      = "log-level"
	scaleFactorCliFlag                   = "scale-factor"
	minRunTimeMinsFlag                   = "minimun-runtime-mins"
	chaosLevelFlag                       = "chaos-level"
	storageUpgradeEndpointURLCliFlag     = "storage-upgrade-endpoint-url"
	storageUpgradeEndpointVersionCliFlag = "storage-upgrade-endpoint-version"
	provisionerFlag                      = "provisioner"
	storageNodesPerAZFlag                = "max-storage-nodes-per-az"
	configMapFlag                        = "config-map"
	enableStorkUpgradeFlag               = "enable-stork-upgrade"
	autopilotUpgradeImageCliFlag         = "autopilot-upgrade-version"
	csiGenericDriverConfigMapFlag        = "csi-generic-driver-config-map"
)

const (
	defaultScheduler                      = "k8s"
	defaultNodeDriver                     = "ssh"
	defaultStorageDriver                  = "pxd"
	defaultLogLocation                    = "/mnt/torpedo_support_dir"
	defaultBundleLocation                 = "/var/cores"
	defaultLogLevel                       = "debug"
	defaultAppScaleFactor                 = 1
	defaultMinRunTimeMins                 = 0
	defaultChaosLevel                     = 5
	defaultStorageUpgradeEndpointURL      = "https://install.portworx.com/upgrade"
	defaultStorageUpgradeEndpointVersion  = "2.1.1"
	defaultStorageProvisioner             = "portworx"
	defaultStorageNodesPerAZ              = 2
	defaultAutoStorageNodeRecoveryTimeout = 30 * time.Minute
	specObjAppWorkloadSizeEnvVar          = "SIZE"
)

const (
	waitResourceCleanup     = 2 * time.Minute
	defaultTimeout          = 5 * time.Minute
	defaultRetryInterval    = 10 * time.Second
	defaultCmdTimeout       = 20 * time.Second
	defaultCmdRetryInterval = 5 * time.Second
)

var (
	context = ginkgo.Context
	// Step is an alias for ginko "By" which represents a step in the spec
	Step         = ginkgo.By
	expect       = gomega.Expect
	haveOccurred = gomega.HaveOccurred
	beEmpty      = gomega.BeEmpty
	beNil        = gomega.BeNil
	equal        = gomega.Equal
)

// InitInstance is the ginkgo spec for initializing torpedo
func InitInstance() {
	var err error
	var token string
	if Inst().ConfigMap != "" {
		logrus.Infof("Using Config Map: %s ", Inst().ConfigMap)
		token, err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
		expect(err).NotTo(haveOccurred())
		logrus.Infof("Token used for initializing: %s ", token)
	} else {
		token = ""
	}

	err = Inst().S.Init(scheduler.InitOptions{
		SpecDir:             Inst().SpecDir,
		VolDriverName:       Inst().V.String(),
		NodeDriverName:      Inst().N.String(),
		SecretConfigMapName: Inst().ConfigMap,
		CustomAppConfig:     Inst().CustomAppConfig,
		StorageProvisioner:  Inst().Provisioner,
		SecretType:          Inst().SecretType,
		VaultAddress:        Inst().VaultAddress,
		VaultToken:          Inst().VaultToken,
	})
	expect(err).NotTo(haveOccurred())

	err = Inst().N.Init()
	expect(err).NotTo(haveOccurred())

	err = Inst().V.Init(Inst().S.String(), Inst().N.String(), token, Inst().Provisioner, Inst().CsiGenericDriverConfigMap)
	expect(err).NotTo(haveOccurred())

	if Inst().Backup != nil {
		err = Inst().Backup.Init(Inst().S.String(), Inst().N.String(), Inst().V.String(), token)
		expect(err).NotTo(haveOccurred())
	}
}

// ValidateCleanup checks that there are no resource leaks after the test run
func ValidateCleanup() {
	Step(fmt.Sprintf("validate cleanup of resources used by the test suite"), func() {
		t := func() (interface{}, bool, error) {
			if err := Inst().V.ValidateVolumeCleanup(); err != nil {
				return "", true, err
			}

			return "", false, nil
		}

		_, err := task.DoRetryWithTimeout(t, waitResourceCleanup, 10*time.Second)
		if err != nil {
			logrus.Info("an error occurred, collecting bundle")
			CollectSupport()
		}
		expect(err).NotTo(haveOccurred())
	})
}

func processError(err error, errChan ...*chan error) {
	updateChannel(err, errChan...)
	expect(err).NotTo(haveOccurred())
}

func updateChannel(err error, errChan ...*chan error) {
	if len(errChan) > 0 && err != nil {
		*errChan[0] <- err
	}
}

// ValidateContext is the ginkgo spec for validating a scheduled context
func ValidateContext(ctx *scheduler.Context, errChan ...*chan error) {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	ginkgo.Describe(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {
		var timeout time.Duration
		appScaleFactor := time.Duration(Inst().ScaleFactor)
		if ctx.ReadinessTimeout == time.Duration(0) {
			timeout = appScaleFactor * defaultTimeout
		} else {
			timeout = appScaleFactor * ctx.ReadinessTimeout
		}

		Step(fmt.Sprintf("validate %s app's volumes", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateVolumes(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("wait for %s app to start running", ctx.App.Key), func() {
			err := Inst().S.WaitForRunning(ctx, timeout, defaultRetryInterval)
			processError(err, errChan...)
		})

		Step(fmt.Sprintf("validate if %s app's volumes are setup", ctx.App.Key), func() {
			if ctx.SkipVolumeValidation {
				return
			}

			vols, err := Inst().S.GetVolumes(ctx)
			processError(err, errChan...)

			for _, vol := range vols {
				Step(fmt.Sprintf("validate if %s app's volume: %v is setup", ctx.App.Key, vol), func() {
					err := Inst().V.ValidateVolumeSetup(vol)
					processError(err, errChan...)
				})
			}
		})
	})
}

// ValidateVolumes is the ginkgo spec for validating volumes of a context
func ValidateVolumes(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of an app's volumes", func() {
		var err error
		Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
			appScaleFactor := time.Duration(Inst().ScaleFactor)
			err = Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval, nil)
			processError(err, errChan...)
		})

		var vols map[string]map[string]string
		Step(fmt.Sprintf("get %s app's volume's custom parameters", ctx.App.Key), func() {
			vols, err = Inst().S.GetVolumeParameters(ctx)
			processError(err, errChan...)
		})

		for vol, params := range vols {
			if Inst().ConfigMap != "" {
				params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
				processError(err, errChan...)
			}
			if ctx.RefreshStorageEndpoint {
				params["refresh-endpoint"] = "true"
			}
			Step(fmt.Sprintf("get %s app's volume: %s inspected by the volume driver", ctx.App.Key, vol), func() {
				err = Inst().V.ValidateCreateVolume(vol, params)
				processError(err, errChan...)
			})
		}
	})
}

// GetVolumeParameters retuns volume parameters for all volumes for given context
func GetVolumeParameters(ctx *scheduler.Context) map[string]map[string]string {
	var vols map[string]map[string]string
	var err error
	Step(fmt.Sprintf("get %s app's volume's custom parameters", ctx.App.Key), func() {
		vols, err = Inst().S.GetVolumeParameters(ctx)
		expect(err).NotTo(haveOccurred())
	})
	return vols
}

// UpdateVolumeInVolumeParameters modifies volume parameters with correct PV name from PVC
func UpdateVolumeInVolumeParameters(volParam map[string]map[string]string) map[string]map[string]string {
	updatedVolumeParam := make(map[string]map[string]string)
	for _, param := range volParam {
		pvcName, pvcNamespace := param[k8s.PvcNameKey], param[k8s.PvcNamespaceKey]
		PVName, err := Inst().S.GetVolumeDriverVolumeName(pvcName, pvcNamespace)
		expect(err).NotTo(haveOccurred())
		updatedVolumeParam[PVName] = param
	}
	return updatedVolumeParam
}

// ValidateVolumeParameters validates volume parameters using volume driver
func ValidateVolumeParameters(volParam map[string]map[string]string) {
	var err error
	for vol, params := range volParam {
		if Inst().ConfigMap != "" {
			params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
			expect(err).NotTo(haveOccurred())
		}
		Step(fmt.Sprintf("get volume: %s inspected by the volume driver", vol), func() {
			err = Inst().V.ValidateCreateVolume(vol, params)
			expect(err).NotTo(haveOccurred())
		})
	}
}

// ValidateRestoredApplications validates applications restored by backup driver
func ValidateRestoredApplications(contexts []*scheduler.Context, volumeParameters map[string]map[string]string) {
	var updatedVolumeParams map[string]map[string]string
	volOptsMap := make(map[string]bool)
	volOptsMap[SkipClusterScopedObjects] = true

	for _, ctx := range contexts {
		ginkgo.Describe(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {

			Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
				appScaleFactor := time.Duration(Inst().ScaleFactor)
				volOpts := mapToVolumeOptions(volOptsMap)
				err := Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval, volOpts)
				expect(err).NotTo(haveOccurred())
			})

			Step(fmt.Sprintf("wait for %s app to start running", ctx.App.Key), func() {
				appScaleFactor := time.Duration(Inst().ScaleFactor)
				err := Inst().S.WaitForRunning(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval)
				expect(err).NotTo(haveOccurred())
			})

			updatedVolumeParams = UpdateVolumeInVolumeParameters(volumeParameters)
			logrus.Infof("Updated parameter list: [%+v]\n", updatedVolumeParams)
			ValidateVolumeParameters(updatedVolumeParams)

			Step(fmt.Sprintf("validate if %s app's volumes are setup", ctx.App.Key), func() {
				vols, err := Inst().S.GetVolumes(ctx)
				logrus.Infof("List of volumes from scheduler driver :[%+v] \n for context : [%+v]\n", vols, ctx)
				expect(err).NotTo(haveOccurred())

				for _, vol := range vols {
					Step(fmt.Sprintf("validate if %s app's volume: %v is setup", ctx.App.Key, vol), func() {
						err := Inst().V.ValidateVolumeSetup(vol)
						expect(err).NotTo(haveOccurred())
					})
				}
			})
		})
	}
}

// TearDownContext is the ginkgo spec for tearing down a scheduled context
// In the tear down flow we first want to delete volumes, then applications and only then we want to delete StorageClasses
// StorageClass has to be deleted last because it has information that is required for when deleting PVC, if StorageClass objects are deleted before
// deleting PVCs, especially with CSI + Auth enabled, PVC deletion will fail as Auth params are stored inside StorageClass objects
func TearDownContext(ctx *scheduler.Context, opts map[string]bool) {
	context("For tearing down of an app context", func() {
		var err error
		var originalSkipClusterScopedObjects bool

		if opts != nil {
			// Save original value of SkipClusterScopedObjects, if it exists
			originalSkipClusterScopedObjects = opts[SkipClusterScopedObjects]
		} else {
			opts = make(map[string]bool) // If opts was passed as nil make it
		}

		opts[SkipClusterScopedObjects] = true // Skip tearing down cluster scope objects
		options := mapToVolumeOptions(opts)

		// Tear down storage objects
		vols := DeleteVolumes(ctx, options)

		// Tear down application
		Step(fmt.Sprintf("start destroying %s app", ctx.App.Key), func() {
			err = Inst().S.Destroy(ctx, opts)
			expect(err).NotTo(haveOccurred())
		})

		if !ctx.SkipVolumeValidation {
			ValidateVolumesDeleted(ctx.App.Key, vols)
		}

		// Delete Cluster Scope objects
		if !originalSkipClusterScopedObjects {
			opts[SkipClusterScopedObjects] = false // Tearing down cluster scope objects
			options := mapToVolumeOptions(opts)
			DeleteVolumes(ctx, options)
		}

	})
}

// DeleteVolumes deletes volumes of a given context
func DeleteVolumes(ctx *scheduler.Context, options *scheduler.VolumeOptions) []*volume.Volume {
	var err error
	var vols []*volume.Volume
	Step(fmt.Sprintf("destroy the %s app's volumes", ctx.App.Key), func() {
		vols, err = Inst().S.DeleteVolumes(ctx, options)
		expect(err).NotTo(haveOccurred())
	})
	return vols
}

// ValidateVolumesDeleted checks it given volumes got deleted
func ValidateVolumesDeleted(appName string, vols []*volume.Volume) {
	for _, vol := range vols {
		Step(fmt.Sprintf("validate %s app's volume %s has been deleted in the volume driver",
			appName, vol.Name), func() {
			err := Inst().V.ValidateDeleteVolume(vol)
			expect(err).NotTo(haveOccurred())
		})
	}
}

// DeleteVolumesAndWait deletes volumes of given context and waits till they are deleted
func DeleteVolumesAndWait(ctx *scheduler.Context, options *scheduler.VolumeOptions) {
	vols := DeleteVolumes(ctx, options)
	ValidateVolumesDeleted(ctx.App.Key, vols)
}

// GetAppNamespace returns namespace in which context is created
func GetAppNamespace(ctx *scheduler.Context, taskname string) string {
	return ctx.App.GetID(fmt.Sprintf("%s-%s", taskname, Inst().InstanceID))
}

// ScheduleApplications schedules but does not wait for applications
func ScheduleApplications(testname string) []*scheduler.Context {
	var contexts []*scheduler.Context
	var err error

	Step("schedule applications", func() {
		taskName := fmt.Sprintf("%s-%v", testname, Inst().InstanceID)
		contexts, err = Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
			AppKeys:            Inst().AppList,
			StorageProvisioner: Inst().Provisioner,
		})
		expect(err).NotTo(haveOccurred())
		expect(contexts).NotTo(beEmpty())
	})

	return contexts
}

// ValidateApplications validates applications
func ValidateApplications(contexts []*scheduler.Context) {
	Step("validate applications", func() {
		for _, ctx := range contexts {
			ValidateContext(ctx)
		}
	})
}

// StartVolDriverAndWait starts volume driver on given app nodes
func StartVolDriverAndWait(appNodes []node.Node) {
	context(fmt.Sprintf("starting volume driver %s", Inst().V.String()), func() {
		Step(fmt.Sprintf("start volume driver on nodes: %v", appNodes), func() {
			for _, n := range appNodes {
				err := Inst().V.StartDriver(n)
				expect(err).NotTo(haveOccurred())
			}
		})

		Step(fmt.Sprintf("wait for volume driver to start on nodes: %v", appNodes), func() {
			for _, n := range appNodes {
				err := Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
				expect(err).NotTo(haveOccurred())
			}
		})

	})
}

// StopVolDriverAndWait stops volume driver on given app nodes and waits till driver is down
func StopVolDriverAndWait(appNodes []node.Node) {
	context(fmt.Sprintf("stopping volume driver %s", Inst().V.String()), func() {
		Step(fmt.Sprintf("stop volume driver on nodes: %v", appNodes), func() {
			err := Inst().V.StopDriver(appNodes, false, nil)
			expect(err).NotTo(haveOccurred())
		})

		Step(fmt.Sprintf("wait for volume driver to stop on nodes: %v", appNodes), func() {
			for _, n := range appNodes {
				err := Inst().V.WaitDriverDownOnNode(n)
				expect(err).NotTo(haveOccurred())
			}
		})

	})
}

// CrashVolDriverAndWait crashes volume driver on given app nodes and waits till driver is back up
func CrashVolDriverAndWait(appNodes []node.Node) {
	context(fmt.Sprintf("crashing volume driver %s", Inst().V.String()), func() {
		Step(fmt.Sprintf("crash volume driver on nodes: %v", appNodes), func() {
			err := Inst().V.StopDriver(appNodes, true, nil)
			expect(err).NotTo(haveOccurred())
		})

		Step(fmt.Sprintf("wait for volume driver to start on nodes: %v", appNodes), func() {
			for _, n := range appNodes {
				err := Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
				expect(err).NotTo(haveOccurred())
			}
		})

	})
}

// ValidateAndDestroy validates application and then destroys them
func ValidateAndDestroy(contexts []*scheduler.Context, opts map[string]bool) {
	Step("validate apps", func() {
		for _, ctx := range contexts {
			ValidateContext(ctx)
		}
	})

	Step("destroy apps", func() {
		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})
}

// AddLabelsOnNode adds labels on the node
func AddLabelsOnNode(n node.Node, labels map[string]string) error {
	for labelKey, labelValue := range labels {
		if err := Inst().S.AddLabelOnNode(n, labelKey, labelValue); err != nil {
			return err
		}
	}
	return nil
}

// ValidateStoragePools is the ginkgo spec for validating storage pools
func ValidateStoragePools(contexts []*scheduler.Context) {

	strExpansionEnabled, err := Inst().V.IsStorageExpansionEnabled()
	expect(err).NotTo(haveOccurred())

	if strExpansionEnabled {
		var wSize uint64
		var workloadSizesByPool = make(map[string]uint64)
		logrus.Debugf("storage expansion enabled on at least one storage pool")
		// for each replica set add the workloadSize of app workload to each storage pool where replica resides on
		for _, ctx := range contexts {
			Step(fmt.Sprintf("get replica sets for app: %s's volumes", ctx.App.Key), func() {
				appVolumes, err := Inst().S.GetVolumes(ctx)
				expect(err).NotTo(haveOccurred())
				expect(appVolumes).NotTo(beEmpty())
				for _, vol := range appVolumes {
					if Inst().S.IsAutopilotEnabledForVolume(vol) {
						replicaSets, err := Inst().V.GetReplicaSets(vol)
						expect(err).NotTo(haveOccurred())
						expect(replicaSets).NotTo(beEmpty())
						for _, poolUUID := range replicaSets[0].PoolUuids {
							wSize, err = Inst().S.GetWorkloadSizeFromAppSpec(ctx)
							expect(err).NotTo(haveOccurred())
							workloadSizesByPool[poolUUID] += wSize
							logrus.Debugf("pool: %s workloadSize increased by: %d total now: %d", poolUUID, wSize, workloadSizesByPool[poolUUID])
						}
					}
				}
			})
		}

		// update each storage pool with the app workload sizes
		nodes := node.GetWorkerNodes()
		expect(nodes).NotTo(beEmpty())
		for _, n := range nodes {
			for id, sPool := range n.StoragePools {
				if workloadSizeForPool, ok := workloadSizesByPool[sPool.Uuid]; ok {
					n.StoragePools[id].WorkloadSize = workloadSizeForPool
				}

				logrus.Debugf("pool: %s InitialSize: %d WorkloadSize: %d", sPool.Uuid, sPool.StoragePoolAtInit.TotalSize, n.StoragePools[id].WorkloadSize)
			}
			err = node.UpdateNode(n)
			expect(err).NotTo(haveOccurred())
		}
	}

	err = Inst().V.ValidateStoragePools()
	expect(err).NotTo(haveOccurred())

}

// DescribeNamespace takes in the scheduler contexts and describes each object within the test context.
func DescribeNamespace(contexts []*scheduler.Context) {
	context(fmt.Sprintf("generating namespace info..."), func() {
		Step(fmt.Sprintf("Describe Namespace objects for test %s \n", ginkgo.CurrentGinkgoTestDescription().TestText), func() {
			for _, ctx := range contexts {
				filename := fmt.Sprintf("%s/%s-%s.namespace.log", defaultBundleLocation, ctx.App.Key, ctx.UID)
				namespaceDescription, err := Inst().S.Describe(ctx)
				if err != nil {
					logrus.Errorf("failed to describe namespace for [%s] %s. Cause: %v", ctx.UID, ctx.App.Key, err)
				}
				if err = ioutil.WriteFile(filename, []byte(namespaceDescription), 0755); err != nil {
					logrus.Errorf("failed to save file %s. Cause: %v", filename, err)
				}
			}
		})
	})
}

// ValidateClusterSize validates number of storage nodes in given cluster
// using total cluster size `count` and max_storage_nodes_per_zone
func ValidateClusterSize(count int64) {
	zones, err := Inst().N.GetZones()
	logrus.Debugf("ASG is running in [%+v] zones\n", zones)
	perZoneCount := count / int64(len(zones))

	// Validate total node count
	currentNodeCount, err := Inst().N.GetASGClusterSize()
	expect(err).NotTo(haveOccurred())
	expect(perZoneCount*int64(len(zones))).Should(equal(currentNodeCount),
		"ASG cluster size is not as expected."+
			" Current size is [%d]. Expected ASG size is [%d]",
		currentNodeCount, perZoneCount*int64(len(zones)))

	// Validate storage node count
	var expectedStorageNodesPerZone int
	if Inst().MaxStorageNodesPerAZ <= int(perZoneCount) {
		expectedStorageNodesPerZone = Inst().MaxStorageNodesPerAZ
	} else {
		expectedStorageNodesPerZone = int(perZoneCount)
	}
	storageNodes, err := getStorageNodes()

	expect(err).NotTo(haveOccurred())
	expect(len(storageNodes)).Should(equal(expectedStorageNodesPerZone*len(zones)),
		"Current number of storeage nodes [%d] does not match expected number of storage nodes [%d]."+
			"List of storage nodes:[%v]",
		len(storageNodes), expectedStorageNodesPerZone*len(zones), storageNodes)

	logrus.Infof("Validated successfully that [%d] storage nodes are present", len(storageNodes))
}

func getStorageNodes() ([]node.Node, error) {

	storageNodes := []node.Node{}
	nodes := node.GetStorageDriverNodes()

	for _, node := range nodes {
		devices, err := Inst().V.GetStorageDevices(node)
		if err != nil {
			return nil, err
		}
		if len(devices) > 0 {
			storageNodes = append(storageNodes, node)
		}
	}
	return storageNodes, nil
}

// CollectSupport creates a support bundle
func CollectSupport() {
	context(fmt.Sprintf("generating support bundle..."), func() {
		nodes := node.GetWorkerNodes()
		expect(nodes).NotTo(beEmpty())

		for _, n := range nodes {
			if !n.IsStorageDriverInstalled {
				continue
			}
			Step(fmt.Sprintf("save all useful logs on node %s", n.SchedulerNodeName), func() {

				Inst().V.CollectDiags(n)

				journalCmd := fmt.Sprintf("journalctl -l > %s/all_journal_%v.log", Inst().BundleLocation, time.Now().Format(time.RFC3339))
				runCmd(journalCmd, n)

				runCmd(fmt.Sprintf("journalctl -lu portworx* > %s/portworx.log", Inst().BundleLocation), n)

				Inst().S.SaveSchedulerLogsToFile(n, Inst().BundleLocation)

				runCmd(fmt.Sprintf("dmesg -T > %s/dmesg.log", Inst().BundleLocation), n)

				runCmd(fmt.Sprintf("lsblk > %s/lsblk.log", Inst().BundleLocation), n)

				runCmd(fmt.Sprintf("cat /proc/mounts > %s/mounts.log", Inst().BundleLocation), n)

				// this is a small tweak especially for providers like openshift, aws where oci-mon saves this file
				// with root read permissions only but collect support bundle is a non-root user
				runCmd(fmt.Sprintf("chmod 755 %s/oci.log", Inst().BundleLocation), n)
			})
		}
	})
}

func runCmd(cmd string, n node.Node) {
	_, err := Inst().N.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         defaultCmdTimeout,
		TimeBeforeRetry: defaultCmdRetryInterval,
		Sudo:            true,
	})
	if err != nil {
		logrus.Warnf("failed to run cmd: %s. err: %v", cmd, err)
	}
}

// PerformSystemCheck check if core files are present on each node
func PerformSystemCheck() {
	context(fmt.Sprintf("checking for core files..."), func() {
		Step(fmt.Sprintf("verifying if core files are present on each node"), func() {
			nodes := node.GetWorkerNodes()
			expect(nodes).NotTo(beEmpty())
			for _, n := range nodes {
				if !n.IsStorageDriverInstalled {
					continue
				}
				logrus.Infof("looking for core files on node %s", n.Name)
				file, err := Inst().N.SystemCheck(n, node.ConnectionOpts{
					Timeout:         2 * time.Minute,
					TimeBeforeRetry: 10 * time.Second,
				})
				if len(file) != 0 || err != nil {
					logrus.Info("an error occurred, collecting bundle")
					CollectSupport()
				}
				expect(err).NotTo(haveOccurred())
				expect(file).To(beEmpty())
			}
		})
	})
}

// ChangeNamespaces updates the namespace in supplied in-memory contexts.
// It does not apply changes on scheduler
func ChangeNamespaces(contexts []*scheduler.Context,
	namespaceMapping map[string]string) error {

	for _, ctx := range contexts {
		for _, spec := range ctx.App.SpecList {
			err := updateNamespace(spec, namespaceMapping)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func updateNamespace(in interface{}, namespaceMapping map[string]string) error {
	if specObj, ok := in.(*appsapi.Deployment); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*appsapi.StatefulSet); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*appsapi.DaemonSet); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*v1.Service); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*v1.PersistentVolumeClaim); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storageapi.StorageClass); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*snapv1.VolumeSnapshot); ok {
		namespace := namespaceMapping[specObj.Metadata.GetNamespace()]
		specObj.Metadata.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storkapi.GroupVolumeSnapshot); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*v1.Secret); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*v1.ConfigMap); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storkapi.Rule); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*v1.Pod); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storkapi.ClusterPair); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storkapi.Migration); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storkapi.MigrationSchedule); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storkapi.BackupLocation); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storkapi.ApplicationBackup); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storkapi.SchedulePolicy); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storkapi.ApplicationRestore); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storkapi.ApplicationClone); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storkapi.VolumeSnapshotRestore); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*apapi.AutopilotRule); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*v1.ServiceAccount); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*rbacv1.Role); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*rbacv1.RoleBinding); ok {
		namespace := namespaceMapping[specObj.GetNamespace()]
		specObj.SetNamespace(namespace)
		return nil
	}

	return fmt.Errorf("unsupported object while setting namespace: %v", reflect.TypeOf(in))
}

// AfterEachTest runs collect support bundle after each test when it fails
func AfterEachTest(contexts []*scheduler.Context) {
	logrus.Debugf("contexts: %v", contexts)
	if ginkgo.CurrentGinkgoTestDescription().Failed {
		CollectSupport()
		DescribeNamespace(contexts)
	}
}

// Inst returns the Torpedo instances
func Inst() *Torpedo {
	return instance
}

var instance *Torpedo
var once sync.Once

// Torpedo is the torpedo testsuite
type Torpedo struct {
	InstanceID                          string
	S                                   scheduler.Driver
	V                                   volume.Driver
	N                                   node.Driver
	SpecDir                             string
	AppList                             []string
	LogLoc                              string
	LogLevel                            string
	ScaleFactor                         int
	StorageDriverUpgradeEndpointURL     string
	StorageDriverUpgradeEndpointVersion string
	EnableStorkUpgrade                  bool
	MinRunTimeMins                      int
	ChaosLevel                          int
	Provisioner                         string
	MaxStorageNodesPerAZ                int
	DestroyAppTimeout                   time.Duration
	DriverStartTimeout                  time.Duration
	AutoStorageNodeRecoveryTimeout      time.Duration
	ConfigMap                           string
	BundleLocation                      string
	CustomAppConfig                     map[string]scheduler.AppConfig
	Backup                              backup.Driver
	SecretType                          string
	VaultAddress                        string
	VaultToken                          string
	SchedUpgradeHops                    string
	AutopilotUpgradeImage               string
	CsiGenericDriverConfigMap           string
}

// ParseFlags parses command line flags
func ParseFlags() {
	var err error
	var s, n, v, backupDriverName, specDir, logLoc, logLevel, appListCSV, provisionerName, configMapName string
	var schedulerDriver scheduler.Driver
	var volumeDriver volume.Driver
	var nodeDriver node.Driver
	var backupDriver backup.Driver
	var appScaleFactor int
	var volUpgradeEndpointURL string
	var volUpgradeEndpointVersion string
	var minRunTimeMins int
	var chaosLevel int
	var storageNodesPerAZ int
	var destroyAppTimeout time.Duration
	var driverStartTimeout time.Duration
	var autoStorageNodeRecoveryTimeout time.Duration
	var bundleLocation string
	var customConfigPath string
	var customAppConfig map[string]scheduler.AppConfig
	var enableStorkUpgrade bool
	var secretType string
	var vaultAddress string
	var vaultToken string
	var schedUpgradeHops string
	var autopilotUpgradeImage string
	var csiGenericDriverConfigMapName string

	flag.StringVar(&s, schedulerCliFlag, defaultScheduler, "Name of the scheduler to use")
	flag.StringVar(&n, nodeDriverCliFlag, defaultNodeDriver, "Name of the node driver to use")
	flag.StringVar(&v, storageDriverCliFlag, defaultStorageDriver, "Name of the storage driver to use")
	flag.StringVar(&backupDriverName, backupCliFlag, "", "Name of the backup driver to use")
	flag.StringVar(&specDir, specDirCliFlag, defaultSpecsRoot, "Root directory containing the application spec files")
	flag.StringVar(&logLoc, logLocationCliFlag, defaultLogLocation,
		"Path to save logs/artifacts upon failure. Default: /mnt/torpedo_support_dir")
	flag.StringVar(&logLevel, logLevelCliFlag, defaultLogLevel, "Log level")
	flag.IntVar(&appScaleFactor, scaleFactorCliFlag, defaultAppScaleFactor, "Factor by which to scale applications")
	flag.IntVar(&minRunTimeMins, minRunTimeMinsFlag, defaultMinRunTimeMins, "Minimum Run Time in minutes for appliation deletion tests")
	flag.IntVar(&chaosLevel, chaosLevelFlag, defaultChaosLevel, "Application deletion frequency in minutes")
	flag.StringVar(&volUpgradeEndpointURL, storageUpgradeEndpointURLCliFlag, defaultStorageUpgradeEndpointURL,
		"Endpoint URL link which will be used for upgrade storage driver")
	flag.StringVar(&volUpgradeEndpointVersion, storageUpgradeEndpointVersionCliFlag, defaultStorageUpgradeEndpointVersion,
		"Endpoint version which will be used for checking version after upgrade storage driver")
	flag.BoolVar(&enableStorkUpgrade, enableStorkUpgradeFlag, false, "Enable stork upgrade during storage driver upgrade")
	flag.StringVar(&appListCSV, appListCliFlag, "", "Comma-separated list of apps to run as part of test. The names should match directories in the spec dir.")
	flag.StringVar(&provisionerName, provisionerFlag, defaultStorageProvisioner, "Name of the storage provisioner Portworx or CSI.")
	flag.IntVar(&storageNodesPerAZ, storageNodesPerAZFlag, defaultStorageNodesPerAZ, "Maximum number of storage nodes per availability zone")
	flag.DurationVar(&destroyAppTimeout, "destroy-app-timeout", defaultTimeout, "Maximum ")
	flag.DurationVar(&driverStartTimeout, "driver-start-timeout", defaultTimeout, "Maximum wait volume driver startup")
	flag.DurationVar(&autoStorageNodeRecoveryTimeout, "storagenode-recovery-timeout", defaultAutoStorageNodeRecoveryTimeout, "Maximum wait time in minutes for storageless nodes to transition to storagenodes in case of ASG")
	flag.StringVar(&configMapName, configMapFlag, "", "Name of the config map to be used.")
	flag.StringVar(&bundleLocation, "bundle-location", defaultBundleLocation, "Path to support bundle output files")
	flag.StringVar(&customConfigPath, "custom-config", "", "Path to custom configuration files")
	flag.StringVar(&secretType, "secret-type", scheduler.SecretK8S, "Path to custom configuration files")
	flag.StringVar(&vaultAddress, "vault-addr", "", "Path to custom configuration files")
	flag.StringVar(&vaultToken, "vault-token", "", "Path to custom configuration files")
	flag.StringVar(&schedUpgradeHops, "sched-upgrade-hops", "", "Comma separated list of versions scheduler upgrade to take hops")
	flag.StringVar(&autopilotUpgradeImage, autopilotUpgradeImageCliFlag, "", "Autopilot version which will be used for checking version after upgrade autopilot")
	flag.StringVar(&csiGenericDriverConfigMapName, csiGenericDriverConfigMapFlag, "", "Name of config map that stores provisioner details when CSI generic driver is being used")
	flag.Parse()

	appList, err := splitCsv(appListCSV)
	if err != nil {
		logrus.Fatalf("failed to parse app list: %v. err: %v", appListCSV, err)
	}

	sched.Init(time.Second)

	if schedulerDriver, err = scheduler.Get(s); err != nil {
		logrus.Fatalf("Cannot find scheduler driver for %v. Err: %v\n", s, err)
	} else if volumeDriver, err = volume.Get(v); err != nil {
		logrus.Fatalf("Cannot find volume driver for %v. Err: %v\n", v, err)
	} else if nodeDriver, err = node.Get(n); err != nil {
		logrus.Fatalf("Cannot find node driver for %v. Err: %v\n", n, err)
	} else if err = os.MkdirAll(logLoc, os.ModeDir); err != nil {
		logrus.Fatalf("Cannot create path %s for saving support bundle. Error: %v", logLoc, err)
	} else {
		if _, err = os.Stat(customConfigPath); err == nil {
			var data []byte

			logrus.Infof("Using custom app config file %s", customConfigPath)
			data, err = ioutil.ReadFile(customConfigPath)
			if err != nil {
				logrus.Fatalf("Cannot read file %s. Error: %v", customConfigPath, err)
			}
			err = yaml.Unmarshal(data, &customAppConfig)
			if err != nil {
				logrus.Fatalf("Cannot unmarshal yml %s. Error: %v", customConfigPath, err)
			}
			logrus.Infof("Parsed custom app config file: %+v", customAppConfig)
		}
		logrus.Infof("Backup driver name %s", backupDriverName)
		if backupDriverName != "" {
			if backupDriver, err = backup.Get(backupDriverName); err != nil {
				logrus.Fatalf("cannot find backup driver for %s. Err: %v\n", backupDriverName, err)
			} else {
				logrus.Infof("Backup driver found %v", backupDriver)
			}
		}

		once.Do(func() {
			instance = &Torpedo{
				InstanceID:                          time.Now().Format("01-02-15h04m05s"),
				S:                                   schedulerDriver,
				V:                                   volumeDriver,
				N:                                   nodeDriver,
				SpecDir:                             specDir,
				LogLoc:                              logLoc,
				LogLevel:                            logLevel,
				ScaleFactor:                         appScaleFactor,
				MinRunTimeMins:                      minRunTimeMins,
				ChaosLevel:                          chaosLevel,
				StorageDriverUpgradeEndpointURL:     volUpgradeEndpointURL,
				StorageDriverUpgradeEndpointVersion: volUpgradeEndpointVersion,
				EnableStorkUpgrade:                  enableStorkUpgrade,
				AppList:                             appList,
				Provisioner:                         provisionerName,
				MaxStorageNodesPerAZ:                storageNodesPerAZ,
				DestroyAppTimeout:                   destroyAppTimeout,
				DriverStartTimeout:                  driverStartTimeout,
				AutoStorageNodeRecoveryTimeout:      autoStorageNodeRecoveryTimeout,
				ConfigMap:                           configMapName,
				BundleLocation:                      bundleLocation,
				CustomAppConfig:                     customAppConfig,
				Backup:                              backupDriver,
				SecretType:                          secretType,
				VaultAddress:                        vaultAddress,
				VaultToken:                          vaultToken,
				SchedUpgradeHops:                    schedUpgradeHops,
				AutopilotUpgradeImage:               autopilotUpgradeImage,
				CsiGenericDriverConfigMap:           csiGenericDriverConfigMapName,
			}
		})
	}

	// Set log level
	logLvl, err := logrus.ParseLevel(instance.LogLevel)
	if err != nil {
		logrus.Fatalf("Failed to set log level due to Err: %v", err)
	}
	logrus.SetLevel(logLvl)

}

func splitCsv(in string) ([]string, error) {
	r := csv.NewReader(strings.NewReader(in))
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil || len(records) < 1 {
		return []string{}, err
	} else if len(records) > 1 {
		return []string{}, fmt.Errorf("Multiline CSV not supported")
	}
	return records[0], err
}

func mapToVolumeOptions(options map[string]bool) *scheduler.VolumeOptions {
	if val, ok := options[SkipClusterScopedObjects]; ok {
		return &scheduler.VolumeOptions{
			SkipClusterScopedObjects: val,
		}
	}

	return &scheduler.VolumeOptions{
		SkipClusterScopedObjects: false,
	}
}

func init() {
	logrus.SetLevel(logrus.InfoLevel)
	logrus.StandardLogger().Hooks.Add(log.NewHook())
	logrus.SetOutput(os.Stdout)
}
