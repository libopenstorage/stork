package tests

import (
	"encoding/base64"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/pkg/sched"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	"github.com/sirupsen/logrus"
	appsapi "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storageapi "k8s.io/api/storage/v1"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/pkg/osutils"
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

	context1 "context"
	"github.com/pborman/uuid"
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
	licenseExpiryTimeoutHoursFlag        = "license_expiry_timeout_hours"
	meteringIntervalMinsFlag             = "metering_interval_mins"
	sourceClusterName                    = "source-cluster"
	destinationClusterName               = "destination-cluster"
	backupLocationName                   = "tp-blocation"
	backupScheduleNamePrefix             = "tp-bkp-schedule"
	backupScheduleScaleName              = "-scale"
	configMapName                        = "kubeconfigs"

	pxbackupDeploymentName             = "px-backup"
	pxbackupDeploymentNamespace        = "px-backup"
	pxbackupMongodbDeploymentName      = "pxc-backup-mongodb"
	pxbackupMongodbDeploymentNamespace = "px-backup"

	milestoneFlag               = "testrail-milestone"
	testrailRunNameFlag         = "testrail-run-name"
	testrailRunIDFlag           = "testrail-run-id"
	testrailJenkinsBuildURLFlag = "testrail-jeknins-build-url"
	testRailHostFlag            = "testrail-host"
	testRailUserNameFlag        = "testrail-username"
	testRailPasswordFlag        = "testrail-password"
)

// Backup constants
const (
	BackupNamePrefix                  = "tp-backup"
	BackupRestoreCompletionTimeoutMin = 20
	CredName                          = "tp-backup-cred"
	KubeconfigDirectory               = "/tmp"
	RetrySeconds                      = 10
	BackupScheduleAllName             = "-all"
	SchedulePolicyAllName             = "schedule-policy-all"
	SchedulePolicyScaleName           = "schedule-policy-scale"
	BucketNamePrefix                  = "tp-backup-bucket"
)

const (
	oneMegabytes                          = 1024 * 1024
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
	defaultLicenseExpiryTimeoutHours      = 1 * time.Hour
	defaultMeteringIntervalMins           = 10 * time.Minute
	authTokenParam                        = "auth-token"
)

const (
	waitResourceCleanup       = 2 * time.Minute
	defaultTimeout            = 5 * time.Minute
	defaultRetryInterval      = 10 * time.Second
	defaultCmdTimeout         = 20 * time.Second
	defaultCmdRetryInterval   = 5 * time.Second
	defaultDriverStartTimeout = 10 * time.Minute
)

var (
	errPureBlockNotSupported         = errors.New("pure_block pass through volume not supported")
	errPureFileSnapshotNotSupported  = errors.New("snapshot feature is not supported for pure_file volumes")
	errUnexpectedSizeChangeAfterFBIO = errors.New("the size change in bytes is not expected after write to FB volume")
)

var (
	context = ginkgo.Context
	fail    = ginkgo.Fail
	// Step is an alias for ginko "By" which represents a step in the spec
	Step          = ginkgo.By
	expect        = gomega.Expect
	haveOccurred  = gomega.HaveOccurred
	beEmpty       = gomega.BeEmpty
	beNil         = gomega.BeNil
	equal         = gomega.Equal
	contain       = gomega.ContainSubstring
	beTrue        = gomega.BeTrue
	beNumerically = gomega.BeNumerically
)

// Backup vars
var (
	// OrgID is pxbackup OrgID
	OrgID      string
	BucketName string
	// CloudCredUID is pxbackup cloud cred UID
	CloudCredUID string
	// BackupLocationUID is pxbackup backupLocation UID
	BackupLocationUID                    string
	BackupScheduleAllUID                 string
	SchedulePolicyAllUID                 string
	ScheduledBackupAllNamespacesInterval time.Duration
	BackupScheduleScaleUID               string
	SchedulePolicyScaleUID               string
	ScheduledBackupScaleInterval         time.Duration
	contextsCreated                      []*scheduler.Context
)

var (
	testRailHostname string
	testRailUsername string
	testRailPassword string
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
		SpecDir:                 Inst().SpecDir,
		VolDriverName:           Inst().V.String(),
		NodeDriverName:          Inst().N.String(),
		SecretConfigMapName:     Inst().ConfigMap,
		CustomAppConfig:         Inst().CustomAppConfig,
		StorageProvisioner:      Inst().Provisioner,
		SecretType:              Inst().SecretType,
		VaultAddress:            Inst().VaultAddress,
		VaultToken:              Inst().VaultToken,
		PureVolumes:             Inst().PureVolumes,
		HelmValuesConfigMapName: Inst().HelmValuesConfigMap,
	})
	expect(err).NotTo(haveOccurred())

	err = Inst().N.Init(node.InitOptions{
		SpecDir: Inst().SpecDir,
	})
	expect(err).NotTo(haveOccurred())

	err = Inst().V.Init(Inst().S.String(), Inst().N.String(), token, Inst().Provisioner, Inst().CsiGenericDriverConfigMap)
	expect(err).NotTo(haveOccurred())

	if Inst().Backup != nil {
		err = Inst().Backup.Init(Inst().S.String(), Inst().N.String(), Inst().V.String(), token)
		expect(err).NotTo(haveOccurred())
	}
	if testRailHostname != "" && testRailUsername != "" && testRailPassword != "" {
		err = testrailuttils.Init(testRailHostname, testRailUsername, testRailPassword)
		if err == nil {
			if testrailuttils.MilestoneName == "" || testrailuttils.RunName == "" || testrailuttils.JobRunID == "" {
				processError(fmt.Errorf("Not all details provided to update testrail"))
			}
			testrailuttils.CreateMilestone()
		}
	} else {
		logrus.Debugf("Not all information to connect to testrail is provided, skipping updates to testrail")
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
	// if errChan is provided then just push err to on channel
	// Useful for frameworks like longevity that must continue
	// execution and must not not fail immidiately
	if len(errChan) > 0 {
		logrus.Error(err)
		updateChannel(err, errChan...)
	} else {
		expect(err).NotTo(haveOccurred())
	}
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
		appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
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

// ValidateContextForPureVolumesSDK is the ginkgo spec for validating a scheduled context
func ValidateContextForPureVolumesSDK(ctx *scheduler.Context, errChan ...*chan error) {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	ginkgo.Describe(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {
		var timeout time.Duration
		appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
		if ctx.ReadinessTimeout == time.Duration(0) {
			timeout = appScaleFactor * defaultTimeout
		} else {
			timeout = appScaleFactor * ctx.ReadinessTimeout
		}
		Step(fmt.Sprintf("validate %s app's volumes", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateFBSnapshotsSDK(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's volumes resizing ", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateResizeFBPVC(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("wait for %s app to start running", ctx.App.Key), func() {
			err := Inst().S.WaitForRunning(ctx, timeout, defaultRetryInterval)
			processError(err, errChan...)
		})

		Step(fmt.Sprintf("validate %s app's volums statstics ", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateVolumeStatsticsDynamicUpdate(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's pure volumes has no replicaset", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidatePureVolumeNoReplicaSet(ctx, errChan...)
			}
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

// ValidateContextForPureVolumesPXCTL is the ginkgo spec for validating a scheduled context
func ValidateContextForPureVolumesPXCTL(ctx *scheduler.Context, errChan ...*chan error) {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	ginkgo.Describe(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {
		var timeout time.Duration
		appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
		if ctx.ReadinessTimeout == time.Duration(0) {
			timeout = appScaleFactor * defaultTimeout
		} else {
			timeout = appScaleFactor * ctx.ReadinessTimeout
		}

		Step(fmt.Sprintf("validate %s app's volumes for pxctl", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateFBSnapshotsPXCTL(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's volumes resizing ", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateResizeFBPVC(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("wait for %s app to start running", ctx.App.Key), func() {
			err := Inst().S.WaitForRunning(ctx, timeout, defaultRetryInterval)
			processError(err, errChan...)
		})

		Step(fmt.Sprintf("validate %s app's volums statstics ", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateVolumeStatsticsDynamicUpdate(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's pure volumes has no replicaset", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidatePureVolumeNoReplicaSet(ctx, errChan...)
			}
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
			appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
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
				params[authTokenParam], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
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

// ValidateFBSnapshotsSDK is the ginkgo spec for validating FB volume snapshots using API for a context
func ValidateFBSnapshotsSDK(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of an app's volumes", func() {
		var err error
		Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
			appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
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
				params[authTokenParam], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
				processError(err, errChan...)
			}
			if ctx.RefreshStorageEndpoint {
				params["refresh-endpoint"] = "true"
			}
			Step(fmt.Sprintf("get %s app's volume: %s inspected by the volume driver", ctx.App.Key, vol), func() {
				err = Inst().V.ValidateCreateVolume(vol, params)
				processError(err, errChan...)
			})
			Step(fmt.Sprintf("get %s app's volume: %s then create local snapshot", ctx.App.Key, vol), func() {
				err = Inst().V.ValidateCreateSnapshot(vol, params)
				if err != nil {
					expect(err.Error()).To(contain(errPureFileSnapshotNotSupported.Error()))
				}
			})
			Step(fmt.Sprintf("get %s app's volume: %s then create cloudsnap", ctx.App.Key, vol), func() {
				err = Inst().V.ValidateCreateCloudsnap(vol, params)
				if err != nil {
					expect(err.Error()).To(contain(errPureFileSnapshotNotSupported.Error()))
				}
			})
		}
	})
}

// ValidateFBSnapshotsPXCTL is the ginkgo spec for validating FB volume snapshots using PXCTL for a context
func ValidateFBSnapshotsPXCTL(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of an app's volumes", func() {
		var err error
		Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
			appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
			err = Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval, nil)
			processError(err, errChan...)
		})

		var vols []*volume.Volume
		Step(fmt.Sprintf("get %s app's pure volumes", ctx.App.Key), func() {
			vols, err = Inst().S.GetPureVolumes(ctx)
			processError(err, errChan...)
		})

		for _, vol := range vols {

			Step(fmt.Sprintf("get %s app's volume: %s then create snapshot using pxctl", ctx.App.Key, vol), func() {
				err = Inst().V.ValidateCreateSnapshotUsingPxctl(vol.ID)
				if err != nil {
					expect(err.Error()).To(contain(errPureFileSnapshotNotSupported.Error()))
				}
			})
			Step(fmt.Sprintf("get %s app's volume: %s then create cloudsnap using pxctl", ctx.App.Key, vol), func() {
				err = Inst().V.ValidateCreateCloudsnapUsingPxctl(vol.ID)
				if err != nil {
					expect(err.Error()).To(contain(errPureFileSnapshotNotSupported.Error()))
				}
			})
		}
		Step(fmt.Sprintf("validating groupsnap for using pxctl"), func() {
			err = Inst().V.ValidateCreateGroupSnapshotUsingPxctl()
			if err != nil {
				expect(err.Error()).To(contain(errPureFileSnapshotNotSupported.Error()))
			}
		})
	})
}

// ValidateResizeFBPVC is the ginkgo spec for validating resize of volumes
func ValidateResizeFBPVC(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of an resizing pvc", func() {
		var err error
		Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
			appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
			err = Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval, nil)
			processError(err, errChan...)
		})

		Step(fmt.Sprintf("validating resizing pvcs"), func() {
			err = Inst().S.ResizePureVolumes(ctx)
			if err != nil {
				expect(err).ToNot(haveOccurred())
			}
		})
	})
}

// ValidatePureVolumeNoReplicaSet is the ginko spec for validating empty replicaset for pure volumes
func ValidatePureVolumeNoReplicaSet(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of an resizing pvc", func() {
		var err error
		Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
			appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
			err = Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval, nil)
			processError(err, errChan...)
		})
		var vols []*volume.Volume
		Step(fmt.Sprintf("get %s app's pure volumes", ctx.App.Key), func() {
			vols, err = Inst().S.GetPureVolumes(ctx)
			processError(err, errChan...)
		})

		err = Inst().V.ValidatePureVolumesNoReplicaSets(vols[0].ID, make(map[string]string))
		expect(err).NotTo(haveOccurred())

	})
}

// ValidateVolumeStatsticsDynamicUpdate is the ginkgo spec for validating dynamic update of byteUsed statstic for pure volumes
func ValidateVolumeStatsticsDynamicUpdate(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of an resizing pvc", func() {
		var err error
		Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
			appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
			err = Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval, nil)
			processError(err, errChan...)
		})
		var vols []*volume.Volume
		Step(fmt.Sprintf("get %s app's pure volumes", ctx.App.Key), func() {
			vols, err = Inst().S.GetPureVolumes(ctx)
			processError(err, errChan...)
		})
		byteUsedInitial, err := Inst().V.ValidateGetByteUsedForVolume(vols[0].ID, make(map[string]string))
		fmt.Println(fmt.Sprintf("initially the byteUsed is %v", byteUsedInitial))
		// get the pod for this pvc
		pods, err := Inst().S.GetPodsForPVC(vols[0].Name, vols[0].Namespace)

		// write to the FB volume
		cmdArgs := []string{"exec", "-it", pods[0].Name, "-n", pods[0].Namespace, "--", "bash", "-c", "dd bs=512 count=1048576 </dev/urandom > " + getMountPath(pods[0].Namespace) + "/myfile"}
		err = osutils.Kubectl(cmdArgs)
		processError(err, errChan...)
		// wait until the backends size is reflected before making the REST call
		time.Sleep(125 * time.Second)

		byteUsedafter, err := Inst().V.ValidateGetByteUsedForVolume(vols[0].ID, make(map[string]string))
		fmt.Println(fmt.Sprintf("after writing random bytes to the file the byteUsed is %v", byteUsedafter))
		err = fbVolumeExpectedSizechange(byteUsedafter - byteUsedInitial)
		expect(err).NotTo(haveOccurred())

	})
}

func fbVolumeExpectedSizechange(sizeChangeInBytes uint64) error {
	if sizeChangeInBytes < (512-30)*oneMegabytes || sizeChangeInBytes > (512+30)*oneMegabytes {
		return errUnexpectedSizeChangeAfterFBIO
	}
	return nil
}

// getMountPath checkts the podname prefix, and finds the mountpath.
//	unfortunately, the mountpath for PVCs all vary among applications, so Im getting the hardcoded value here
//	maybe there will be some improvement later
func getMountPath(namespace string) string {
	if strings.HasPrefix(namespace, "nginx-without-enc") {
		return "/usr/share/nginx/html"
	} else if strings.HasPrefix(namespace, "wordpress") {
		return "/var/www/html"
	} else {
		return ""
	}
}

// GetVolumeParameters returns volume parameters for all volumes for given context
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
		if _, ok := param[k8s.PvcNameKey]; ok {
			if _, ok := param[k8s.PvcNamespaceKey]; ok {
				pvcName, pvcNamespace := param[k8s.PvcNameKey], param[k8s.PvcNamespaceKey]
				PVName, err := Inst().S.GetVolumeDriverVolumeName(pvcName, pvcNamespace)
				expect(err).NotTo(haveOccurred())
				updatedVolumeParam[PVName] = param
			}
		}

	}
	return updatedVolumeParam
}

// ValidateVolumeParameters validates volume parameters using volume driver
func ValidateVolumeParameters(volParam map[string]map[string]string) {
	var err error
	for vol, params := range volParam {
		if Inst().ConfigMap != "" {
			params[authTokenParam], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
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
				appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
				volOpts := mapToVolumeOptions(volOptsMap)
				err := Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval, volOpts)
				expect(err).NotTo(haveOccurred())
			})

			Step(fmt.Sprintf("wait for %s app to start running", ctx.App.Key), func() {
				appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
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
func ScheduleApplications(testname string, errChan ...*chan error) []*scheduler.Context {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	var contexts []*scheduler.Context
	var err error

	Step("schedule applications", func() {
		taskName := fmt.Sprintf("%s-%v", testname, Inst().InstanceID)
		contexts, err = Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
			AppKeys:            Inst().AppList,
			StorageProvisioner: Inst().Provisioner,
		})
		processError(err, errChan...)
		if len(contexts) == 0 {
			processError(fmt.Errorf("list of contexts is empty for [%s]", taskName), errChan...)
		}
	})

	return contexts
}

// ValidateApplicationsPurePxctl validates applications
func ValidateApplicationsPurePxctl(contexts []*scheduler.Context) {
	Step("validate applications", func() {
		for _, ctx := range contexts {
			ValidateContextForPureVolumesPXCTL(ctx)
		}
	})
}

// ValidateApplicationsPureSDK validates applications
func ValidateApplicationsPureSDK(contexts []*scheduler.Context) {
	Step("validate applications", func() {
		for _, ctx := range contexts {
			ValidateContextForPureVolumesSDK(ctx)
		}
	})
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
func StartVolDriverAndWait(appNodes []node.Node, errChan ...*chan error) {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	context(fmt.Sprintf("starting volume driver %s", Inst().V.String()), func() {
		Step(fmt.Sprintf("start volume driver on nodes: %v", appNodes), func() {
			for _, n := range appNodes {
				err := Inst().V.StartDriver(n)
				processError(err, errChan...)
			}
		})

		Step(fmt.Sprintf("wait for volume driver to start on nodes: %v", appNodes), func() {
			for _, n := range appNodes {
				err := Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
				processError(err, errChan...)
			}
		})

	})
}

// StopVolDriverAndWait stops volume driver on given app nodes and waits till driver is down
func StopVolDriverAndWait(appNodes []node.Node, errChan ...*chan error) {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	context(fmt.Sprintf("stopping volume driver %s", Inst().V.String()), func() {
		Step(fmt.Sprintf("stop volume driver on nodes: %v", appNodes), func() {
			err := Inst().V.StopDriver(appNodes, false, nil)
			processError(err, errChan...)
		})

		Step(fmt.Sprintf("wait for volume driver to stop on nodes: %v", appNodes), func() {
			for _, n := range appNodes {
				err := Inst().V.WaitDriverDownOnNode(n)
				processError(err, errChan...)
			}
		})

	})
}

// CrashVolDriverAndWait crashes volume driver on given app nodes and waits till driver is back up
func CrashVolDriverAndWait(appNodes []node.Node, errChan ...*chan error) {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	context(fmt.Sprintf("crashing volume driver %s", Inst().V.String()), func() {
		Step(fmt.Sprintf("crash volume driver on nodes: %v", appNodes), func() {
			err := Inst().V.StopDriver(appNodes, true, nil)
			processError(err, errChan...)
		})

		Step(fmt.Sprintf("wait for volume driver to start on nodes: %v", appNodes), func() {
			for _, n := range appNodes {
				err := Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
				processError(err, errChan...)
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
	expect(err).NotTo(haveOccurred())
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

				// Moves this out to deal with diag testing.
				r := &volume.DiagRequestConfig{
					DockerHost:    "unix:///var/run/docker.sock",
					OutputFile:    fmt.Sprintf("/var/cores/diags-%s-%d.tar.gz", n.Name, time.Now().Unix()),
					ContainerName: "",
					Profile:       false,
					Live:          true,
					Upload:        false,
					All:           true,
					Force:         true,
					OnHost:        true,
					Extra:         false,
				}

				Inst().V.CollectDiags(n, r, volume.DiagOps{})

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

// DeleteCloudCredential deletes cloud credentials
func DeleteCloudCredential(name string, orgID string, cloudCredUID string) {
	Step(fmt.Sprintf("Delete cloud credential [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup

		credDeleteRequest := &api.CloudCredentialDeleteRequest{
			Name:  name,
			OrgId: orgID,
			Uid:   cloudCredUID,
		}
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))
		backupDriver.DeleteCloudCredential(ctx, credDeleteRequest)
		// Best effort cleanup, dont fail test, if deletion fails
		// expect(err).NotTo(haveOccurred(),
		//  fmt.Sprintf("Failed to delete cloud credential [%s] in org [%s]", name, orgID))
		// TODO: validate CreateCloudCredentialResponse also
	})
}

// ValidateVolumeParametersGetErr validates volume parameters using volume driver and returns err instead of failing
func ValidateVolumeParametersGetErr(volParam map[string]map[string]string) error {
	var err error
	for vol, params := range volParam {
		if Inst().ConfigMap != "" {
			params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
			expect(err).NotTo(haveOccurred())
		}
		Step(fmt.Sprintf("get volume: %s inspected by the volume driver", vol), func() {
			err = Inst().V.ValidateCreateVolume(vol, params)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// AfterEachTest runs collect support bundle after each test when it fails
func AfterEachTest(contexts []*scheduler.Context, ids ...int) {
	testStatus := "Pass"
	logrus.Debugf("contexts: %v", contexts)
	if ginkgo.CurrentGinkgoTestDescription().Failed {
		CollectSupport()
		DescribeNamespace(contexts)
		testStatus = "Fail"
	}
	if len(ids) >= 1 {
		driverVersion, err := Inst().V.GetDriverVersion()
		if err != nil {
			logrus.Errorf("Error in getting driver version")
		}
		testrailObject := testrailuttils.Testrail{
			Status:        testStatus,
			TestID:        ids[0],
			RunID:         ids[1],
			DriverVersion: driverVersion,
		}
		testrailuttils.AddTestEntry(testrailObject)
	}
}

// SetClusterContext sets context to clusterConfigPath
func SetClusterContext(clusterConfigPath string) {
	err := Inst().S.SetConfig(clusterConfigPath)
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to switch to context. Error: [%v]", err))

	err = Inst().S.RefreshNodeRegistry()
	expect(err).NotTo(haveOccurred())

	err = Inst().V.RefreshDriverEndpoints()
	expect(err).NotTo(haveOccurred())
}

// ValidateRestoredApplicationsGetErr validates applications restored by backup driver and updates errors instead of failing the test
func ValidateRestoredApplicationsGetErr(contexts []*scheduler.Context, volumeParameters map[string]map[string]string, bkpErrors map[string]error) {
	var updatedVolumeParams map[string]map[string]string
	volOptsMap := make(map[string]bool)
	volOptsMap[SkipClusterScopedObjects] = true

	var wg sync.WaitGroup
	for _, ctx := range contexts {
		wg.Add(1)
		go func(wg *sync.WaitGroup, ctx *scheduler.Context) {
			defer wg.Done()
			namespace := ctx.App.SpecList[0].(*v1.PersistentVolumeClaim).Namespace
			if err, ok := bkpErrors[namespace]; ok {
				logrus.Infof("Skipping validating namespace %s because %s", namespace, err)
			} else {
				ginkgo.Describe(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {

					Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
						appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
						volOpts := mapToVolumeOptions(volOptsMap)
						err = Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval, volOpts)
					})
					if err != nil {
						bkpErrors[namespace] = err
						logrus.Errorf("Failed to validate [%s] app. Error: [%v]", ctx.App.Key, err)
						return
					}

					Step(fmt.Sprintf("wait for %s app to start running", ctx.App.Key), func() {
						appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
						err = Inst().S.WaitForRunning(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval)
					})
					if err != nil {
						bkpErrors[namespace] = err
						logrus.Errorf("Failed to validate [%s] app. Error: [%v]", ctx.App.Key, err)
						return
					}

					updatedVolumeParams = UpdateVolumeInVolumeParameters(volumeParameters)
					logrus.Infof("Updated parameter list: [%+v]\n", updatedVolumeParams)
					err = ValidateVolumeParametersGetErr(updatedVolumeParams)
					if err != nil {
						bkpErrors[namespace] = err
						logrus.Errorf("Failed to validate [%s] app. Error: [%v]", ctx.App.Key, err)
						return
					}

					Step(fmt.Sprintf("validate if %s app's volumes are setup", ctx.App.Key), func() {
						var vols []*volume.Volume
						vols, err = Inst().S.GetVolumes(ctx)
						logrus.Infof("List of volumes from scheduler driver :[%+v] \n for context : [%+v]\n", vols, ctx)
						if err != nil {
							bkpErrors[namespace] = err
							logrus.Errorf("Failed to validate [%s] app. Error: [%v]", ctx.App.Key, err)
						}

						for _, vol := range vols {
							Step(fmt.Sprintf("validate if %s app's volume: %v is setup", ctx.App.Key, vol), func() {
								err = Inst().V.ValidateVolumeSetup(vol)
								if err != nil {
									bkpErrors[namespace] = err
									logrus.Errorf("Failed to validate [%s] app. Error: [%v]", ctx.App.Key, err)
								}
							})
						}
					})
				})
			}
		}(&wg, ctx)
	}
	wg.Wait()
}

// CreateBackupGetErr creates backup without ending the test if it errors
func CreateBackupGetErr(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string) (err error) {

	Step(fmt.Sprintf("Create backup [%s] in org [%s] from cluster [%s]",
		backupName, orgID, clusterName), func() {

		backupDriver := Inst().Backup
		bkpCreateRequest := &api.BackupCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  backupName,
				OrgId: orgID,
			},
			BackupLocationRef: &api.ObjectRef{
				Name: bLocation,
				Uid:  bLocationUID,
			},
			Cluster:        sourceClusterName,
			Namespaces:     namespaces,
			LabelSelectors: labelSelectors,
		}
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))
		_, err = backupDriver.CreateBackup(ctx, bkpCreateRequest)
		if err != nil {
			logrus.Errorf("Failed to create backup [%s] in org [%s]. Error: [%v]",
				backupName, orgID, err)
		}
	})

	return err
}

// CreateScheduledBackup creates a scheduled backup with time interval
func CreateScheduledBackup(backupScheduleName, backupScheduleUID, schedulePolicyName, schedulePolicyUID string,
	interval time.Duration, namespaces []string) (err error) {
	var ctx context1.Context
	labelSelectors := make(map[string]string)
	Step(fmt.Sprintf("Create scheduled backup %s of namespaces %v on cluster %s in organization %s",
		backupScheduleNamePrefix+backupScheduleName, namespaces, sourceClusterName, OrgID), func() {
		backupDriver := Inst().Backup

		// Create a schedule policy
		schedulePolicyCreateRequest := &api.SchedulePolicyCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  schedulePolicyName,
				Uid:   schedulePolicyUID,
				OrgId: OrgID,
			},

			SchedulePolicy: &api.SchedulePolicyInfo{
				Interval: &api.SchedulePolicyInfo_IntervalPolicy{
					// Retain 5 backups at a time for ease of inspection
					Retain:  5,
					Minutes: int64(interval / time.Minute),
					IncrementalCount: &api.SchedulePolicyInfo_IncrementalCount{
						Count: 0,
					},
				},
			},
		}
		//ctx, err = backup.GetPxCentralAdminCtx()
		ctx, err = backup.GetAdminCtxFromSecret()
		if err != nil {
			return
		}
		_, err = backupDriver.CreateSchedulePolicy(ctx, schedulePolicyCreateRequest)
		if err != nil {
			return
		}

		// Create a backup schedule
		bkpScheduleCreateRequest := &api.BackupScheduleCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  backupScheduleNamePrefix + backupScheduleName,
				Uid:   backupScheduleUID,
				OrgId: OrgID,
			},

			Namespaces: namespaces,

			ReclaimPolicy: api.BackupScheduleInfo_Delete,
			// Name of Cluster
			Cluster: sourceClusterName,
			// Label selectors to choose resources
			LabelSelectors: labelSelectors,

			SchedulePolicyRef: &api.ObjectRef{
				Name: schedulePolicyName,
				Uid:  schedulePolicyUID,
			},
			BackupLocationRef: &api.ObjectRef{
				Name: backupLocationName,
				Uid:  BackupLocationUID,
			},
		}
		//ctx, err = backup.GetPxCentralAdminCtx()
		ctx, err = backup.GetAdminCtxFromSecret()
		if err != nil {
			return
		}
		_, err = backupDriver.CreateBackupSchedule(ctx, bkpScheduleCreateRequest)
		if err != nil {
			return
		}
	})
	return err
}

// DeleteNamespace tears down the last nginx app
func DeleteNamespace() error {
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	if err != nil {
		return err
	}
	SetClusterContext(sourceClusterConfigPath)
	if len(contextsCreated) == 0 {
		logrus.Infof("No namespace to delete")
		return nil
	}
	TearDownContext(contextsCreated[0], map[string]bool{
		SkipClusterScopedObjects:                    true,
		scheduler.OptionsWaitForResourceLeakCleanup: true,
		scheduler.OptionsWaitForDestroy:             true,
	})
	contextsCreated = contextsCreated[1:]

	SetClusterContext(sourceClusterConfigPath)
	newNamespaceCounter++

	return nil
}

// CreateNamespace creates a new nginx app
func CreateNamespace(appKeys []string) error {
	volumeParams := make(map[string]map[string]string)
	taskName := fmt.Sprintf("new-%s-%d", Inst().InstanceID, newNamespaceCounter)
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	if err != nil {
		return err
	}
	SetClusterContext(sourceClusterConfigPath)

	contexts, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
		AppKeys:            appKeys,
		StorageProvisioner: Inst().Provisioner,
	})
	if err != nil {
		return err
	}
	// Skip volume validation until other volume providers are implemented.
	for _, ctx := range contexts {
		ctx.SkipVolumeValidation = true
	}

	ValidateApplications(contexts)
	for _, ctx := range contexts {
		for vol, params := range GetVolumeParameters(ctx) {
			volumeParams[vol] = params
		}
	}

	SetClusterContext(sourceClusterConfigPath)
	contextsCreated = append(contextsCreated, contexts...)
	newNamespaceCounter++

	return nil
}

// ObjectExists returns whether err is from an object not being found by a backup api call
func ObjectExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "object not found")
}

//GetBackupCreateRequest returns a backupcreaterequest
func GetBackupCreateRequest(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string) *api.BackupCreateRequest {
	return &api.BackupCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  backupName,
			OrgId: orgID,
		},
		BackupLocationRef: &api.ObjectRef{
			Name: bLocation,
			Uid:  bLocationUID,
		},
		Cluster:        clusterName,
		Namespaces:     namespaces,
		LabelSelectors: labelSelectors,
	}
}

//CreateBackupFromRequest creates a backup using a provided request
func CreateBackupFromRequest(backupName string, orgID string, request *api.BackupCreateRequest) (err error) {
	//ctx, err := backup.GetPxCentralAdminCtx()
	ctx, err := backup.GetAdminCtxFromSecret()
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]", err))
	backupDriver := Inst().Backup
	_, err = backupDriver.CreateBackup(ctx, request)
	if err != nil {
		logrus.Errorf("Failed to create backup [%s] in org [%s]. Error: [%v]",
			backupName, orgID, err)
	}
	return err
}

// InspectBackup inspects the backup name passed in
func InspectBackup(backupName string) (bkpInspectResponse *api.BackupInspectResponse, err error) {
	var ctx context1.Context

	Step(fmt.Sprintf("Inspect backup %s in org %s",
		backupName, OrgID), func() {
		backupDriver := Inst().Backup

		bkpInspectRequest := &api.BackupInspectRequest{
			OrgId: OrgID,
			Name:  backupName,
		}
		//ctx, err = backup.GetPxCentralAdminCtx()
		ctx, err = backup.GetAdminCtxFromSecret()
		if err != nil {
			return
		}
		bkpInspectResponse, err = backupDriver.InspectBackup(ctx, bkpInspectRequest)
		if err != nil {
			return
		}
	})
	return bkpInspectResponse, err
}

// WaitForScheduledBackup waits until a new backup is taken from scheduled backup with UID backupScheduleUID
func WaitForScheduledBackup(backupScheduleName string, retryInterval time.Duration, timeout time.Duration) (*api.BackupObject, error) {
	beginTime := time.Now()
	beginTimeSec := beginTime.Unix()

	t := func() (interface{}, bool, error) {
		logrus.Infof("Enumerating backups")
		bkpEnumerateReq := &api.BackupEnumerateRequest{
			OrgId: OrgID}
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		if err != nil {
			return nil, true, err
		}
		curBackups, err := Inst().Backup.EnumerateBackup(ctx, bkpEnumerateReq)
		if err != nil {
			return nil, true, err
		}
		for _, bkp := range curBackups.GetBackups() {
			createTime := bkp.GetCreateTime()
			if beginTimeSec > createTime.GetSeconds() {
				break
			}
			if (bkp.GetStatus().GetStatus() == api.BackupInfo_StatusInfo_Success ||
				bkp.GetStatus().GetStatus() == api.BackupInfo_StatusInfo_PartialSuccess) &&
				bkp.GetBackupSchedule().GetName() == backupScheduleName {
				return bkp, false, nil
			}
		}
		err = fmt.Errorf("unable to find backup from backup schedule with name %s after time %v",
			backupScheduleName, beginTime)
		return nil, true, err
	}

	bkpInterface, err := task.DoRetryWithTimeout(t, timeout, retryInterval)
	if err != nil {
		return nil, err
	}
	bkp := bkpInterface.(*api.BackupObject)
	return bkp, nil

}

// InspectScheduledBackup inspects the scheduled backup
func InspectScheduledBackup(backupScheduleName, backupScheduleUID string) (bkpScheduleInspectResponse *api.BackupScheduleInspectResponse, err error) {
	var ctx context1.Context

	Step(fmt.Sprintf("Inspect scheduled backup %s of all namespaces on cluster %s in organization %s",
		backupScheduleNamePrefix, sourceClusterName, OrgID), func() {
		backupDriver := Inst().Backup

		bkpScheduleInspectRequest := &api.BackupScheduleInspectRequest{
			OrgId: OrgID,
			Name:  backupScheduleNamePrefix + backupScheduleName,
			Uid:   backupScheduleUID,
		}
		//ctx, err = backup.GetPxCentralAdminCtx()
		ctx, err = backup.GetAdminCtxFromSecret()
		if err != nil {
			return
		}
		bkpScheduleInspectResponse, err = backupDriver.InspectBackupSchedule(ctx, bkpScheduleInspectRequest)
		if err != nil {
			return
		}
	})
	return bkpScheduleInspectResponse, err
}

//DeleteLabelFromResource deletes a label by key from some resource and doesn't error if something doesn't exist
func DeleteLabelFromResource(spec interface{}, key string) {
	if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
		if obj.Labels != nil {
			_, ok := obj.Labels[key]
			if ok {
				logrus.Infof("Deleting label with key [%s] from PVC %s", key, obj.Name)
				delete(obj.Labels, key)
				core.Instance().UpdatePersistentVolumeClaim(obj)
			}
		}
	} else if obj, ok := spec.(*v1.ConfigMap); ok {
		if obj.Labels != nil {
			_, ok := obj.Labels[key]
			if ok {
				logrus.Infof("Deleting label with key [%s] from ConfigMap %s", key, obj.Name)
				delete(obj.Labels, key)
				core.Instance().UpdateConfigMap(obj)
			}
		}
	} else if obj, ok := spec.(*v1.Secret); ok {
		if obj.Labels != nil {
			_, ok := obj.Labels[key]
			if ok {
				logrus.Infof("Deleting label with key [%s] from Secret %s", key, obj.Name)
				delete(obj.Labels, key)
				core.Instance().UpdateSecret(obj)
			}
		}
	}
}

// DeleteBackupAndDependencies deletes backup and dependent backups
func DeleteBackupAndDependencies(backupName string, orgID string, clusterName string) error {
	//ctx, err := backup.GetPxCentralAdminCtx()
	ctx, err := backup.GetAdminCtxFromSecret()

	backupDeleteRequest := &api.BackupDeleteRequest{
		Name:    backupName,
		OrgId:   orgID,
		Cluster: clusterName,
	}
	_, err = Inst().Backup.DeleteBackup(ctx, backupDeleteRequest)
	if err != nil {
		return err
	}

	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
	}
	resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return err
	}

	backupDelStatus := resp.GetBackup().GetStatus()
	if backupDelStatus.GetStatus() == api.BackupInfo_StatusInfo_DeletePending {
		reason := strings.Split(backupDelStatus.GetReason(), ": ")
		dependency := reason[len(reason)-1]
		err = DeleteBackupAndDependencies(dependency, orgID, clusterName)
		if err != nil {
			return err
		}
	}

	err = Inst().Backup.WaitForBackupDeletion(ctx, backupName, orgID, defaultTimeout, defaultRetryInterval)
	if err != nil {
		return err
	}

	return nil
}

// DeleteRestore creates restore
func DeleteRestore(restoreName string, orgID string) {

	Step(fmt.Sprintf("Delete restore [%s] in org [%s]",
		restoreName, orgID), func() {

		backupDriver := Inst().Backup
		expect(backupDriver).NotTo(beNil(),
			"Backup driver is not initialized")

		deleteRestoreReq := &api.RestoreDeleteRequest{
			OrgId: orgID,
			Name:  restoreName,
		}
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))
		_, err = backupDriver.DeleteRestore(ctx, deleteRestoreReq)
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to delete restore [%s] in org [%s]. Error: [%v]",
				restoreName, orgID, err))
		// TODO: validate createClusterResponse also
	})
}

// SetupBackup sets up backup location and source and destination clusters
func SetupBackup(testName string) {
	logrus.Infof("Backup driver: %v", Inst().Backup)
	provider := GetProvider()
	logrus.Infof("Run Setup backup with object store provider: %s", provider)
	OrgID = "default"
	BucketName = fmt.Sprintf("%s-%s", BucketNamePrefix, Inst().InstanceID)
	CloudCredUID = uuid.New()
	//cloudCredUID = "5a48be84-4f63-40ae-b7f1-4e4039ab7477"
	BackupLocationUID = uuid.New()
	//backupLocationUID = "64d908e7-40cf-4c9e-a5cf-672e955fd0ca"

	CreateBucket(provider, BucketName)
	CreateOrganization(OrgID)
	CreateCloudCredential(provider, CredName, CloudCredUID, OrgID)
	CreateBackupLocation(provider, backupLocationName, BackupLocationUID, CredName, CloudCredUID, BucketName, OrgID)
	CreateSourceAndDestClusters(CredName, OrgID)
}

// DeleteBackup deletes backup
func DeleteBackup(backupName string, orgID string) {

	Step(fmt.Sprintf("Delete backup [%s] in org [%s]",
		backupName, orgID), func() {

		backupDriver := Inst().Backup
		bkpDeleteRequest := &api.BackupDeleteRequest{
			Name:  backupName,
			OrgId: orgID,
		}
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))
		backupDriver.DeleteBackup(ctx, bkpDeleteRequest)
		// Best effort cleanup, dont fail test, if deletion fails
		//expect(err).NotTo(haveOccurred(),
		//	fmt.Sprintf("Failed to delete backup [%s] in org [%s]", backupName, orgID))
		// TODO: validate createClusterResponse also
	})
}

// DeleteCluster deletes/de-registers cluster from px-backup
func DeleteCluster(name string, orgID string) {

	Step(fmt.Sprintf("Delete cluster [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup
		clusterDeleteReq := &api.ClusterDeleteRequest{
			OrgId: orgID,
			Name:  name,
		}
		ctx, err := backup.GetPxCentralAdminCtx()
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))
		backupDriver.DeleteCluster(ctx, clusterDeleteReq)
		// Best effort cleanup, dont fail test, if deletion fails
		//expect(err).NotTo(haveOccurred(),
		//	fmt.Sprintf("Failed to delete cluster [%s] in org [%s]", name, orgID))
	})
}

// DeleteBackupLocation deletes backuplocation
func DeleteBackupLocation(name string, orgID string) {
	Step(fmt.Sprintf("Delete backup location [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup
		bLocationDeleteReq := &api.BackupLocationDeleteRequest{
			Name:  name,
			OrgId: orgID,
		}
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))
		backupDriver.DeleteBackupLocation(ctx, bLocationDeleteReq)
		// Best effort cleanup, dont fail test, if deletion fails
		//expect(err).NotTo(haveOccurred(),
		//	fmt.Sprintf("Failed to delete backup location [%s] in org [%s]", name, orgID))
		// TODO: validate createBackupLocationResponse also
	})
}

// CreateSourceAndDestClusters creates source and destination cluster
// 1st cluster in KUBECONFIGS ENV var is source cluster while
// 2nd cluster is destination cluster
func CreateSourceAndDestClusters(cloudCred, orgID string) {
	// TODO: Add support for adding multiple clusters from
	// comma separated list of kubeconfig files
	kubeconfigs := os.Getenv("KUBECONFIGS")
	expect(kubeconfigs).NotTo(equal(""),
		"KUBECONFIGS Environment variable should not be empty")

	kubeconfigList := strings.Split(kubeconfigs, ",")
	// Validate user has provided at least 2 kubeconfigs for source and destination cluster
	expect(len(kubeconfigList)).Should(beNumerically(">=", 2), "At least minimum two kubeconfigs required")

	err := dumpKubeConfigs(configMapName, kubeconfigList)
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to get kubeconfigs [%v] from configmap [%s]", kubeconfigList, configMapName))

	// Register source cluster with backup driver
	Step(fmt.Sprintf("Create cluster [%s] in org [%s]", sourceClusterName, orgID), func() {
		srcClusterConfigPath, err := GetSourceClusterConfigPath()
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		logrus.Debugf("Save cluster %s kubeconfig to %s", sourceClusterName, srcClusterConfigPath)
		CreateCluster(sourceClusterName, cloudCred, srcClusterConfigPath, orgID)
	})

	// Register destination cluster with backup driver
	Step(fmt.Sprintf("Create cluster [%s] in org [%s]", destinationClusterName, orgID), func() {
		dstClusterConfigPath, err := GetDestinationClusterConfigPath()
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))
		logrus.Debugf("Save cluster %s kubeconfig to %s", destinationClusterName, dstClusterConfigPath)
		CreateCluster(destinationClusterName, cloudCred, dstClusterConfigPath, orgID)
	})
}

// CreateBackupLocation creates backup location
func CreateBackupLocation(provider, name, uid, credName, credUID, bucketName, orgID string) {
	switch provider {
	case drivers.ProviderAws:
		createS3BackupLocation(name, uid, credName, credUID, bucketName, orgID)
	case drivers.ProviderAzure:
		createAzureBackupLocation(name, uid, credName, CloudCredUID, bucketName, orgID)
	}
}

// CreateCluster creates/registers cluster with px-backup
func CreateCluster(name string, cloudCred string, kubeconfigPath string, orgID string) {

	Step(fmt.Sprintf("Create cluster [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup
		kubeconfigRaw, err := ioutil.ReadFile(kubeconfigPath)
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to read kubeconfig file from location [%s]. Error:[%v]",
				kubeconfigPath, err))

		clusterCreateReq := &api.ClusterCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  name,
				OrgId: orgID,
			},
			Kubeconfig:      base64.StdEncoding.EncodeToString(kubeconfigRaw),
			CloudCredential: cloudCred,
		}
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))
		_, err = backupDriver.CreateCluster(ctx, clusterCreateReq)
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to create cluster [%s] in org [%s]. Error : [%v]",
				name, orgID, err))
	})
}

// createS3BackupLocation creates backup location
func createS3BackupLocation(name string, uid, cloudCred string, cloudCredUID, bucketName string, orgID string) {
	Step(fmt.Sprintf("Create S3 backup location [%s] in org [%s]", name, orgID), func() {
		CreateS3BackupLocation(name, uid, cloudCred, cloudCredUID, bucketName, orgID)
	})
}

// CreateCloudCredential creates cloud credetials
func CreateCloudCredential(provider, name string, uid, orgID string) {
	Step(fmt.Sprintf("Create cloud credential [%s] in org [%s]", name, orgID), func() {
		logrus.Printf("Create credential name %s for org %s provider %s", name, orgID, provider)
		backupDriver := Inst().Backup
		switch provider {
		case drivers.ProviderAws:
			logrus.Infof("Create creds for aws")
			id := os.Getenv("AWS_ACCESS_KEY_ID")
			expect(id).NotTo(equal(""),
				"AWS_ACCESS_KEY_ID Environment variable should not be empty")

			secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
			expect(secret).NotTo(equal(""),
				"AWS_SECRET_ACCESS_KEY Environment variable should not be empty")

			credCreateRequest := &api.CloudCredentialCreateRequest{
				CreateMetadata: &api.CreateMetadata{
					Name:  name,
					Uid:   uid,
					OrgId: orgID,
				},
				CloudCredential: &api.CloudCredentialInfo{
					Type: api.CloudCredentialInfo_AWS,
					Config: &api.CloudCredentialInfo_AwsConfig{
						AwsConfig: &api.AWSConfig{
							AccessKey: id,
							SecretKey: secret,
						},
					},
				},
			}
			//ctx, err := backup.GetPxCentralAdminCtx()
			ctx, err := backup.GetAdminCtxFromSecret()
			expect(err).NotTo(haveOccurred(),
				fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
					err))
			_, err = backupDriver.CreateCloudCredential(ctx, credCreateRequest)
			if err != nil && strings.Contains(err.Error(), "already exists") {
				return
			}
			expect(err).NotTo(haveOccurred(),
				fmt.Sprintf("Failed to create cloud credential [%s] in org [%s]", name, orgID))
		// TODO: validate CreateCloudCredentialResponse also
		case drivers.ProviderAzure:
			logrus.Infof("Create creds for azure")
			tenantID, clientID, clientSecret, subscriptionID, accountName, accountKey := GetAzureCredsFromEnv()
			credCreateRequest := &api.CloudCredentialCreateRequest{
				CreateMetadata: &api.CreateMetadata{
					Name:  name,
					Uid:   uid,
					OrgId: orgID,
				},
				CloudCredential: &api.CloudCredentialInfo{
					Type: api.CloudCredentialInfo_Azure,
					Config: &api.CloudCredentialInfo_AzureConfig{
						AzureConfig: &api.AzureConfig{
							TenantId:       tenantID,
							ClientId:       clientID,
							ClientSecret:   clientSecret,
							AccountName:    accountName,
							AccountKey:     accountKey,
							SubscriptionId: subscriptionID,
						},
					},
				},
			}
			//ctx, err := backup.GetPxCentralAdminCtx()
			ctx, err := backup.GetAdminCtxFromSecret()
			expect(err).NotTo(haveOccurred(),
				fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
					err))
			_, err = backupDriver.CreateCloudCredential(ctx, credCreateRequest)
			if err != nil && strings.Contains(err.Error(), "already exists") {
				return
			}
			expect(err).NotTo(haveOccurred(),
				fmt.Sprintf("Failed to create cloud credential [%s] in org [%s]", name, orgID))
			// TODO: validate CreateCloudCredentialResponse also
		}
	})
}

// CreateS3BackupLocation creates backuplocation for S3
func CreateS3BackupLocation(name string, uid, cloudCred string, cloudCredUID string, bucketName string, orgID string) {
	time.Sleep(60 * time.Second)
	backupDriver := Inst().Backup
	//inspReq := &api.CloudCredentialInspectRequest{Name: cloudCred, Uid: cloudCredUID, OrgId: orgID, IncludeSecrets: true}
	//credCtx, err := backup.GetAdminCtxFromSecret()
	//obj, err := backupDriver.InspectCloudCredential(credCtx, inspReq)
	_, _, endpoint, region, disableSSLBool := GetAWSDetailsFromEnv()
	encryptionKey := "torpedo"
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
			Uid:   uid,
		},
		BackupLocation: &api.BackupLocationInfo{
			Path:          bucketName,
			EncryptionKey: encryptionKey,
			// CloudCredential: "foo",
			// CloudCredential: cloudCred,
			CloudCredentialRef: &api.ObjectRef{
				Name: cloudCred,
				Uid:  cloudCredUID,
			},
			Type: api.BackupLocationInfo_S3,
			Config: &api.BackupLocationInfo_S3Config{
				S3Config: &api.S3Config{
					Endpoint:   endpoint,
					Region:     region,
					DisableSsl: disableSSLBool,
				},
			},
		},
	}
	//ctx, err := backup.GetPxCentralAdminCtx()
	ctx, err := backup.GetAdminCtxFromSecret()
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
			err))
	_, err = backupDriver.CreateBackupLocation(ctx, bLocationCreateReq)
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return
	}
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to create backuplocation [%s] in org [%s]", name, orgID))
}

// CreateAzureBackupLocation creates backuplocation for Azure
func CreateAzureBackupLocation(name string, uid string, cloudCred string, cloudCredUID string, bucketName string, orgID string) {
	backupDriver := Inst().Backup
	encryptionKey := "torpedo"
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
			Uid:   uid,
		},
		BackupLocation: &api.BackupLocationInfo{
			Path:          bucketName,
			EncryptionKey: encryptionKey,
			CloudCredentialRef: &api.ObjectRef{
				Name: cloudCred,
				Uid:  cloudCredUID,
			},
			Type: api.BackupLocationInfo_Azure,
		},
	}
	//ctx, err := backup.GetPxCentralAdminCtx()
	ctx, err := backup.GetAdminCtxFromSecret()
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
			err))
	_, err = backupDriver.CreateBackupLocation(ctx, bLocationCreateReq)
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return
	}
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to create backuplocation [%s] in org [%s]", name, orgID))
}

// GetProvider validates and return object store provider
func GetProvider() string {
	provider, ok := os.LookupEnv("OBJECT_STORE_PROVIDER")
	expect(ok).To(beTrue(), fmt.Sprintf("No environment variable 'PROVIDER' supplied. Valid values are: %s, %s, %s",
		drivers.ProviderAws, drivers.ProviderAzure, drivers.ProviderGke))
	switch provider {
	case drivers.ProviderAws, drivers.ProviderAzure, drivers.ProviderGke:
	default:
		fail(fmt.Sprintf("Valid values for 'PROVIDER' environment variables are: %s, %s, %s",
			drivers.ProviderAws, drivers.ProviderAzure, drivers.ProviderGke))
	}
	return provider
}

// CreateOrganization creates org on px-backup
func CreateOrganization(orgID string) {
	Step(fmt.Sprintf("Create organization [%s]", orgID), func() {
		backupDriver := Inst().Backup
		req := &api.OrganizationCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name: orgID,
			},
		}
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))
		_, err = backupDriver.CreateOrganization(ctx, req)
		//expect(err).NotTo(haveOccurred(),
		//	fmt.Sprintf("Failed to create organization [%s]. Error: [%v]",
		//		orgID, err))
	})
}

// UpdateScheduledBackup updates the scheduled backup with time interval from global vars
func UpdateScheduledBackup(schedulePolicyName, schedulePolicyUID string, ScheduledBackupInterval time.Duration) (err error) {
	var ctx context1.Context

	Step(fmt.Sprintf("Update schedule policy %s", schedulePolicyName), func() {
		backupDriver := Inst().Backup

		// Create a backup schedule
		schedulePolicyUpdateRequest := &api.SchedulePolicyUpdateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  schedulePolicyName,
				Uid:   schedulePolicyUID,
				OrgId: OrgID,
			},

			SchedulePolicy: &api.SchedulePolicyInfo{
				Interval: &api.SchedulePolicyInfo_IntervalPolicy{
					// Retain 5 backups at a time for ease of inspection
					Retain:  5,
					Minutes: int64(ScheduledBackupInterval / time.Minute),
					IncrementalCount: &api.SchedulePolicyInfo_IncrementalCount{
						Count: 0,
					},
				},
			},
		}
		//ctx, err = backup.GetPxCentralAdminCtx()
		ctx, err = backup.GetAdminCtxFromSecret()
		if err != nil {
			return
		}
		_, err = backupDriver.UpdateSchedulePolicy(ctx, schedulePolicyUpdateRequest)
		if err != nil {
			return
		}
	})
	return err
}

// DeleteScheduledBackup deletes the scheduled backup and schedule policy from the CreateScheduledBackup
func DeleteScheduledBackup(backupScheduleName, backupScheduleUID, schedulePolicyName, schedulePolicyUID string) (err error) {
	var ctx context1.Context

	Step(fmt.Sprintf("Delete scheduled backup %s of all namespaces on cluster %s in organization %s",
		backupScheduleName, sourceClusterName, OrgID), func() {
		backupDriver := Inst().Backup

		bkpScheduleDeleteRequest := &api.BackupScheduleDeleteRequest{
			OrgId: OrgID,
			Name:  backupScheduleName,
			// delete_backups indicates whether the cloud backup files need to
			// be deleted or retained.
			DeleteBackups: true,
			Uid:           backupScheduleUID,
		}
		ctx, err = backup.GetPxCentralAdminCtx()
		if err != nil {
			return
		}
		_, err = backupDriver.DeleteBackupSchedule(ctx, bkpScheduleDeleteRequest)
		if err != nil {
			return
		}

		clusterReq := &api.ClusterInspectRequest{OrgId: OrgID, Name: sourceClusterName, IncludeSecrets: true}
		clusterResp, err := backupDriver.InspectCluster(ctx, clusterReq)
		if err != nil {
			return
		}
		clusterObj := clusterResp.GetCluster()

		namespace := "*"
		err = backupDriver.WaitForBackupScheduleDeletion(ctx, backupScheduleName, namespace, OrgID,
			clusterObj,
			BackupRestoreCompletionTimeoutMin*time.Minute,
			RetrySeconds*time.Second)

		schedulePolicyDeleteRequest := &api.SchedulePolicyDeleteRequest{
			OrgId: OrgID,
			Name:  schedulePolicyName,
			Uid:   schedulePolicyUID,
		}
		ctx, err = backup.GetPxCentralAdminCtx()
		if err != nil {
			return
		}
		_, err = backupDriver.DeleteSchedulePolicy(ctx, schedulePolicyDeleteRequest)
		if err != nil {
			return
		}
	})
	return err
}

//AddLabelToResource adds a label to a resource and errors if the resource type is not implemented
func AddLabelToResource(spec interface{}, key string, val string) error {
	if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
		if obj.Labels == nil {
			obj.Labels = make(map[string]string)
		}
		logrus.Infof("Adding label [%s=%s] to PVC %s", key, val, obj.Name)
		obj.Labels[key] = val
		core.Instance().UpdatePersistentVolumeClaim(obj)
		return nil
	} else if obj, ok := spec.(*v1.ConfigMap); ok {
		if obj.Labels == nil {
			obj.Labels = make(map[string]string)
		}
		logrus.Infof("Adding label [%s=%s] to ConfigMap %s", key, val, obj.Name)
		obj.Labels[key] = val
		core.Instance().UpdateConfigMap(obj)
		return nil
	} else if obj, ok := spec.(*v1.Secret); ok {
		if obj.Labels == nil {
			obj.Labels = make(map[string]string)
		}
		logrus.Infof("Adding label [%s=%s] to Secret %s", key, val, obj.Name)
		obj.Labels[key] = val
		core.Instance().UpdateSecret(obj)
		return nil
	}
	return fmt.Errorf("Spec is of unknown resource type")
}

// GetSourceClusterConfigPath returns kubeconfig for source
func GetSourceClusterConfigPath() (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return "", fmt.Errorf("Empty KUBECONFIGS environment variable")
	}

	kubeconfigList := strings.Split(kubeconfigs, ",")
	expect(len(kubeconfigList)).Should(beNumerically(">=", 2),
		"At least minimum two kubeconfigs required")

	return fmt.Sprintf("%s/%s", KubeconfigDirectory, kubeconfigList[0]), nil
}

// GetAzureCredsFromEnv get creds for azure
func GetAzureCredsFromEnv() (tenantID, clientID, clientSecret, subscriptionID, accountName, accountKey string) {
	accountName = os.Getenv("AZURE_ACCOUNT_NAME")
	expect(accountName).NotTo(equal(""),
		"AZURE_ACCOUNT_NAME Environment variable should not be empty")

	accountKey = os.Getenv("AZURE_ACCOUNT_KEY")
	expect(accountKey).NotTo(equal(""),
		"AZURE_ACCOUNT_KEY Environment variable should not be empty")

	logrus.Infof("Create creds for azure")
	tenantID = os.Getenv("AZURE_TENANT_ID")
	expect(tenantID).NotTo(equal(""),
		"AZURE_TENANT_ID Environment variable should not be empty")

	clientID = os.Getenv("AZURE_CLIENT_ID")
	expect(clientID).NotTo(equal(""),
		"AZURE_CLIENT_ID Environment variable should not be empty")

	clientSecret = os.Getenv("AZURE_CLIENT_SECRET")
	expect(clientSecret).NotTo(equal(""),
		"AZURE_CLIENT_SECRET Environment variable should not be empty")

	subscriptionID = os.Getenv("AZURE_SUBSCRIPTION_ID")
	expect(clientSecret).NotTo(equal(""),
		"AZURE_SUBSCRIPTION_ID Environment variable should not be empty")

	return tenantID, clientID, clientSecret, subscriptionID, accountName, accountKey
}

// GetDestinationClusterConfigPath get cluster config of destination cluster
func GetDestinationClusterConfigPath() (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return "", fmt.Errorf("Empty KUBECONFIGS environment variable")
	}

	kubeconfigList := strings.Split(kubeconfigs, ",")
	expect(len(kubeconfigList)).Should(beNumerically(">=", 2),
		"At least minimum two kubeconfigs required")

	return fmt.Sprintf("%s/%s", KubeconfigDirectory, kubeconfigList[1]), nil
}

// SetScheduledBackupInterval sets scheduled backup interval
func SetScheduledBackupInterval(interval time.Duration, triggerType string) {
	scheduledBackupInterval := interval

	var schedulePolicyName string
	var schedulePolicyUID string
	if triggerType == BackupScheduleAllName {
		schedulePolicyName = SchedulePolicyAllName
		schedulePolicyUID = SchedulePolicyAllUID
		ScheduledBackupAllNamespacesInterval = scheduledBackupInterval
	} else {
		schedulePolicyName = SchedulePolicyScaleName
		schedulePolicyUID = SchedulePolicyScaleUID
		ScheduledBackupScaleInterval = scheduledBackupInterval
	}
	_, err := InspectScheduledBackup(schedulePolicyName, schedulePolicyUID)
	if ObjectExists(err) {
		UpdateScheduledBackup(schedulePolicyName, schedulePolicyUID, scheduledBackupInterval)
	}
}

// DeleteS3Bucket deletes bucket in S3
func DeleteS3Bucket(bucketName string) {
	id, secret, endpoint, s3Region, disableSSLBool := GetAWSDetailsFromEnv()
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(id, secret, ""),
		Region:           aws.String(s3Region),
		DisableSSL:       aws.Bool(disableSSLBool),
		S3ForcePathStyle: aws.Bool(true),
	},
	)
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to get S3 session to create bucket. Error: [%v]", err))

	S3Client := s3.New(sess)

	iter := s3manager.NewDeleteListIterator(S3Client, &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})

	err = s3manager.NewBatchDeleteWithClient(S3Client).Delete(aws.BackgroundContext(), iter)
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Unable to delete objects from bucket %q, %v", bucketName, err))

	_, err = S3Client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to delete bucket [%v]. Error: [%v]", bucketName, err))
}

// DeleteAzureBucket delete bucket in azure
func DeleteAzureBucket(bucketName string) {
	// From the Azure portal, get your Storage account blob service URL endpoint.
	_, _, _, _, accountName, accountKey := GetAzureCredsFromEnv()

	urlStr := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, bucketName)
	logrus.Infof("Delete container url %s", urlStr)
	// Create a ContainerURL object that wraps a soon-to-be-created container's URL and a default pipeline.
	u, _ := url.Parse(urlStr)
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to create shared key credential [%v]", err))

	containerURL := azblob.NewContainerURL(*u, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
	ctx := context1.Background() // This example uses a never-expiring context

	_, err = containerURL.Delete(ctx, azblob.ContainerAccessConditions{})

	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to delete container. Error: [%v]", err))
}

// DeleteBucket deletes bucket from the cloud
func DeleteBucket(provider string, bucketName string) {
	Step(fmt.Sprintf("Delete bucket [%s]", bucketName), func() {
		switch provider {
		// TODO(stgleb): PTX-2359 Add DeleteAzureBucket
		case drivers.ProviderAws:
			DeleteS3Bucket(bucketName)
		case drivers.ProviderAzure:
			DeleteAzureBucket(bucketName)
		}
	})
}

// TearDownBackupRestoreAll enumerates backups and restores before deleting them
func TearDownBackupRestoreAll() {
	logrus.Infof("Enumerating scheduled backups")
	bkpScheduleEnumerateReq := &api.BackupScheduleEnumerateRequest{
		OrgId:  OrgID,
		Labels: make(map[string]string),
		BackupLocationRef: &api.ObjectRef{
			Name: backupLocationName,
			Uid:  BackupLocationUID,
		},
	}
	ctx, err := backup.GetPxCentralAdminCtx()
	expect(err).NotTo(haveOccurred())
	enumBkpScheduleResponse, _ := Inst().Backup.EnumerateBackupSchedule(ctx, bkpScheduleEnumerateReq)
	bkpSchedules := enumBkpScheduleResponse.GetBackupSchedules()
	for _, bkpSched := range bkpSchedules {
		schedPol := bkpSched.GetSchedulePolicyRef()
		DeleteScheduledBackup(bkpSched.GetName(), bkpSched.GetUid(), schedPol.GetName(), schedPol.GetUid())
	}

	logrus.Infof("Enumerating backups")
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: OrgID,
	}
	ctx, err = backup.GetPxCentralAdminCtx()
	expect(err).NotTo(haveOccurred())
	enumBkpResponse, _ := Inst().Backup.EnumerateBackup(ctx, bkpEnumerateReq)
	backups := enumBkpResponse.GetBackups()
	for _, bkp := range backups {
		DeleteBackup(bkp.GetName(), OrgID)
	}

	logrus.Infof("Enumerating restores")
	restoreEnumerateReq := &api.RestoreEnumerateRequest{
		OrgId: OrgID}
	ctx, err = backup.GetPxCentralAdminCtx()
	expect(err).NotTo(haveOccurred())
	enumRestoreResponse, _ := Inst().Backup.EnumerateRestore(ctx, restoreEnumerateReq)
	restores := enumRestoreResponse.GetRestores()
	for _, restore := range restores {
		DeleteRestore(restore.GetName(), OrgID)
	}

	for _, bkp := range backups {
		Inst().Backup.WaitForBackupDeletion(ctx, bkp.GetName(), OrgID,
			BackupRestoreCompletionTimeoutMin*time.Minute,
			RetrySeconds*time.Second)
	}
	for _, restore := range restores {
		Inst().Backup.WaitForRestoreDeletion(ctx, restore.GetName(), OrgID,
			BackupRestoreCompletionTimeoutMin*time.Minute,
			RetrySeconds*time.Second)
	}
	provider := GetProvider()
	DeleteCluster(destinationClusterName, OrgID)
	DeleteCluster(sourceClusterName, OrgID)
	DeleteBackupLocation(backupLocationName, OrgID)
	DeleteCloudCredential(CredName, OrgID, CloudCredUID)
	DeleteBucket(provider, BucketName)
}

// CreateBucket creates bucket on the appropriate cloud platform
func CreateBucket(provider string, bucketName string) {
	Step(fmt.Sprintf("Create bucket [%s]", bucketName), func() {
		switch provider {
		case drivers.ProviderAws:
			CreateS3Bucket(bucketName)
		case drivers.ProviderAzure:
			CreateAzureBucket(bucketName)
		}
	})
}

// CreateS3Bucket creates bucket in S3
func CreateS3Bucket(bucketName string) {
	id, secret, endpoint, s3Region, disableSSLBool := GetAWSDetailsFromEnv()
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(id, secret, ""),
		Region:           aws.String(s3Region),
		DisableSSL:       aws.Bool(disableSSLBool),
		S3ForcePathStyle: aws.Bool(true),
	},
	)
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to get S3 session to create bucket. Error: [%v]", err))

	S3Client := s3.New(sess)

	_, err = S3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to create bucket [%v]. Error: [%v]", bucketName, err))

	err = S3Client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to wait for bucket [%v] to get created. Error: [%v]", bucketName, err))
}

// CreateAzureBucket creates bucket in Azure
func CreateAzureBucket(bucketName string) {
	// From the Azure portal, get your Storage account blob service URL endpoint.
	_, _, _, _, accountName, accountKey := GetAzureCredsFromEnv()

	urlStr := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, bucketName)
	logrus.Infof("Create container url %s", urlStr)
	// Create a ContainerURL object that wraps a soon-to-be-created container's URL and a default pipeline.
	u, _ := url.Parse(urlStr)
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to create shared key credential [%v]", err))

	containerURL := azblob.NewContainerURL(*u, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
	ctx := context1.Background() // This example uses a never-expiring context

	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)

	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to create container. Error: [%v]", err))
}

// createAzureBackupLocation creates backup location
func createAzureBackupLocation(name, uid, cloudCred, cloudCredUID, bucketName, orgID string) {
	Step(fmt.Sprintf("Create Azure backup location [%s] in org [%s]", name, orgID), func() {
		CreateAzureBackupLocation(name, uid, cloudCred, cloudCredUID, bucketName, orgID)
	})
}

func dumpKubeConfigs(configObject string, kubeconfigList []string) error {
	logrus.Infof("dump kubeconfigs to file system")
	cm, err := core.Instance().GetConfigMap(configObject, "default")
	if err != nil {
		logrus.Errorf("Error reading config map: %v", err)
		return err
	}
	logrus.Infof("Get over kubeconfig list %v", kubeconfigList)
	for _, kubeconfig := range kubeconfigList {
		config := cm.Data[kubeconfig]
		if len(config) == 0 {
			configErr := fmt.Sprintf("Error reading kubeconfig: found empty %s in config map %s",
				kubeconfig, configObject)
			return fmt.Errorf(configErr)
		}
		filePath := fmt.Sprintf("%s/%s", KubeconfigDirectory, kubeconfig)
		logrus.Infof("Save kubeconfig to %s", filePath)
		err := ioutil.WriteFile(filePath, []byte(config), 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetAWSDetailsFromEnv returns AWS details
func GetAWSDetailsFromEnv() (id string, secret string, endpoint string,
	s3Region string, disableSSLBool bool) {

	// TODO: add separate function to return cred object based on type
	id = os.Getenv("AWS_ACCESS_KEY_ID")
	expect(id).NotTo(equal(""),
		"AWS_ACCESS_KEY_ID Environment variable should not be empty")

	secret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	expect(secret).NotTo(equal(""),
		"AWS_SECRET_ACCESS_KEY Environment variable should not be empty")

	endpoint = os.Getenv("S3_ENDPOINT")
	expect(endpoint).NotTo(equal(""),
		"S3_ENDPOINT Environment variable should not be empty")

	s3Region = os.Getenv("S3_REGION")
	expect(s3Region).NotTo(equal(""),
		"S3_REGION Environment variable should not be empty")

	disableSSL := os.Getenv("S3_DISABLE_SSL")
	expect(disableSSL).NotTo(equal(""),
		"S3_DISABLE_SSL Environment variable should not be empty")

	disableSSLBool, err := strconv.ParseBool(disableSSL)
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("S3_DISABLE_SSL=%s is not a valid boolean value", disableSSL))

	return id, secret, endpoint, s3Region, disableSSLBool
}

// DumpKubeconfigs gets kubeconfigs from configmap
func DumpKubeconfigs(kubeconfigList []string) {
	err := dumpKubeConfigs(configMapName, kubeconfigList)
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to get kubeconfigs [%v] from configmap [%s]", kubeconfigList, configMapName))
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
	GlobalScaleFactor                   int
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
	LicenseExpiryTimeoutHours           time.Duration
	MeteringIntervalMins                time.Duration
	ConfigMap                           string
	BundleLocation                      string
	CustomAppConfig                     map[string]scheduler.AppConfig
	Backup                              backup.Driver
	SecretType                          string
	PureVolumes                         bool
	VaultAddress                        string
	VaultToken                          string
	SchedUpgradeHops                    string
	AutopilotUpgradeImage               string
	CsiGenericDriverConfigMap           string
	HelmValuesConfigMap                 string
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
	var licenseExpiryTimeoutHours time.Duration
	var meteringIntervalMins time.Duration
	var bundleLocation string
	var customConfigPath string
	var customAppConfig map[string]scheduler.AppConfig
	var enableStorkUpgrade bool
	var secretType string
	var pureVolumes bool
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
	flag.DurationVar(&driverStartTimeout, "driver-start-timeout", defaultDriverStartTimeout, "Maximum wait volume driver startup")
	flag.DurationVar(&autoStorageNodeRecoveryTimeout, "storagenode-recovery-timeout", defaultAutoStorageNodeRecoveryTimeout, "Maximum wait time in minutes for storageless nodes to transition to storagenodes in case of ASG")
	flag.DurationVar(&licenseExpiryTimeoutHours, licenseExpiryTimeoutHoursFlag, defaultLicenseExpiryTimeoutHours, "Maximum wait time in hours after which force expire license")
	flag.DurationVar(&meteringIntervalMins, meteringIntervalMinsFlag, defaultMeteringIntervalMins, "Metering interval in minutes for metering agent")
	flag.StringVar(&configMapName, configMapFlag, "", "Name of the config map to be used.")
	flag.StringVar(&bundleLocation, "bundle-location", defaultBundleLocation, "Path to support bundle output files")
	flag.StringVar(&customConfigPath, "custom-config", "", "Path to custom configuration files")
	flag.StringVar(&secretType, "secret-type", scheduler.SecretK8S, "Path to custom configuration files")
	flag.BoolVar(&pureVolumes, "pure-volumes", false, "To enable using Pure backend for shared volumes")
	flag.StringVar(&vaultAddress, "vault-addr", "", "Path to custom configuration files")
	flag.StringVar(&vaultToken, "vault-token", "", "Path to custom configuration files")
	flag.StringVar(&schedUpgradeHops, "sched-upgrade-hops", "", "Comma separated list of versions scheduler upgrade to take hops")
	flag.StringVar(&autopilotUpgradeImage, autopilotUpgradeImageCliFlag, "", "Autopilot version which will be used for checking version after upgrade autopilot")
	flag.StringVar(&csiGenericDriverConfigMapName, csiGenericDriverConfigMapFlag, "", "Name of config map that stores provisioner details when CSI generic driver is being used")
	flag.StringVar(&testrailuttils.MilestoneName, milestoneFlag, "", "Testrail milestone name")
	flag.StringVar(&testrailuttils.RunName, testrailRunNameFlag, "", "Testrail run name, this run will be updated in testrail")
	flag.StringVar(&testrailuttils.JobRunID, testrailRunIDFlag, "", "Run ID for the testrail run")
	flag.StringVar(&testrailuttils.JenkinsBuildURL, testrailJenkinsBuildURLFlag, "", "Jenins job url for testrail update")
	flag.StringVar(&testRailHostname, testRailHostFlag, "", "Testrail server hostname")
	flag.StringVar(&testRailUsername, testRailUserNameFlag, "", "Username to be used for adding entries to testrail")
	flag.StringVar(&testRailPassword, testRailPasswordFlag, "", "Password to be used for testrail update")

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
				GlobalScaleFactor:                   appScaleFactor,
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
				PureVolumes:                         pureVolumes,
				VaultAddress:                        vaultAddress,
				VaultToken:                          vaultToken,
				SchedUpgradeHops:                    schedUpgradeHops,
				AutopilotUpgradeImage:               autopilotUpgradeImage,
				CsiGenericDriverConfigMap:           csiGenericDriverConfigMapName,
				LicenseExpiryTimeoutHours:           licenseExpiryTimeoutHours,
				MeteringIntervalMins:                meteringIntervalMins,
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
