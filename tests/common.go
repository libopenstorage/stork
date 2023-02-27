package tests

import (
	"bufio"
	"encoding/base64"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"

	"github.com/portworx/torpedo/pkg/aetosutil"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/units"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/portworx/torpedo/pkg/s3utils"

	storageapi "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	opsapi "github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/pkg/sched"
	oputil "github.com/libopenstorage/operator/drivers/storage/portworx/util"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/storkctl"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/monitor"
	"github.com/portworx/torpedo/drivers/node"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/jirautils"
	"github.com/portworx/torpedo/pkg/osutils"
	"github.com/portworx/torpedo/pkg/pureutils"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	appsapi "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"

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
	// import ibm driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/ibm"
	// import oracle driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/oracle"

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

	// import driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/monitor/prometheus"

	context1 "context"

	"gopkg.in/natefinch/lumberjack.v2"
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
	monitorDriverCliFlag                 = "monitor-driver"
	storageDriverCliFlag                 = "storage-driver"
	backupCliFlag                        = "backup-driver"
	specDirCliFlag                       = "spec-dir"
	appListCliFlag                       = "app-list"
	secureAppsCliFlag                    = "secure-apps"
	repl1AppsCliFlag                     = "repl1-apps"
	logLocationCliFlag                   = "log-location"
	logLevelCliFlag                      = "log-level"
	scaleFactorCliFlag                   = "scale-factor"
	minRunTimeMinsFlag                   = "minimun-runtime-mins"
	chaosLevelFlag                       = "chaos-level"
	hyperConvergedFlag                   = "hyper-converged"
	storageUpgradeEndpointURLCliFlag     = "storage-upgrade-endpoint-url"
	storageUpgradeEndpointVersionCliFlag = "storage-upgrade-endpoint-version"
	upgradeStorageDriverEndpointListFlag = "upgrade-storage-driver-endpoint-list"
	provisionerFlag                      = "provisioner"
	storageNodesPerAZFlag                = "max-storage-nodes-per-az"
	configMapFlag                        = "config-map"
	enableStorkUpgradeFlag               = "enable-stork-upgrade"
	autopilotUpgradeImageCliFlag         = "autopilot-upgrade-version"
	csiGenericDriverConfigMapFlag        = "csi-generic-driver-config-map"
	licenseExpiryTimeoutHoursFlag        = "license_expiry_timeout_hours"
	meteringIntervalMinsFlag             = "metering_interval_mins"
	SourceClusterName                    = "source-cluster"
	destinationClusterName               = "destination-cluster"
	backupLocationNameConst              = "tp-blocation"
	backupScheduleNamePrefix             = "tp-bkp-schedule"
	backupScheduleScaleName              = "-scale"
	configMapName                        = "kubeconfigs"
	pxNamespace                          = "kube-system"

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

	jiraUserNameFlag  = "jira-username"
	jiraTokenFlag     = "jira-token"
	jiraAccountIDFlag = "jira-account-id"

	// Async DR
	pairFileName           = "cluster-pair.yaml"
	remotePairName         = "remoteclusterpair"
	remoteFilePath         = "/tmp/kubeconfig"
	appReadinessTimeout    = 10 * time.Minute
	migrationKey           = "async-dr-"
	migrationRetryTimeout  = 10 * time.Minute
	migrationRetryInterval = 10 * time.Second
	defaultClusterPairDir  = "cluster-pair"

	envSkipDiagCollection = "SKIP_DIAG_COLLECTION"

	torpedoJobNameFlag = "torpedo-job-name"
	torpedoJobTypeFlag = "torpedo-job-type"
)

// Dashboard params
const (
	enableDashBoardFlag     = "enable-dash"
	userFlag                = "user"
	testTypeFlag            = "test-type"
	testDescriptionFlag     = "test-desc"
	testTagsFlag            = "test-tags"
	testSetIDFlag           = "testset-id"
	testBranchFlag          = "branch"
	testProductFlag         = "product"
	failOnPxPodRestartCount = "fail-on-px-pod-restartcount"
	portworxOperatorName    = "portworx-operator"
)

// Backup constants
const (
	BackupNamePrefix                  = "tp-backup"
	RestoreNamePrefix                 = "tp-restore"
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
	defaultMonitorDriver                  = "prometheus"
	defaultStorageDriver                  = "pxd"
	defaultLogLocation                    = "/testresults/"
	defaultBundleLocation                 = "/var/cores"
	defaultLogLevel                       = "debug"
	defaultAppScaleFactor                 = 1
	defaultMinRunTimeMins                 = 0
	defaultChaosLevel                     = 5
	defaultStorageUpgradeEndpointURL      = "https://install.portworx.com"
	defaultStorageUpgradeEndpointVersion  = "2.1.1"
	defaultStorageProvisioner             = "portworx"
	defaultStorageNodesPerAZ              = 2
	defaultAutoStorageNodeRecoveryTimeout = 30 * time.Minute
	specObjAppWorkloadSizeEnvVar          = "SIZE"
	defaultLicenseExpiryTimeoutHours      = 1 * time.Hour
	defaultMeteringIntervalMins           = 10 * time.Minute
	authTokenParam                        = "auth-token"
	defaultTorpedoJob                     = "torpedo-job"
	defaultTorpedoJobType                 = "functional"
	labelNameKey                          = "name"
)

const (
	waitResourceCleanup       = 2 * time.Minute
	defaultTimeout            = 5 * time.Minute
	defaultVolScaleTimeout    = 4 * time.Minute
	defaultRetryInterval      = 10 * time.Second
	defaultCmdTimeout         = 20 * time.Second
	defaultCmdRetryInterval   = 5 * time.Second
	defaultDriverStartTimeout = 10 * time.Minute
)

const (
	VSPHERE_MAX_CLOUD_DRIVES        = 12
	FA_MAX_CLOUD_DRIVES             = 32
	CLOUD_PROVIDER_MAX_CLOUD_DRIVES = 8
	POOL_MAX_CLOUD_DRIVES           = 6

	PX_VSPHERE_SCERET_NAME = "px-vsphere-secret"
	PX_PURE_SECRET_NAME    = "px-pure-secret"
)

const (
	pxctlCDListCmd = "pxctl cd list"
)

var pxRuntimeOpts string

const (
	taskNamePrefix = "backupcreaterestore"
	orgID          = "default"
)

var (
	errPureFileSnapshotNotSupported    = errors.New("snapshot feature is not supported for pure_file volumes")
	errPureCloudsnapNotSupported       = errors.New("cloudsnap feature is not supported for pure volumes")
	errPureGroupsnapNotSupported       = errors.New("groupsnap feature is not supported for pure volumes")
	errUnexpectedSizeChangeAfterPureIO = errors.New("the size change in bytes is not expected after write to Pure volume")
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

var (
	jiraUserName string
	jiraToken    string
)

const (
	rootLogDir   = "/root/logs"
	diagsDirPath = "diags.pwx.dev.purestorage.com:/var/lib/osd/pxns/688230076034934618"
)

type Weekday string

const (
	Monday    Weekday = "Mon"
	Tuesday           = "Tue"
	Wednesday         = "Wed"
	Thursday          = "Thu"
	Friday            = "Fri"
	Saturday          = "Sat"
	Sunday            = "Sun"
)

// TpLogPath torpedo log path
var tpLogPath string
var suiteLogger *lumberjack.Logger

// TestLogger for logging test logs
var TestLogger *lumberjack.Logger
var dash *aetosutil.Dashboard
var post_rule_uid string
var pre_rule_uid string

// InitInstance is the ginkgo spec for initializing torpedo
func InitInstance() {
	var err error
	var token string

	err = Inst().S.Init(scheduler.InitOptions{
		SpecDir:                          Inst().SpecDir,
		VolDriverName:                    Inst().V.String(),
		NodeDriverName:                   Inst().N.String(),
		MonitorDriverName:                Inst().M.String(),
		SecretConfigMapName:              Inst().ConfigMap,
		CustomAppConfig:                  Inst().CustomAppConfig,
		StorageProvisioner:               Inst().Provisioner,
		SecretType:                       Inst().SecretType,
		VaultAddress:                     Inst().VaultAddress,
		VaultToken:                       Inst().VaultToken,
		PureVolumes:                      Inst().PureVolumes,
		PureSANType:                      Inst().PureSANType,
		RunCSISnapshotAndRestoreManyTest: Inst().RunCSISnapshotAndRestoreManyTest,
		HelmValuesConfigMapName:          Inst().HelmValuesConfigMap,
		SecureApps:                       Inst().SecureAppList,
	})

	log.FailOnError(err, "Error occured while Scheduler Driver Initialization")

	if Inst().ConfigMap != "" {
		log.Infof("Using Config Map: %s ", Inst().ConfigMap)
		token, err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
		log.FailOnError(err, "Error occured while getting token from config map")
		log.Infof("Token used for initializing: %s ", token)
	} else {
		token = ""
	}

	err = Inst().N.Init(node.InitOptions{
		SpecDir: Inst().SpecDir,
	})
	log.FailOnError(err, "Error occured while Node Driver Initialization")

	err = Inst().V.Init(Inst().S.String(), Inst().N.String(), token, Inst().Provisioner, Inst().CsiGenericDriverConfigMap)
	log.FailOnError(err, "Error occured while Volume Driver Initialization")

	err = Inst().M.Init(Inst().JobName, Inst().JobType)
	log.FailOnError(err, "Error occured while monitor Initialization")

	if Inst().Backup != nil {
		err = Inst().Backup.Init(Inst().S.String(), Inst().N.String(), Inst().V.String(), token)
		log.FailOnError(err, "Error occured while Backup Driver Initialization")
	}
	if testRailHostname != "" && testRailUsername != "" && testRailPassword != "" {
		err = testrailuttils.Init(testRailHostname, testRailUsername, testRailPassword)
		if err == nil {
			if testrailuttils.MilestoneName == "" || testrailuttils.RunName == "" || testrailuttils.JobRunID == "" {
				err = fmt.Errorf("not all details provided to update testrail")
				log.FailOnError(err, "Error occured while testrail initialization")
			}
			testrailuttils.CreateMilestone()
		}
	} else {
		log.Debugf("Not all information to connect to testrail is provided, skipping updates to testrail")
	}

	if jiraUserName != "" && jiraToken != "" {
		log.Infof("Initializing JIRA connection")
		jirautils.Init(jiraUserName, jiraToken)

	} else {
		log.Debugf("Not all information to connect to JIRA is provided.")
	}

	pxVersion, err := Inst().V.GetDriverVersion()
	log.FailOnError(err, "Error occured while getting PX version")
	commitID := strings.Split(pxVersion, "-")[1]
	t := Inst().Dash.TestSet
	t.CommitID = commitID
	if pxVersion != "" {
		t.Tags["px-version"] = pxVersion
	}
}

// ValidateCleanup checks that there are no resource leaks after the test run
func ValidateCleanup() {
	Step("validate cleanup of resources used by the test suite", func() {
		log.InfoD("validate cleanup of resources used by the test suite")
		t := func() (interface{}, bool, error) {
			if err := Inst().V.ValidateVolumeCleanup(); err != nil {
				return "", true, err
			}

			return "", false, nil
		}

		_, err := task.DoRetryWithTimeout(t, waitResourceCleanup, 10*time.Second)
		if err != nil {
			log.Infof("an error occurred, collecting bundle")
			CollectSupport()
		}
		dash.VerifyFatal(err, nil, "Validate cleanup operation successful?")
	})
}

func processError(err error, errChan ...*chan error) {
	// if errChan is provided then just push err to on channel
	// Useful for frameworks like longevity that must continue
	// execution and must not fail immediately
	if len(errChan) > 0 {
		log.Errorf(fmt.Sprintf("%v", err))
		updateChannel(err, errChan...)
	} else {
		log.FailOnError(err, "processError")
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
		log.InfoD(fmt.Sprintf("Validating %s app", ctx.App.Key))
		appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
		if ctx.ReadinessTimeout == time.Duration(0) {
			timeout = appScaleFactor * defaultTimeout
		} else {
			timeout = appScaleFactor * ctx.ReadinessTimeout
		}

		Step(fmt.Sprintf("validate %s app's volumes", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				log.InfoD(fmt.Sprintf("Validating %s app's volumes", ctx.App.Key))
				ValidateVolumes(ctx, errChan...)
			}
		})

		stepLog := fmt.Sprintf("wait for %s app to start running", ctx.App.Key)

		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := Inst().S.WaitForRunning(ctx, timeout, defaultRetryInterval)
			if err != nil {
				PrintDescribeContext(ctx)
				processError(err, errChan...)
				return
			}
		})

		// Validating Topology Labels for apps if Topology is enabled
		if len(Inst().TopologyLabels) > 0 {
			stepLog = fmt.Sprintf("validate topology labels for %s app", ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				err := Inst().S.ValidateTopologyLabel(ctx)
				if err != nil {
					processError(err, errChan...)
					return
				}
			})
		}
		stepLog = fmt.Sprintf("validate if %s app's volumes are setup", ctx.App.Key)

		Step(stepLog, func() {
			if ctx.SkipVolumeValidation {
				return
			}
			log.InfoD(fmt.Sprintf("validate if %s app's volumes are setup", ctx.App.Key))

			vols, err := Inst().S.GetVolumes(ctx)
			// Fixing issue where it is priniting nil
			if err != nil {
				processError(err, errChan...)
			}

			for _, vol := range vols {
				stepLog = fmt.Sprintf("validate if %s app's volume: %v is setup", ctx.App.Key, vol)
				Step(stepLog, func() {
					log.Infof(stepLog)
					err := Inst().V.ValidateVolumeSetup(vol)
					if err != nil {
						processError(err, errChan...)
					}
				})
			}
		})

		Step("Validate Px pod restart count", func() {
			ValidatePxPodRestartCount(ctx, errChan...)
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
				ValidatePureSnapshotsSDK(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's volumes resizing ", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateResizePurePVC(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("wait for %s app to start running", ctx.App.Key), func() {
			err := Inst().S.WaitForRunning(ctx, timeout, defaultRetryInterval)
			processError(err, errChan...)
		})

		Step(fmt.Sprintf("validate %s app's volumes statistics ", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidatePureVolumeStatisticsDynamicUpdate(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's pure volumes has no replicaset", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidatePureVolumeNoReplicaSet(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's pure volumes cloning", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateCSIVolumeClone(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's pure volumes snapshot and restore", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateCSISnapshotAndRestore(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's pure volume snapshot and restoring to many volumes", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidatePureVolumeLargeNumOfClones(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s px pool expansion when pure volumes attached", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidatePoolExpansionWithPureVolumes(ctx, errChan...)
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
				ValidatePureVolumesPXCTL(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's snapshots for pxctl", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidatePureSnapshotsPXCTL(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's volumes resizing ", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateResizePurePVC(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("wait for %s app to start running", ctx.App.Key), func() {
			err := Inst().S.WaitForRunning(ctx, timeout, defaultRetryInterval)
			processError(err, errChan...)
		})

		Step(fmt.Sprintf("validate %s app's volumes statistics ", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidatePureVolumeStatisticsDynamicUpdate(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's pure volumes has no replicaset", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidatePureVolumeNoReplicaSet(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's pure volumes cloning", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateCSIVolumeClone(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's pure volumes snapshot and restore", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidateCSISnapshotAndRestore(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's pure volume snapshot and restoring to many volumes", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidatePureVolumeLargeNumOfClones(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s px pool expansion when pure volumes attached", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidatePoolExpansionWithPureVolumes(ctx, errChan...)
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
			vols, err := Inst().S.GetVolumes(ctx)
			if err != nil {
				log.Errorf("Failed to get app %s's volumes", ctx.App.Key)
				processError(err, errChan...)
			}
			volScaleFactor := 1
			if len(vols) > 10 {
				// Take into account the number of volumes in the app. More volumes will
				// take longer to format if the backend storage has limited bandwidth. Even if the
				// GlobalScaleFactor is 1, high number of volumes in a single app instance
				// may slow things down.
				volScaleFactor = len(vols) / 10
				log.Infof("Using vol scale factor of %d for app %s", volScaleFactor, ctx.App.Key)
			}
			scaleFactor := time.Duration(Inst().GlobalScaleFactor * volScaleFactor)
			err = Inst().S.ValidateVolumes(ctx, scaleFactor*defaultVolScaleTimeout, defaultRetryInterval, nil)
			if err != nil {
				PrintDescribeContext(ctx)
				processError(err, errChan...)
			}
		})

		var vols map[string]map[string]string
		Step(fmt.Sprintf("get %s app's volume's custom parameters", ctx.App.Key), func() {
			vols, err = Inst().S.GetVolumeParameters(ctx)
			if err != nil {
				processError(err, errChan...)
			}
		})

		for vol, params := range vols {
			if Inst().ConfigMap != "" {
				params[authTokenParam], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
				if err != nil {
					processError(err, errChan...)
				}
			}
			if ctx.RefreshStorageEndpoint {
				params["refresh-endpoint"] = "true"
			}
			Step(fmt.Sprintf("get %s app's volume: %s inspected by the volume driver", ctx.App.Key, vol), func() {
				err = Inst().V.ValidateCreateVolume(vol, params)
				if err != nil {
					PrintDescribeContext(ctx)
					processError(err, errChan...)
				}
			})
		}
	})
}

// ValidatePureSnapshotsSDK is the ginkgo spec for validating Pure direct access volume snapshots using API for a context
func ValidatePureSnapshotsSDK(ctx *scheduler.Context, errChan ...*chan error) {
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
				if params["backend"] == k8s.PureBlock {
					expect(err).To(beNil(), "unexpected error creating pure_block snapshot")
				} else if params["backend"] == k8s.PureFile {
					expect(err).NotTo(beNil(), "error expected but no error received while creating pure_file snapshot")
					if err != nil {
						expect(err.Error()).To(contain(errPureFileSnapshotNotSupported.Error()), "incorrect error received creating pure_file snapshot")
					}
				}
			})
			Step(fmt.Sprintf("get %s app's volume: %s then create cloudsnap", ctx.App.Key, vol), func() {
				err = Inst().V.ValidateCreateCloudsnap(vol, params)
				expect(err).NotTo(beNil(), "error expected but no error received while creating Pure cloudsnap")
				if err != nil {
					expect(err.Error()).To(contain(errPureCloudsnapNotSupported.Error()), "incorrect error received creating Pure cloudsnap")
				}
			})
		}
	})
}

// ValidatePureVolumesPXCTL is the ginkgo spec for validating FA/FB DA volumes using PXCTL for a context
func ValidatePureVolumesPXCTL(ctx *scheduler.Context, errChan ...*chan error) {
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

		for vol := range vols {
			Step(fmt.Sprintf("get %s app's volume: %s then check that it appears in pxctl", ctx.App.Key, vol), func() {
				err = Inst().V.ValidateVolumeInPxctlList(vol)
				expect(err).To(beNil(), "unexpected error validating volume appears in pxctl list")
			})
		}
	})
}

// ValidatePureSnapshotsPXCTL is the ginkgo spec for validating FADA volume snapshots using PXCTL for a context
func ValidatePureSnapshotsPXCTL(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of an app's volumes", func() {
		var (
			err  error
			vols map[string]map[string]string
		)
		Step(fmt.Sprintf("get %s app's volume's custom parameters", ctx.App.Key), func() {
			vols, err = Inst().S.GetVolumeParameters(ctx)
			processError(err, errChan...)
		})

		for vol, params := range vols {
			Step(fmt.Sprintf("get %s app's volume: %s then create snapshot using pxctl", ctx.App.Key, vol), func() {
				err = Inst().V.ValidateCreateSnapshotUsingPxctl(vol)
				if params["backend"] == k8s.PureBlock {
					expect(err).To(beNil(), "unexpected error creating pure_block snapshot")
				} else if params["backend"] == k8s.PureFile {
					expect(err).NotTo(beNil(), "error expected but no error received while creating pure_file snapshot")
					if err != nil {
						expect(err.Error()).To(contain(errPureFileSnapshotNotSupported.Error()), "incorrect error received creating pure_file snapshot")
					}
				}
			})
			Step(fmt.Sprintf("get %s app's volume: %s then create cloudsnap using pxctl", ctx.App.Key, vol), func() {
				err = Inst().V.ValidateCreateCloudsnapUsingPxctl(vol)
				expect(err).NotTo(beNil(), "error expected but no error received while creating Pure cloudsnap")
				if err != nil {
					expect(err.Error()).To(contain(errPureCloudsnapNotSupported.Error()), "incorrect error received creating Pure cloudsnap")
				}
			})
		}
		Step("validating groupsnap for using pxctl", func() {
			err = Inst().V.ValidateCreateGroupSnapshotUsingPxctl()
			expect(err).NotTo(beNil(), "error expected but no error received while creating Pure groupsnap")
			if err != nil {
				expect(err.Error()).To(contain(errPureGroupsnapNotSupported.Error()), "incorrect error received creating Pure groupsnap")
			}
		})
	})
}

// ValidateResizePurePVC is the ginkgo spec for validating resize of volumes
func ValidateResizePurePVC(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of an resizing pvc", func() {
		var err error
		Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
			appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
			err = Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval, nil)
			processError(err, errChan...)
		})

		Step("validating resizing pvcs", func() {
			_, err = Inst().S.ResizeVolume(ctx, "")
			expect(err).To(beNil(), "unexpected error resizing Pure PVC")
		})

		// TODO: add more checks (is the PVC resized in the pod?), we currently only check that the
		//       CSI resize succeeded.
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
			vols, err = Inst().S.GetVolumes(ctx)
			processError(err, errChan...)
		})

		err = Inst().V.ValidatePureVolumesNoReplicaSets(vols[0].ID, make(map[string]string))
		expect(err).NotTo(haveOccurred(), "failed to validate that no replica sets present for Pure volume")

	})
}

// ValidatePureVolumeStatisticsDynamicUpdate is the ginkgo spec for validating dynamic update of byteUsed statistic for pure volumes
func ValidatePureVolumeStatisticsDynamicUpdate(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of a resizing pvc", func() {
		var err error
		Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
			appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
			err = Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval, nil)
			processError(err, errChan...)
		})
		var vols []*volume.Volume
		Step(fmt.Sprintf("get %s app's pure volumes", ctx.App.Key), func() {
			vols, err = Inst().S.GetVolumes(ctx)
			processError(err, errChan...)
		})
		byteUsedInitial, err := Inst().V.ValidateGetByteUsedForVolume(vols[0].ID, make(map[string]string))
		fmt.Printf("initially the byteUsed is %v\n", byteUsedInitial)
		// get the pod for this pvc
		pods, err := Inst().S.GetPodsForPVC(vols[0].Name, vols[0].Namespace)
		processError(err, errChan...)

		mountPath, bytesToWrite := pureutils.GetAppDataDir(pods[0].Namespace)

		// write to the Direct Access volume
		ddCmd := fmt.Sprintf("dd bs=512 count=%d if=/dev/urandom of=%s/myfile", bytesToWrite/512, mountPath)
		cmdArgs := []string{"exec", "-it", pods[0].Name, "-n", pods[0].Namespace, "--", "bash", "-c", ddCmd}
		err = osutils.Kubectl(cmdArgs)
		processError(err, errChan...)
		fmt.Println("sleeping to let volume usage get reflected")
		// wait until the backends size is reflected before making the REST call
		time.Sleep(time.Minute * 2)

		byteUsedAfter, err := Inst().V.ValidateGetByteUsedForVolume(vols[0].ID, make(map[string]string))
		fmt.Printf("after writing random bytes to the file the byteUsed in volume %s is %v\n", vols[0].ID, byteUsedAfter)
		expect(byteUsedAfter > byteUsedInitial).To(beTrue(), "bytes used did not increase after writing random bytes to the file")

	})
}

// ValidateCSISnapshotAndRestore is the ginkgo spec for validating actually creating a FADA snapshot, restoring and verifying the content
func ValidateCSISnapshotAndRestore(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of an snapshot and restoring", func() {
		var err error
		timestamp := strconv.Itoa(int(time.Now().Unix()))
		snapShotClassName := PureSnapShotClass + "-" + timestamp
		if _, err := Inst().S.CreateCsiSnapshotClass(snapShotClassName, "Delete"); err != nil {
			log.Errorf("Create volume snapshot class failed with error: [%v]", err)
			expect(err).NotTo(haveOccurred(), "failed to create snapshot class")
		}

		var vols []*volume.Volume
		Step(fmt.Sprintf("get %s app's pure volumes", ctx.App.Key), func() {
			vols, err = Inst().S.GetPureVolumes(ctx, "pure_block")
			processError(err, errChan...)
		})
		if len(vols) == 0 {
			log.Warnf("No FlashArray DirectAccess volumes, skipping")
			processError(err, errChan...)
		} else {
			request := scheduler.CSISnapshotRequest{
				Namespace:         vols[0].Namespace,
				Timestamp:         timestamp,
				OriginalPVCName:   vols[0].Name,
				SnapName:          "basic-csi-snapshot-" + timestamp,
				RestoredPVCName:   "csi-restored-" + timestamp,
				SnapshotclassName: snapShotClassName,
			}
			err = Inst().S.CSISnapshotTest(ctx, request)
			processError(err, errChan...)
			// the snap name shown in pxctl isn't the CSI snapshot name, it's original PV name + "-snap"
			var volMap map[string]map[string]string
			Step(fmt.Sprintf("get %s app's volume's custom parameters", ctx.App.Key), func() {
				volMap, err = Inst().S.GetVolumeParameters(ctx)
				processError(err, errChan...)
			})
			for k, v := range volMap {
				if v["pvc_name"] == vols[0].Name && v["pvc_namespace"] == vols[0].Namespace {
					Step(fmt.Sprintf("get %s app's snapshot: %s then check that it appears in pxctl", ctx.App.Key, k), func() {
						err = Inst().V.ValidateVolumeInPxctlList(fmt.Sprint(k, "-snap"))
						expect(err).To(beNil(), "unexpected error validating snapshot appears in pxctl list")
					})
					break
				}

			}

		}
	})
}

// ValidateCSIVolumeClone is the ginkgo spec for cloning a volume and verifying the content
func ValidateCSIVolumeClone(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of an cloning", func() {
		var err error
		var vols []*volume.Volume
		Step(fmt.Sprintf("get %s app's pure volumes", ctx.App.Key), func() {
			vols, err = Inst().S.GetPureVolumes(ctx, "pure_block")
			processError(err, errChan...)
		})
		if len(vols) == 0 {
			log.Warnf("No FlashArray DirectAccess volumes, skipping")
			processError(err, errChan...)
		} else {
			timestamp := strconv.Itoa(int(time.Now().Unix()))
			request := scheduler.CSICloneRequest{
				Timestamp:       timestamp,
				Namespace:       vols[0].Namespace,
				OriginalPVCName: vols[0].Name,
				RestoredPVCName: "csi-cloned-" + timestamp,
			}

			err = Inst().S.CSICloneTest(ctx, request)
			processError(err, errChan...)
		}
	})
}

// ValidatePureVolumeLargeNumOfClones is the ginkgo spec for restoring a snapshot to many volumes
func ValidatePureVolumeLargeNumOfClones(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of an restoring large number of volumes from a snapshot", func() {
		var err error
		timestamp := strconv.Itoa(int(time.Now().Unix()))
		snapShotClassName := PureSnapShotClass + "." + timestamp
		if _, err := Inst().S.CreateCsiSnapshotClass(snapShotClassName, "Delete"); err != nil {
			log.Errorf("Create volume snapshot class failed with error: [%v]", err)
			expect(err).NotTo(haveOccurred(), "failed to create snapshot class")
		}

		var vols []*volume.Volume
		Step(fmt.Sprintf("get %s app's pure volumes", ctx.App.Key), func() {
			vols, err = Inst().S.GetPureVolumes(ctx, "pure_block")
			processError(err, errChan...)
		})
		if len(vols) == 0 {
			log.Warnf("No FlashArray DirectAccess volumes, skipping")
			processError(err, errChan...)
		} else {
			request := scheduler.CSISnapshotRequest{
				Namespace:         vols[0].Namespace,
				Timestamp:         timestamp,
				OriginalPVCName:   vols[0].Name,
				SnapName:          "basic-csi-snapshot-many" + timestamp,
				RestoredPVCName:   "csi-restored-many" + timestamp,
				SnapshotclassName: snapShotClassName,
			}
			err = Inst().S.CSISnapshotAndRestoreMany(ctx, request)
			processError(err, errChan...)
		}
	})
}

// ValidatePoolExpansionWithPureVolumes is the ginkgo spec for executing a pool expansion when FA/FB volumes is attached
func ValidatePoolExpansionWithPureVolumes(ctx *scheduler.Context, errChan ...*chan error) {
	context("For validation of an expanding storage pools while FA/FB volumes are attached", func() {
		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		if err != nil {
			err = fmt.Errorf("error getting storage pools list. Err: %v", err)
			log.Error(err.Error())
			processError(err, errChan...)
		}

		if len(pools) == 0 {
			err = fmt.Errorf("length of pools should be greater than 0")
			processError(err, errChan...)
		}
		for _, pool := range pools {
			initialPoolSize := pool.TotalSize / units.GiB
			err = Inst().V.ResizeStoragePoolByPercentage(pool.Uuid, opsapi.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 20)
			if err != nil {
				err = fmt.Errorf("error initiating pool [%v ] %v: [%v]", pool.Uuid, opsapi.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, err.Error())
				log.Error(err.Error())
			} else {
				err = waitForPoolToBeResized(initialPoolSize, pool.Uuid)
				if err != nil {
					err = fmt.Errorf("pool [%v] %v failed. Error: %v", pool.Uuid, opsapi.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, err)
					log.Error(err)
				}
			}
		}
	})

}

func checkPureVolumeExpectedSizeChange(sizeChangeInBytes uint64) error {
	if sizeChangeInBytes < (512-30)*oneMegabytes || sizeChangeInBytes > (512+30)*oneMegabytes {
		return errUnexpectedSizeChangeAfterPureIO
	}
	return nil
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
			log.Infof("Updated parameter list: [%+v]\n", updatedVolumeParams)
			ValidateVolumeParameters(updatedVolumeParams)

			Step(fmt.Sprintf("validate if %s app's volumes are setup", ctx.App.Key), func() {
				vols, err := Inst().S.GetVolumes(ctx)
				log.Infof("List of volumes from scheduler driver :[%+v] \n for context : [%+v]\n", vols, ctx)
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

func ValidateFastpathVolume(ctx *scheduler.Context, expectedStatus opsapi.FastpathStatus) error {
	appVolumes, err := Inst().S.GetVolumes(ctx)
	if err != nil {
		return err
	}
	for _, vol := range appVolumes {
		appVol, err := Inst().V.InspectVolume(vol.ID)
		if err != nil {
			return err
		}
		if decommissionedNode.Name != "" && decommissionedNode.Id == appVol.FpConfig.Replicas[0].NodeUuid {
			expectedStatus = opsapi.FastpathStatus_FASTPATH_INACTIVE

		}

		fpConfig := appVol.FpConfig
		log.Infof("fpconfig: %+v", fpConfig)
		if len(fpConfig.Replicas) > 1 {
			expectedStatus = opsapi.FastpathStatus_FASTPATH_INACTIVE
		}
		if fpConfig.Status == expectedStatus {
			log.Infof("Fastpath status is %v", fpConfig.Status)
			if fpConfig.Status == opsapi.FastpathStatus_FASTPATH_ACTIVE {
				if fpConfig.Dirty {
					return fmt.Errorf("fastpath vol %s is dirty", vol.Name)
				}
				if !fpConfig.Promote {
					return fmt.Errorf("fastpath vol %s is not promoted", vol.Name)
				}
			}
		} else {
			return fmt.Errorf("expected Fastpath Status: %v, Actual: %v", expectedStatus, fpConfig.Status)
		}
	}

	return nil
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
		stepLog := fmt.Sprintf("start destroying %s app", ctx.App.Key)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err = Inst().S.Destroy(ctx, opts)
			if err != nil {
				PrintDescribeContext(ctx)
			}
			log.FailOnError(err, "Failed to destroy app %s", ctx.App.Key)

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

func PrintDescribeContext(ctx *scheduler.Context) {
	descOut, descErr := Inst().S.Describe(ctx)
	if descErr != nil {
		log.Warnf("Error describing context %s", ctx.App.Key)
		log.Warn(descErr)
	} else {
		log.Warnf("Context %s Details:", ctx.App.Key)
		log.Warnf(descOut)
	}

}

// DeleteVolumes deletes volumes of a given context
func DeleteVolumes(ctx *scheduler.Context, options *scheduler.VolumeOptions) []*volume.Volume {
	var err error
	var vols []*volume.Volume
	Step(fmt.Sprintf("destroy the %s app's volumes", ctx.App.Key), func() {
		log.Infof("destroy the %s app's volumes", ctx.App.Key)
		vols, err = Inst().S.DeleteVolumes(ctx, options)
		log.FailOnError(err, "Failed to delete app %s's volumes", ctx.App.Key)
	})
	return vols
}

// ValidateVolumesDeleted checks it given volumes got deleted
func ValidateVolumesDeleted(appName string, vols []*volume.Volume) {
	for _, vol := range vols {
		Step(fmt.Sprintf("validate %s app's volume %s has been deleted in the volume driver",
			appName, vol.Name), func() {
			log.InfoD("validate %s app's volume %s has been deleted in the volume driver",
				appName, vol.Name)
			err := Inst().V.ValidateDeleteVolume(vol)
			log.FailOnError(err, fmt.Sprintf("%s's volume %s deletion failed", appName, vol.Name))
			dash.VerifyFatal(err, nil, fmt.Sprintf("%s's volume %s deleted successfully?", appName, vol.Name))
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
		options := scheduler.ScheduleOptions{
			AppKeys:            Inst().AppList,
			StorageProvisioner: Inst().Provisioner,
		}
		//if not hyper converged set up deploy apps only on storageless nodes
		if !Inst().IsHyperConverged {
			log.Infof("Scheduling apps only on storageless nodes")
			storagelessNodes := node.GetStorageLessNodes()
			if len(storagelessNodes) == 0 {
				log.Info("No storageless nodes available in the PX Cluster. Setting HyperConverges as true")
				Inst().IsHyperConverged = true
			}
			for _, storagelessNode := range storagelessNodes {
				if err = Inst().S.AddLabelOnNode(storagelessNode, "storage", "NO"); err != nil {
					err = fmt.Errorf("failed to add label key [%s] and value [%s] in node [%s]. Error:[%v]",
						"storage", "NO", storagelessNode.Name, err)
					processError(err, errChan...)
				}
			}
			storageLessNodeLabels := make(map[string]string)
			storageLessNodeLabels["storage"] = "NO"

			options = scheduler.ScheduleOptions{
				AppKeys:            Inst().AppList,
				StorageProvisioner: Inst().Provisioner,
				Nodes:              storagelessNodes,
				Labels:             storageLessNodeLabels,
			}

		} else {
			log.Infof("Scheduling Apps with hyper-converged")
		}
		taskName := fmt.Sprintf("%s-%v", testname, Inst().InstanceID)
		contexts, err = Inst().S.Schedule(taskName, options)
		// Need to check err != nil before calling processError
		if err != nil {
			processError(err, errChan...)
		}
		if len(contexts) == 0 {
			processError(fmt.Errorf("list of contexts is empty for [%s]", taskName), errChan...)
		}
	})

	return contexts
}

// ScheduleAppsInTopologyEnabledCluster schedules but does not wait for applications
func ScheduleAppsInTopologyEnabledCluster(
	testname string, labels []map[string]string, errChan ...*chan error) []*scheduler.Context {
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
			TopologyLabels:     labels,
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
		log.InfoD("Validate applications")
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
		stepLog := fmt.Sprintf("start volume driver on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.Info(stepLog)
			for _, n := range appNodes {
				err := Inst().V.StartDriver(n)
				processError(err, errChan...)
			}
		})

		stepLog = fmt.Sprintf("wait for volume driver to start on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.Info(stepLog)
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
		stepLog := fmt.Sprintf("stop volume driver on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.Info(stepLog)
			err := Inst().V.StopDriver(appNodes, false, nil)
			processError(err, errChan...)
		})

		stepLog = fmt.Sprintf("wait for volume driver to stop on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.Info(stepLog)
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
		stepLog := fmt.Sprintf("crash volume driver on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := Inst().V.StopDriver(appNodes, true, nil)
			processError(err, errChan...)
		})

		stepLog = fmt.Sprintf("wait for volume driver to start on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.Info(stepLog)
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
		log.InfoD("Validating apps")
		for _, ctx := range contexts {
			ValidateContext(ctx)
		}
	})

	Step("destroy apps", func() {
		log.InfoD("Destroying apps")
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
		log.Debugf("storage expansion enabled on at least one storage pool")
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
							log.Debugf("pool: %s workloadSize increased by: %d total now: %d", poolUUID, wSize, workloadSizesByPool[poolUUID])
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

				log.Debugf("pool: %s InitialSize: %d WorkloadSize: %d", sPool.Uuid, sPool.StoragePoolAtInit.TotalSize, n.StoragePools[id].WorkloadSize)
			}
			err = node.UpdateNode(n)
			expect(err).NotTo(haveOccurred())
		}
	}

	err = Inst().V.ValidateStoragePools()
	expect(err).NotTo(haveOccurred())

}

// ValidatePxPodRestartCount validates portworx restart count
func ValidatePxPodRestartCount(ctx *scheduler.Context, errChan ...*chan error) {
	context("Validating portworx pods restart count ...", func() {
		Step("Getting current restart counts for portworx pods and matching", func() {
			pxLabel := make(map[string]string)
			pxLabel[labelNameKey] = defaultStorageProvisioner
			pxPodRestartCountMap, err := Inst().S.GetPodsRestartCount(pxNamespace, pxLabel)
			//Using fatal verification will abort longevity runs
			if err != nil {
				log.Errorf(fmt.Sprintf("Failed to get portworx pod restart count for %v, Err : %v", pxLabel, err))
			}

			// Validate portworx pod restart count after test
			for pod, value := range pxPodRestartCountMap {
				n, err := node.GetNodeByIP(pod.Status.HostIP)
				log.FailOnError(err, "Failed to get node object using IP: %s", pod.Status.HostIP)
				if n.PxPodRestartCount != value {
					log.Warnf("Portworx pods restart count not matches, expected %d actual %d", value, n.PxPodRestartCount)
					if Inst().PortworxPodRestartCheck {
						log.Fatalf("portworx pods restart [%d] times", value)
					}
				}
			}

			// Validate portworx operator pod check
			pxLabel[labelNameKey] = portworxOperatorName
			pxPodRestartCountMap, err = Inst().S.GetPodsRestartCount(pxNamespace, pxLabel)
			//Using fatal verification will abort longevity runs
			if err != nil {
				log.Errorf(fmt.Sprintf("Failed to get portworx pod restart count for %v, Err : %v", pxLabel, err))
			}
			for _, v := range pxPodRestartCountMap {
				if v > 0 {
					log.Warnf("Portworx operator pods restart count %d is greater than 0", v)
					if Inst().PortworxPodRestartCheck {
						log.Fatalf("portworx operator pods restart [%d] times", v)
					}
				}
			}
		})
	})
}

// DescribeNamespace takes in the scheduler contexts and describes each object within the test context.
func DescribeNamespace(contexts []*scheduler.Context) {
	context("generating namespace info...", func() {
		Step(fmt.Sprintf("Describe Namespace objects for test %s \n", ginkgo.CurrentGinkgoTestDescription().TestText), func() {
			for _, ctx := range contexts {
				filename := fmt.Sprintf("%s/%s-%s.namespace.log", defaultBundleLocation, ctx.App.Key, ctx.UID)
				namespaceDescription, err := Inst().S.Describe(ctx)
				if err != nil {
					log.Errorf("failed to describe namespace for [%s] %s. Cause: %v", ctx.UID, ctx.App.Key, err)
				}
				if err = ioutil.WriteFile(filename, []byte(namespaceDescription), 0755); err != nil {
					log.Errorf("failed to save file %s. Cause: %v", filename, err)
				}
			}
		})
	})
}

// ValidateClusterSize validates number of storage nodes in given cluster
// using total cluster size `count` and max_storage_nodes_per_zone
func ValidateClusterSize(count int64) {
	zones, err := Inst().N.GetZones()
	log.FailOnError(err, "Zones empty")
	log.InfoD("ASG is running in [%+v] zones\n", zones)
	perZoneCount := count / int64(len(zones))

	// Validate total node count
	currentNodeCount, err := Inst().N.GetASGClusterSize()
	log.FailOnError(err, "Failed to Get ASG Cluster Size")

	dash.VerifyFatal(currentNodeCount, perZoneCount*int64(len(zones)), "ASG cluster size is as expected?")

	// Validate storage node count
	var expectedStorageNodesPerZone int
	if Inst().MaxStorageNodesPerAZ <= int(perZoneCount) {
		expectedStorageNodesPerZone = Inst().MaxStorageNodesPerAZ
	} else {
		expectedStorageNodesPerZone = int(perZoneCount)
	}
	storageNodes, err := GetStorageNodes()
	log.FailOnError(err, "Storage nodes are empty")

	log.Info("List of storage nodes:[%v]", storageNodes)
	dash.VerifyFatal(len(storageNodes), expectedStorageNodesPerZone*len(zones), "Storage nodes matches the expected number?")
}

// GetStorageNodes get storage nodes in the cluster
func GetStorageNodes() ([]node.Node, error) {

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
	context("generating support bundle...", func() {
		log.InfoD("generating support bundle...")
		skipStr := os.Getenv(envSkipDiagCollection)
		if skipStr != "" {
			if skip, err := strconv.ParseBool(skipStr); err == nil && skip {
				log.Infof("skipping diag collection because env var %s=%s", envSkipDiagCollection, skipStr)
				return
			}
		}
		nodes := node.GetWorkerNodes()
		dash.VerifyFatal(len(nodes) > 0, true, "Worker nodes found ?")

		for _, n := range nodes {
			if !n.IsStorageDriverInstalled {
				continue
			}
			Step(fmt.Sprintf("save all useful logs on node %s", n.SchedulerNodeName), func() {
				log.Infof("save all useful logs on node %s", n.SchedulerNodeName)

				// Moves this out to deal with diag testing.
				r := &volume.DiagRequestConfig{
					DockerHost:    "unix:///var/run/docker.sock",
					OutputFile:    fmt.Sprintf("/var/cores/diags-%s-%d.tar.gz", n.Name, time.Now().Unix()),
					ContainerName: "",
					Profile:       false,
					Live:          false,
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

func runCmd(cmd string, n node.Node) error {
	_, err := Inst().N.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         defaultCmdTimeout,
		TimeBeforeRetry: defaultCmdRetryInterval,
		Sudo:            true,
	})
	if err != nil {
		log.Warnf("failed to run cmd: %s. err: %v", cmd, err)
	}

	return err

}

func runCmdWithNoSudo(cmd string, n node.Node) error {
	_, err := Inst().N.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         defaultCmdTimeout,
		TimeBeforeRetry: defaultCmdRetryInterval,
		Sudo:            false,
	})
	if err != nil {
		log.Warnf("failed to run cmd: %s. err: %v", cmd, err)
	}

	return err

}

// PerformSystemCheck check if core files are present on each node
func PerformSystemCheck() {
	context("checking for core files...", func() {
		log.Info("checking for core files...")
		Step("verifying if core files are present on each node", func() {
			log.InfoD("verifying if core files are present on each node")
			nodes := node.GetNodes()
			expect(nodes).NotTo(beEmpty())
			for _, n := range nodes {
				if !n.IsStorageDriverInstalled {
					continue
				}
				log.InfoD("looking for core files on node %s", n.Name)
				file, err := Inst().N.SystemCheck(n, node.ConnectionOpts{
					Timeout:         2 * time.Minute,
					TimeBeforeRetry: 10 * time.Second,
				})
				if len(file) != 0 || err != nil {
					dash.Errorf("Core file was found on node %s, Core Path: %s", n.Name, file)
					// Collect Support Bundle only once
					CollectSupport()
					log.Fatalf("Core generated, please check logs for more details")
				}
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
func DeleteCloudCredential(name string, orgID string, cloudCredUID string) error {

	backupDriver := Inst().Backup
	credDeleteRequest := &api.CloudCredentialDeleteRequest{
		Name:  name,
		OrgId: orgID,
		Uid:   cloudCredUID,
	}
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return err
	}
	_, err = backupDriver.DeleteCloudCredential(ctx, credDeleteRequest)
	return err
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
	log.Debugf("contexts: %v", &contexts)
	ginkgoTestDescr := ginkgo.CurrentGinkgoTestDescription()
	if ginkgoTestDescr.Failed {
		log.Infof(">>>> FAILED TEST: %s", ginkgoTestDescr.FullTestText)
		CollectSupport()
		DescribeNamespace(contexts)
		testStatus = "Fail"
	}
	if len(ids) >= 1 {
		driverVersion, err := Inst().V.GetDriverVersion()
		if err != nil {
			log.Errorf("Error in getting driver version")
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
func SetClusterContext(clusterConfigPath string) error {
	err := Inst().S.SetConfig(clusterConfigPath)
	if err != nil {
		return fmt.Errorf("Failed to switch to context. Set Config Error: [%v]", err)
	}
	err = Inst().S.RefreshNodeRegistry()
	if err != nil {
		return fmt.Errorf("Failed to switch to context. RefreshNodeRegistry Error: [%v]", err)
	}

	err = Inst().V.RefreshDriverEndpoints()
	if err != nil {
		return fmt.Errorf("Failed to switch to context. RefreshDriverEndpoints Error: [%v]", err)
	}
	return nil
}

// SetSourceKubeConfig sets current context to the kubeconfig passed as source to the torpedo test
func SetSourceKubeConfig() error {
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	if err != nil {
		return err
	}
	SetClusterContext(sourceClusterConfigPath)
	return nil
}

// SetDestinationKubeConfig sets current context to the kubeconfig passed as destination to the torpedo test
func SetDestinationKubeConfig() {
	destClusterConfigPath, err := GetDestinationClusterConfigPath()
	expect(err).NotTo(haveOccurred())
	SetClusterContext(destClusterConfigPath)
}

// ScheduleValidateClusterPair Schedule a clusterpair by creating a yaml file and validate it
func ScheduleValidateClusterPair(ctx *scheduler.Context, skipStorage, resetConfig bool, clusterPairDir string, reverse bool) error {
	var kubeConfigPath string
	var err error
	if reverse {
		SetSourceKubeConfig()
		// get the kubeconfig path to get the correct pairing info
		kubeConfigPath, err = GetSourceClusterConfigPath()
		if err != nil {
			return err
		}
	} else {
		SetDestinationKubeConfig()
		// get the kubeconfig path to get the correct pairing info
		kubeConfigPath, err = GetDestinationClusterConfigPath()
		if err != nil {
			return err
		}
	}

	pairInfo, err := Inst().V.GetClusterPairingInfo(kubeConfigPath, "", IsEksPxOperator(), reverse)
	if err != nil {
		log.Errorf("Error writing to clusterpair.yml: %v", err)
		return err
	}

	err = CreateClusterPairFile(pairInfo, skipStorage, resetConfig, clusterPairDir, kubeConfigPath)
	if err != nil {
		log.Errorf("Error creating cluster Spec: %v", err)
		return err
	}
	err = Inst().S.RescanSpecs(Inst().SpecDir, Inst().V.String())
	if err != nil {
		log.Errorf("Unable to parse spec dir: %v", err)
		return err
	}

	// Set the correct cluster context to apply the cluster pair spec
	if reverse {
		SetDestinationKubeConfig()
	} else {
		SetSourceKubeConfig()
	}

	err = Inst().S.AddTasks(ctx,
		scheduler.ScheduleOptions{AppKeys: []string{clusterPairDir}})
	if err != nil {
		log.Errorf("Failed to schedule Cluster Pair Specs: %v", err)
		return err
	}

	err = Inst().S.WaitForRunning(ctx, defaultTimeout, defaultRetryInterval)
	if err != nil {
		log.Errorf("Error waiting to get cluster pair in ready state: %v", err)
		return err
	}

	return nil
}

// CreateClusterPairFile creates a cluster pair yaml file inside the stork test pod in path 'clusterPairDir'
func CreateClusterPairFile(pairInfo map[string]string, skipStorage, resetConfig bool, clusterPairDir string, kubeConfigPath string) error {
	log.Infof("Entering cluster pair")
	err := os.MkdirAll(path.Join(Inst().SpecDir, clusterPairDir), 0777)
	if err != nil {
		log.Errorf("Unable to make directory (%v) for cluster pair spec: %v", Inst().SpecDir+"/"+clusterPairDir, err)
		return err
	}
	clusterPairFileName := path.Join(Inst().SpecDir, clusterPairDir, pairFileName)
	pairFile, err := os.Create(clusterPairFileName)
	if err != nil {
		log.Errorf("Unable to create clusterPair.yaml: %v", err)
		return err
	}
	defer func() {
		err := pairFile.Close()
		if err != nil {
			log.Errorf("Error closing pair file: %v", err)
		}
	}()

	factory := storkctl.NewFactory()
	cmd := storkctl.NewCommand(factory, os.Stdin, pairFile, os.Stderr)
	cmd.SetArgs([]string{"generate", "clusterpair", remotePairName, "--kubeconfig", kubeConfigPath})
	if err := cmd.Execute(); err != nil {
		log.Errorf("Execute storkctl failed: %v", err)
		return err
	}

	truncCmd := `sed -i "$((` + "`wc -l " + clusterPairFileName + "|awk '{print $1}'`" + `-4)),$ d" ` + clusterPairFileName
	log.Infof("trunc cmd: %v", truncCmd)
	err = exec.Command("sh", "-c", truncCmd).Run()
	if err != nil {
		log.Errorf("truncate failed %v", err)
		return err
	}

	if resetConfig {
		// storkctl generate command sets sched-ops to source cluster config
		SetSourceKubeConfig()
	} else {
		// Change kubeconfig to destination cluster config
		SetDestinationKubeConfig()
	}

	if skipStorage {
		log.Info("cluster-pair.yml created")
		return nil
	}

	return addStorageOptions(pairInfo, clusterPairFileName)
}

func addStorageOptions(pairInfo map[string]string, clusterPairFileName string) error {
	file, err := os.OpenFile(clusterPairFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		log.Errorf("Unable to open %v: %v", pairFileName, err)
		return err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			log.Errorf("Error closing pair file: %v", err)
		}
	}()
	w := bufio.NewWriter(file)
	for k, v := range pairInfo {
		if k == "port" {
			// port is integer
			v = "\"" + v + "\""
		}
		_, err = fmt.Fprintf(w, "    %v: %v\n", k, v)
		if err != nil {
			log.Infof("error writing file %v", err)
			return err
		}
	}
	err = w.Flush()
	if err != nil {
		return err
	}

	log.Infof("cluster-pair.yml created with storage options in %s", clusterPairFileName)
	return nil

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
				log.Infof("Skipping validating namespace %s because %s", namespace, err)
			} else {
				ginkgo.Describe(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {

					Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
						appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
						volOpts := mapToVolumeOptions(volOptsMap)
						err = Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval, volOpts)
					})
					if err != nil {
						bkpErrors[namespace] = err
						log.Errorf("Failed to validate [%s] app. Error: [%v]", ctx.App.Key, err)
						return
					}

					Step(fmt.Sprintf("wait for %s app to start running", ctx.App.Key), func() {
						appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
						err = Inst().S.WaitForRunning(ctx, appScaleFactor*defaultTimeout, defaultRetryInterval)
					})
					if err != nil {
						bkpErrors[namespace] = err
						log.Errorf("Failed to validate [%s] app. Error: [%v]", ctx.App.Key, err)
						return
					}

					updatedVolumeParams = UpdateVolumeInVolumeParameters(volumeParameters)
					log.Infof("Updated parameter list: [%+v]\n", updatedVolumeParams)
					err = ValidateVolumeParametersGetErr(updatedVolumeParams)
					if err != nil {
						bkpErrors[namespace] = err
						log.Errorf("Failed to validate [%s] app. Error: [%v]", ctx.App.Key, err)
						return
					}

					Step(fmt.Sprintf("validate if %s app's volumes are setup", ctx.App.Key), func() {
						var vols []*volume.Volume
						vols, err = Inst().S.GetVolumes(ctx)
						log.Infof("List of volumes from scheduler driver :[%+v] \n for context : [%+v]\n", vols, ctx)
						if err != nil {
							bkpErrors[namespace] = err
							log.Errorf("Failed to validate [%s] app. Error: [%v]", ctx.App.Key, err)
						}

						for _, vol := range vols {
							Step(fmt.Sprintf("validate if %s app's volume: %v is setup", ctx.App.Key, vol), func() {
								err = Inst().V.ValidateVolumeSetup(vol)
								if err != nil {
									bkpErrors[namespace] = err
									log.Errorf("Failed to validate [%s] app. Error: [%v]", ctx.App.Key, err)
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
			Cluster:        SourceClusterName,
			Namespaces:     namespaces,
			LabelSelectors: labelSelectors,
		}
		ctx, err := backup.GetAdminCtxFromSecret()
		expect(err).NotTo(haveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))
		_, err = backupDriver.CreateBackup(ctx, bkpCreateRequest)
		if err != nil {
			log.Errorf("Failed to create backup [%s] in org [%s]. Error: [%v]",
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
		backupScheduleNamePrefix+backupScheduleName, namespaces, SourceClusterName, OrgID), func() {
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
			Cluster: SourceClusterName,
			// Label selectors to choose resources
			LabelSelectors: labelSelectors,

			SchedulePolicyRef: &api.ObjectRef{
				Name: schedulePolicyName,
				Uid:  schedulePolicyUID,
			},
			BackupLocationRef: &api.ObjectRef{
				Name: backupLocationNameConst,
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
		log.Infof("No namespace to delete")
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

// GetBackupCreateRequest returns a backupcreaterequest
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

// CreateBackupFromRequest creates a backup using a provided request
func CreateBackupFromRequest(backupName string, orgID string, request *api.BackupCreateRequest) (err error) {
	ctx, err := backup.GetAdminCtxFromSecret()
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]", err))
	backupDriver := Inst().Backup
	_, err = backupDriver.CreateBackup(ctx, request)
	if err != nil {
		log.Errorf("Failed to create backup [%s] in org [%s]. Error: [%v]",
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
		log.Infof("Enumerating backups")
		bkpEnumerateReq := &api.BackupEnumerateRequest{
			OrgId: OrgID}
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
		backupScheduleNamePrefix, SourceClusterName, OrgID), func() {
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

// DeleteLabelFromResource deletes a label by key from some resource and doesn't error if something doesn't exist
func DeleteLabelFromResource(spec interface{}, key string) {
	if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
		if obj.Labels != nil {
			_, ok := obj.Labels[key]
			if ok {
				log.Infof("Deleting label with key [%s] from PVC %s", key, obj.Name)
				delete(obj.Labels, key)
				core.Instance().UpdatePersistentVolumeClaim(obj)
			}
		}
	} else if obj, ok := spec.(*v1.ConfigMap); ok {
		if obj.Labels != nil {
			_, ok := obj.Labels[key]
			if ok {
				log.Infof("Deleting label with key [%s] from ConfigMap %s", key, obj.Name)
				delete(obj.Labels, key)
				core.Instance().UpdateConfigMap(obj)
			}
		}
	} else if obj, ok := spec.(*v1.Secret); ok {
		if obj.Labels != nil {
			_, ok := obj.Labels[key]
			if ok {
				log.Infof("Deleting label with key [%s] from Secret %s", key, obj.Name)
				delete(obj.Labels, key)
				core.Instance().UpdateSecret(obj)
			}
		}
	}
}

// DeleteBackupAndDependencies deletes backup and dependent backups
func DeleteBackupAndDependencies(backupName string, backupUID string, orgID string, clusterName string) error {
	ctx, err := backup.GetAdminCtxFromSecret()

	backupDeleteRequest := &api.BackupDeleteRequest{
		Name:    backupName,
		Uid:     backupUID,
		OrgId:   orgID,
		Cluster: clusterName,
	}
	_, err = Inst().Backup.DeleteBackup(ctx, backupDeleteRequest)
	if err != nil {
		return err
	}

	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUID,
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
		err = DeleteBackupAndDependencies(dependency, backupUID, orgID, clusterName)
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
func DeleteRestore(restoreName string, orgID string, ctx context1.Context) error {
	backupDriver := Inst().Backup
	dash.VerifyFatal(backupDriver != nil, true, "Getting the backup driver")
	deleteRestoreReq := &api.RestoreDeleteRequest{
		OrgId: orgID,
		Name:  restoreName,
	}
	_, err := backupDriver.DeleteRestore(ctx, deleteRestoreReq)
	return err
	// TODO: validate createClusterResponse also
}

// DeleteBackup deletes backup
func DeleteBackup(backupName string, backupUID string, orgID string, ctx context1.Context) (*api.BackupDeleteResponse, error) {
	var err error
	var backupDeleteResponse *api.BackupDeleteResponse

	Step(fmt.Sprintf("Delete backup [%s] in org [%s]",
		backupName, orgID), func() {
		backupDriver := Inst().Backup
		bkpDeleteRequest := &api.BackupDeleteRequest{
			Name:  backupName,
			OrgId: orgID,
			Uid:   backupUID,
		}
		backupDeleteResponse, err = backupDriver.DeleteBackup(ctx, bkpDeleteRequest)
		// Best effort cleanup, dont fail test, if deletion fails
		//expect(err).NotTo(haveOccurred(),
		//	fmt.Sprintf("Failed to delete backup [%s] in org [%s]", backupName, orgID))
		// TODO: validate createClusterResponse also
	})
	return backupDeleteResponse, err
}

// DeleteCluster deletes/de-registers cluster from px-backup
func DeleteCluster(name string, orgID string, ctx context1.Context) error {

	backupDriver := Inst().Backup
	clusterDeleteReq := &api.ClusterDeleteRequest{
		OrgId: orgID,
		Name:  name,
	}
	_, err := backupDriver.DeleteCluster(ctx, clusterDeleteReq)
	return err
}

// DeleteBackupLocation deletes backup location
func DeleteBackupLocation(name string, backupLocationUID string, orgID string) error {

	backupDriver := Inst().Backup
	bLocationDeleteReq := &api.BackupLocationDeleteRequest{
		Name:          name,
		OrgId:         orgID,
		DeleteBackups: true,
		Uid:           backupLocationUID,
	}
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return err
	}
	_, err = backupDriver.DeleteBackupLocation(ctx, bLocationDeleteReq)
	if err != nil {
		return err
	}
	// TODO: validate createBackupLocationResponse also
	return nil

}

// DeleteSchedule deletes backup schedule
func DeleteSchedule(backupScheduleName, backupScheduleUID, schedulePolicyName, schedulePolicyUID, OrgID string) error {
	backupDriver := Inst().Backup
	bkpScheduleDeleteRequest := &api.BackupScheduleDeleteRequest{
		OrgId: OrgID,
		Name:  backupScheduleName,
		// DeleteBackups indicates whether the cloud backup files need to
		// be deleted or retained.
		DeleteBackups: true,
		Uid:           backupScheduleUID,
	}
	ctx, err := backup.GetPxCentralAdminCtx()
	if err != nil {
		return err
	}
	_, err = backupDriver.DeleteBackupSchedule(ctx, bkpScheduleDeleteRequest)
	if err != nil {
		return err
	}
	clusterReq := &api.ClusterInspectRequest{OrgId: OrgID, Name: SourceClusterName, IncludeSecrets: true}
	clusterResp, err := backupDriver.InspectCluster(ctx, clusterReq)
	if err != nil {
		return err
	}
	clusterObj := clusterResp.GetCluster()
	namespace := "*"
	err = backupDriver.WaitForBackupScheduleDeletion(ctx, backupScheduleName, namespace, OrgID,
		clusterObj,
		BackupRestoreCompletionTimeoutMin*time.Minute,
		RetrySeconds*time.Second)
	if err != nil {
		return err
	}
	schedulePolicyDeleteRequest := &api.SchedulePolicyDeleteRequest{
		OrgId: OrgID,
		Name:  schedulePolicyName,
		Uid:   schedulePolicyUID,
	}
	ctx, err = backup.GetPxCentralAdminCtx()
	if err != nil {
		return err
	}
	_, err = backupDriver.DeleteSchedulePolicy(ctx, schedulePolicyDeleteRequest)
	if err != nil {
		return err
	}
	return nil
}

// CreateSourceAndDestClusters creates source and destination cluster
// 1st cluster in KUBECONFIGS ENV var is source cluster while
// 2nd cluster is destination cluster
func CreateSourceAndDestClusters(orgID string, cloudName string, uid string, ctx context1.Context) error {
	// TODO: Add support for adding multiple clusters from
	// comma separated list of kubeconfig files
	kubeconfigs := os.Getenv("KUBECONFIGS")
	dash.VerifyFatal(kubeconfigs != "", true, "Getting KUBECONFIGS Environment variable")
	kubeconfigList := strings.Split(kubeconfigs, ",")
	// Validate user has provided at least 2 kubeconfigs for source and destination cluster
	if len(kubeconfigList) != 2 {
		return fmt.Errorf("2 kubeconfigs are required for source and destination cluster")
	}
	err := dumpKubeConfigs(configMapName, kubeconfigList)
	if err != nil {
		return err
	}
	// Register source cluster with backup driver
	log.InfoD("Create cluster [%s] in org [%s]", SourceClusterName, orgID)
	srcClusterConfigPath, err := GetSourceClusterConfigPath()
	if err != nil {
		return err
	}
	log.Infof("Save cluster %s kubeconfig to %s", SourceClusterName, srcClusterConfigPath)

	sourceClusterStatus := func() (interface{}, bool, error) {
		err = CreateCluster(SourceClusterName, srcClusterConfigPath, orgID, cloudName, uid, ctx)
		if err != nil && !strings.Contains(err.Error(), "already exists with status: Online") {
			return "", true, err
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(sourceClusterStatus, 2*time.Minute, 10*time.Second)
	if err != nil {
		return err
	}
	// Register destination cluster with backup driver
	log.InfoD("Create cluster [%s] in org [%s]", destinationClusterName, orgID)
	dstClusterConfigPath, err := GetDestinationClusterConfigPath()
	if err != nil {
		return err
	}
	log.Infof("Save cluster %s kubeconfig to %s", destinationClusterName, dstClusterConfigPath)
	destClusterStatus := func() (interface{}, bool, error) {
		err = CreateCluster(destinationClusterName, dstClusterConfigPath, orgID, cloudName, uid, ctx)
		if err != nil && !strings.Contains(err.Error(), "already exists with status: Online") {
			return "", true, err
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(destClusterStatus, 2*time.Minute, 10*time.Second)
	if err != nil {
		return err
	}
	return nil
}

// CreateBackupLocation creates backup location
func CreateBackupLocation(provider, name, uid, credName, credUID, bucketName, orgID string, encryptionKey string) error {
	var err error
	switch provider {
	case drivers.ProviderAws:
		err = CreateS3BackupLocation(name, uid, credName, credUID, bucketName, orgID, encryptionKey)
	case drivers.ProviderAzure:
		err = CreateAzureBackupLocation(name, uid, credName, CloudCredUID, bucketName, orgID)
	}
	return err
}

// CreateCluster creates/registers cluster with px-backup
func CreateCluster(name string, kubeconfigPath string, orgID string, cloud_name string, uid string, ctx context1.Context) error {
	var clusterCreateReq *api.ClusterCreateRequest

	log.InfoD("Create cluster [%s] in org [%s]", name, orgID)
	backupDriver := Inst().Backup
	kubeconfigRaw, err := ioutil.ReadFile(kubeconfigPath)
	if err != nil {
		return err
	}
	if cloud_name != "" {
		clusterCreateReq = &api.ClusterCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  name,
				OrgId: orgID,
			},
			Kubeconfig: base64.StdEncoding.EncodeToString(kubeconfigRaw),
			CloudCredentialRef: &api.ObjectRef{
				Name: cloud_name,
				Uid:  uid,
			},
		}
	} else {
		clusterCreateReq = &api.ClusterCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  name,
				OrgId: orgID,
			},
			Kubeconfig: base64.StdEncoding.EncodeToString(kubeconfigRaw),
		}
	}
	_, err = backupDriver.CreateCluster(ctx, clusterCreateReq)
	if err != nil {
		return err
	}
	return nil
}

// CreateCloudCredential creates cloud credetials
func CreateCloudCredential(provider, name string, uid, orgID string) {
	Step(fmt.Sprintf("Create cloud credential [%s] in org [%s]", name, orgID), func() {
		log.Infof("Create credential name %s for org %s provider %s", name, orgID, provider)
		backupDriver := Inst().Backup
		switch provider {
		case drivers.ProviderAws:
			log.Infof("Create creds for aws")
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

			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]", err))

			_, err = backupDriver.CreateCloudCredential(ctx, credCreateRequest)
			if err != nil && strings.Contains(err.Error(), "already exists") {
				return
			}
			expect(err).NotTo(haveOccurred(),
				fmt.Sprintf("Failed to create cloud credential [%s] in org [%s]", name, orgID))
		// TODO: validate CreateCloudCredentialResponse also
		case drivers.ProviderAzure:
			log.Infof("Create creds for azure")
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

// CreateCloudCredential creates cloud credetials
func CreateCloudCredentialNonAdminUser(provider, name string, uid, orgID string, ctx context1.Context) error {
	log.Infof("Create credential name %s for org %s provider %s", name, orgID, provider)
	backupDriver := Inst().Backup
	switch provider {
	case drivers.ProviderAws:
		log.Infof("Create creds for aws")
		id := os.Getenv("AWS_ACCESS_KEY_ID")
		if id == "" {
			return fmt.Errorf("AWS_ACCESS_KEY_ID Environment variable should not be empty")
		}
		secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
		if secret == "" {
			return fmt.Errorf("AWS_SECRET_ACCESS_KEY Environment variable should not be empty")
		}
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
		_, err := backupDriver.CreateCloudCredential(ctx, credCreateRequest)
		if err != nil && strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return err
	// TODO: validate CreateCloudCredentialResponse also
	case drivers.ProviderAzure:
		log.Infof("Create creds for azure")
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

		_, err := backupDriver.CreateCloudCredential(ctx, credCreateRequest)
		if err != nil && strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return err
	}
	return nil
}

// CreateS3BackupLocation creates backuplocation for S3
func CreateS3BackupLocation(name string, uid, cloudCred string, cloudCredUID string, bucketName string, orgID string, encryptionKey string) error {
	time.Sleep(60 * time.Second)
	backupDriver := Inst().Backup
	_, _, endpoint, region, disableSSLBool := s3utils.GetAWSDetailsFromEnv()
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

	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return err
	}

	_, err = backupDriver.CreateBackupLocation(ctx, bLocationCreateReq)
	if err != nil {
		return fmt.Errorf("failed to create backup location: %v", err)
	}
	return nil
}

// CreateS3BackupLocationNonAdminUser creates backuplocation for S3
func CreateS3BackupLocationNonAdminUser(name string, uid, cloudCred string, cloudCredUID string, bucketName string, orgID string, encryptionKey string, ctx context1.Context) error {
	backupDriver := Inst().Backup
	_, _, endpoint, region, disableSSLBool := s3utils.GetAWSDetailsFromEnv()
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

	_, err := backupDriver.CreateBackupLocation(ctx, bLocationCreateReq)
	if err != nil {
		return err
	}
	return nil
}

// CreateAzureBackupLocation creates backuplocation for Azure
func CreateAzureBackupLocation(name string, uid string, cloudCred string, cloudCredUID string, bucketName string, orgID string) error {
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
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return err
	}
	_, err = backupDriver.CreateBackupLocation(ctx, bLocationCreateReq)
	if err != nil {
		return fmt.Errorf("failed to create backup location Error: %v", err)
	}
	return nil
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
		backupScheduleName, SourceClusterName, OrgID), func() {
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

		clusterReq := &api.ClusterInspectRequest{OrgId: OrgID, Name: SourceClusterName, IncludeSecrets: true}
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

// AddLabelToResource adds a label to a resource and errors if the resource type is not implemented
func AddLabelToResource(spec interface{}, key string, val string) error {
	if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
		if obj.Labels == nil {
			obj.Labels = make(map[string]string)
		}
		log.Infof("Adding label [%s=%s] to PVC %s", key, val, obj.Name)
		obj.Labels[key] = val
		core.Instance().UpdatePersistentVolumeClaim(obj)
		return nil
	} else if obj, ok := spec.(*v1.ConfigMap); ok {
		if obj.Labels == nil {
			obj.Labels = make(map[string]string)
		}
		log.Infof("Adding label [%s=%s] to ConfigMap %s", key, val, obj.Name)
		obj.Labels[key] = val
		core.Instance().UpdateConfigMap(obj)
		return nil
	} else if obj, ok := spec.(*v1.Secret); ok {
		if obj.Labels == nil {
			obj.Labels = make(map[string]string)
		}
		log.Infof("Adding label [%s=%s] to Secret %s", key, val, obj.Name)
		obj.Labels[key] = val
		core.Instance().UpdateSecret(obj)
		return nil
	}
	return fmt.Errorf("spec is of unknown resource type")
}

// GetSourceClusterConfigPath returns kubeconfig for source
func GetSourceClusterConfigPath() (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return "", fmt.Errorf("Failed to get source config path. Empty KUBECONFIGS environment variable")
	}

	kubeconfigList := strings.Split(kubeconfigs, ",")
	if len(kubeconfigList) < 2 {
		return "", fmt.Errorf(`Failed to get source config path.
				At least minimum two kubeconfigs required but has %d`, len(kubeconfigList))
	}

	log.Infof("Source config path: %s", fmt.Sprintf("%s/%s", KubeconfigDirectory, kubeconfigList[0]))
	return fmt.Sprintf("%s/%s", KubeconfigDirectory, kubeconfigList[0]), nil
}

// GetDestinationClusterConfigPath get cluster config of destination cluster
func GetDestinationClusterConfigPath() (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return "", fmt.Errorf("empty KUBECONFIGS environment variable")
	}

	kubeconfigList := strings.Split(kubeconfigs, ",")
	if len(kubeconfigList) < 2 {
		return "", fmt.Errorf(`Failed to get source config path.
				At least minimum two kubeconfigs required but has %d`, len(kubeconfigList))
	}

	log.Infof("Destination config path: %s", fmt.Sprintf("%s/%s", KubeconfigDirectory, kubeconfigList[1]))
	return fmt.Sprintf("%s/%s", KubeconfigDirectory, kubeconfigList[1]), nil
}

// GetAzureCredsFromEnv get creds for azure
func GetAzureCredsFromEnv() (tenantID, clientID, clientSecret, subscriptionID, accountName, accountKey string) {
	accountName = os.Getenv("AZURE_ACCOUNT_NAME")
	expect(accountName).NotTo(equal(""),
		"AZURE_ACCOUNT_NAME Environment variable should not be empty")

	accountKey = os.Getenv("AZURE_ACCOUNT_KEY")
	expect(accountKey).NotTo(equal(""),
		"AZURE_ACCOUNT_KEY Environment variable should not be empty")

	log.Infof("Create creds for azure")
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
	id, secret, endpoint, s3Region, disableSSLBool := s3utils.GetAWSDetailsFromEnv()
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
	log.Infof("Delete container url %s", urlStr)
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

// HaIncreaseRebootTargetNode repl increase and reboot target node
func HaIncreaseRebootTargetNode(event *EventRecord, ctx *scheduler.Context, v *volume.Volume, storageNodeMap map[string]node.Node) {

	stepLog := fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v and reboot target node",
		Inst().V.String(), ctx.App.Key, v)

	Step(stepLog,
		func() {
			log.InfoD(stepLog)
			currRep, err := Inst().V.GetReplicationFactor(v)

			if err != nil {
				err = fmt.Errorf("error getting replication factor for volume %s, Error: %v", v.Name, err)
				log.Error(err)
				UpdateOutcome(event, err)
				return
			}
			//if repl is 3 cannot increase repl for the volume
			if currRep == 3 {
				err = fmt.Errorf("cannot perform repl incease as current repl factor is %d", currRep)
				log.Warn(err)
				UpdateOutcome(event, err)
				return
			}

			replicaSets, err := Inst().V.GetReplicaSets(v)
			if err == nil {
				replicaNodes := replicaSets[0].Nodes
				log.InfoD("Current replica nodes of volume %v are %v", v.Name, replicaNodes)
				var newReplID string
				var newReplNode node.Node

				//selecting the target node for repl increase
				for nID, node := range storageNodeMap {
					nExist := false
					for _, id := range replicaNodes {
						if nID == id {
							nExist = true
							break
						}
					}
					if !nExist {
						newReplID = nID
						newReplNode = node
						poolsUsedSize, err := Inst().V.GetPoolsUsedSize(&newReplNode)
						if err != nil {
							UpdateOutcome(event, err)
							return
						}
						pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
						if err != nil {
							UpdateOutcome(event, err)
							return
						}
						for p, u := range poolsUsedSize {
							listPool := pools[p]
							usedSize, err := strconv.ParseUint(u, 10, 64)
							if err != nil {
								UpdateOutcome(event, err)
								return
							}
							freeSize := listPool.TotalSize - usedSize
							vol, err := Inst().V.InspectVolume(v.ID)
							if err != nil {
								UpdateOutcome(event, err)
								return
							}
							if freeSize >= vol.Usage {
								break
							}
						}
					}
				}
				if newReplID != "" {

					stepLog = fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v)
					Step(stepLog,
						func() {
							log.InfoD(stepLog)
							if strings.Contains(ctx.App.Key, fastpathAppName) {
								defer Inst().S.RemoveLabelOnNode(newReplNode, k8s.NodeType)
								Inst().S.AddLabelOnNode(newReplNode, k8s.NodeType, k8s.FastpathNodeType)

							}
							log.InfoD("Increasing repl with target node  [%v]", newReplID)
							err = Inst().V.SetReplicationFactor(v, currRep+1, []string{newReplID}, nil, false)
							if err != nil {
								log.Errorf("There is an error increasing repl [%v]", err.Error())
								UpdateOutcome(event, err)
							}
						})

					if err == nil {
						stepLog = fmt.Sprintf("reboot target node %s while repl increase is in-progres",
							newReplNode.Hostname)
						Step(stepLog,
							func() {
								log.InfoD(stepLog)
								log.Info("Waiting for 10 seconds for re-sync to initialize before target node reboot")
								time.Sleep(10 * time.Second)

								err = Inst().N.RebootNode(newReplNode, node.RebootNodeOpts{
									Force: true,
									ConnectionOpts: node.ConnectionOpts{
										Timeout:         1 * time.Minute,
										TimeBeforeRetry: 5 * time.Second,
									},
								})
								if err != nil {
									log.Errorf("error rebooting node %v, Error: %v", newReplNode.Name, err)
									UpdateOutcome(event, err)
								}

								err = ValidateReplFactorUpdate(v, currRep+1)
								if err != nil {
									err = fmt.Errorf("error in ha-increse after  target node reboot. Error: %v", err)
									log.Error(err)
									UpdateOutcome(event, err)
								} else {
									dash.VerifySafely(true, true, fmt.Sprintf("repl successfully increased to %d", currRep+1))
								}
								if strings.Contains(ctx.App.Key, fastpathAppName) {
									err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_INACTIVE)
									UpdateOutcome(event, err)
									err = Inst().V.SetReplicationFactor(v, currRep-1, nil, nil, true)
								}
							})
					}
				} else {
					UpdateOutcome(event, fmt.Errorf("no node identified to repl increase for vol: %s", v.Name))
				}
			} else {
				log.Error(err)
				UpdateOutcome(event, err)

			}
		})
}

// HaIncreaseRebootSourceNode repl increase and reboot source node
func HaIncreaseRebootSourceNode(event *EventRecord, ctx *scheduler.Context, v *volume.Volume, storageNodeMap map[string]node.Node) {
	stepLog := fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v and reboot source node",
		Inst().V.String(), ctx.App.Key, v)
	Step(stepLog,
		func() {
			log.InfoD(stepLog)
			currRep, err := Inst().V.GetReplicationFactor(v)
			if err != nil {
				err = fmt.Errorf("error getting replication factor for volume %s, Error: %v", v.Name, err)
				log.Error(err)
				UpdateOutcome(event, err)
				return
			}

			//if repl is 3 cannot increase repl for the volume
			if currRep == 3 {
				err = fmt.Errorf("cannot perform repl incease as current repl factor is %d", currRep)
				log.Warn(err)
				UpdateOutcome(event, err)
				return
			}

			if err == nil {
				stepLog = fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v",
					Inst().V.String(), ctx.App.Key, v)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						replicaSets, err := Inst().V.GetReplicaSets(v)
						if err == nil {
							replicaNodes := replicaSets[0].Nodes
							if strings.Contains(ctx.App.Key, fastpathAppName) {
								newFastPathNode, err := AddFastPathLabel(ctx)
								if err == nil {
									defer Inst().S.RemoveLabelOnNode(*newFastPathNode, k8s.NodeType)
								}
								UpdateOutcome(event, err)
							}
							err = Inst().V.SetReplicationFactor(v, currRep+1, nil, nil, false)
							if err != nil {
								log.Errorf("There is an error increasing repl [%v]", err.Error())
								UpdateOutcome(event, err)
							} else {
								log.Info("Waiting for 10 seconds for re-sync to initialize before source nodes reboot")
								time.Sleep(10 * time.Second)
								//rebooting source nodes one by one
								for _, nID := range replicaNodes {
									replNodeToReboot := storageNodeMap[nID]
									err = Inst().N.RebootNode(replNodeToReboot, node.RebootNodeOpts{
										Force: true,
										ConnectionOpts: node.ConnectionOpts{
											Timeout:         1 * time.Minute,
											TimeBeforeRetry: 5 * time.Second,
										},
									})
									if err != nil {
										log.Errorf("error rebooting node %v, Error: %v", replNodeToReboot.Name, err)
										UpdateOutcome(event, err)
									}
								}
								err = ValidateReplFactorUpdate(v, currRep+1)
								if err != nil {
									err = fmt.Errorf("error in ha-increse after  source node reboot. Error: %v", err)
									log.Error(err)
									UpdateOutcome(event, err)
								} else {
									dash.VerifySafely(true, true, fmt.Sprintf("repl successfully increased to %d", currRep+1))
								}
								if strings.Contains(ctx.App.Key, fastpathAppName) {
									err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_INACTIVE)
									UpdateOutcome(event, err)
									err = Inst().V.SetReplicationFactor(v, currRep-1, nil, nil, true)
								}
							}
						} else {
							err = fmt.Errorf("error getting relicasets for volume %s, Error: %v", v.Name, err)
							log.Error(err)
							UpdateOutcome(event, err)
						}

					})
			} else {
				err = fmt.Errorf("error getting current replication factor for volume %s, Error: %v", v.Name, err)
				log.Error(err)
				UpdateOutcome(event, err)
			}

		})
}

func AddFastPathLabel(ctx *scheduler.Context) (*node.Node, error) {
	sNodes := node.GetStorageDriverNodes()
	appNodes, err := Inst().S.GetNodesForApp(ctx)
	if err == nil {
		appNode := appNodes[0]
		for _, n := range sNodes {
			if n.Name != appNode.Name {
				Inst().S.AddLabelOnNode(n, k8s.NodeType, k8s.FastpathNodeType)
				return &n, nil
			}
		}
	}
	return nil, err
}

func ValidateReplFactorUpdate(v *volume.Volume, expaectedReplFactor int64) error {
	t := func() (interface{}, bool, error) {
		err := Inst().V.WaitForReplicationToComplete(v, expaectedReplFactor, validateReplicationUpdateTimeout)
		if err != nil {
			statusErr, _ := status.FromError(err)
			if statusErr.Code() == codes.NotFound || strings.Contains(err.Error(), "code = NotFound") {
				return nil, false, err
			}
			return nil, true, err
		}
		return 0, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, validateReplicationUpdateTimeout, defaultRetryInterval); err != nil {
		return fmt.Errorf("failed to set replication factor of the volume: %v due to err: %v", v.Name, err.Error())
	}
	return nil
}

// CreateBucket creates bucket on the appropriate cloud platform
func CreateBucket(provider string, bucketName string) {
	Step(fmt.Sprintf("Create bucket [%s]", bucketName), func() {
		switch provider {
		case drivers.ProviderAws:
			CreateS3Bucket(bucketName, false, 0, "")
		case drivers.ProviderAzure:
			CreateAzureBucket(bucketName)
		}
	})
}

// CreateS3Bucket creates bucket in S3
func CreateS3Bucket(bucketName string, objectLock bool, retainCount int64, objectLockMode string) error {
	id, secret, endpoint, s3Region, disableSSLBool := s3utils.GetAWSDetailsFromEnv()
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

	if retainCount > 0 && objectLock == true {
		// Create object locked bucket
		_, err = S3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket:                     aws.String(bucketName),
			ObjectLockEnabledForBucket: aws.Bool(true),
		})
	} else {
		// Create standard bucket
		_, err = S3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
	}
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to create bucket [%v]. Error: [%v]", bucketName, err))

	err = S3Client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to wait for bucket [%v] to get created. Error: [%v]", bucketName, err))

	if retainCount > 0 && objectLock == true {
		// Update ObjectLockConfigureation to bucket
		enabled := "Enabled"
		_, err = S3Client.PutObjectLockConfiguration(&s3.PutObjectLockConfigurationInput{
			Bucket: aws.String(bucketName),
			ObjectLockConfiguration: &s3.ObjectLockConfiguration{
				ObjectLockEnabled: aws.String(enabled),
				Rule: &s3.ObjectLockRule{
					DefaultRetention: &s3.DefaultRetention{
						Days: aws.Int64(retainCount),
						Mode: aws.String(objectLockMode)}}}})
		if err != nil {
			err = fmt.Errorf("Failed to update Objectlock config with Retain Count [%v] and Mode [%v]. Error: [%v]", retainCount, objectLockMode, err)
		}
	}
	return err
}

// CreateAzureBucket creates bucket in Azure
func CreateAzureBucket(bucketName string) {
	// From the Azure portal, get your Storage account blob service URL endpoint.
	_, _, _, _, accountName, accountKey := GetAzureCredsFromEnv()

	urlStr := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, bucketName)
	log.Infof("Create container url %s", urlStr)
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

func dumpKubeConfigs(configObject string, kubeconfigList []string) error {
	log.Infof("dump kubeconfigs to file system")
	cm, err := core.Instance().GetConfigMap(configObject, "default")
	if err != nil {
		log.Errorf("Error reading config map: %v", err)
		return err
	}
	log.Infof("Get over kubeconfig list %v", kubeconfigList)
	for _, kubeconfig := range kubeconfigList {
		config := cm.Data[kubeconfig]
		if len(config) == 0 {
			configErr := fmt.Sprintf("Error reading kubeconfig: found empty %s in config map %s",
				kubeconfig, configObject)
			return fmt.Errorf(configErr)
		}
		filePath := fmt.Sprintf("%s/%s", KubeconfigDirectory, kubeconfig)
		log.Infof("Save kubeconfig to %s", filePath)
		err := ioutil.WriteFile(filePath, []byte(config), 0644)
		if err != nil {
			return err
		}
	}
	return nil
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
	M                                   monitor.Driver
	SpecDir                             string
	AppList                             []string
	SecureAppList                       []string
	LogLoc                              string
	LogLevel                            string
	Logger                              *logrus.Logger
	GlobalScaleFactor                   int
	StorageDriverUpgradeEndpointURL     string
	StorageDriverUpgradeEndpointVersion string
	UpgradeStorageDriverEndpointList    string
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
	TopologyLabels                      []map[string]string
	Backup                              backup.Driver
	SecretType                          string
	PureVolumes                         bool
	PureSANType                         string
	RunCSISnapshotAndRestoreManyTest    bool
	VaultAddress                        string
	VaultToken                          string
	SchedUpgradeHops                    string
	AutopilotUpgradeImage               string
	CsiGenericDriverConfigMap           string
	HelmValuesConfigMap                 string
	IsHyperConverged                    bool
	Dash                                *aetosutil.Dashboard
	JobName                             string
	JobType                             string
	PortworxPodRestartCheck             bool
}

// ParseFlags parses command line flags
func ParseFlags() {
	var err error

	var s, m, n, v, backupDriverName, specDir, logLoc, logLevel, appListCSV, secureAppsCSV, repl1AppsCSV, provisionerName, configMapName string
	var schedulerDriver scheduler.Driver
	var volumeDriver volume.Driver
	var nodeDriver node.Driver
	var monitorDriver monitor.Driver
	var backupDriver backup.Driver
	var appScaleFactor int
	var volUpgradeEndpointURL string
	var volUpgradeEndpointVersion string
	var upgradeStorageDriverEndpointList string
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
	var hyperConverged bool
	var enableDash bool
	var pxPodRestartCheck bool

	// TODO: We rely on the customAppConfig map to be passed into k8s.go and stored there.
	// We modify this map from the tests and expect that the next RescanSpecs will pick up the new custom configs.
	// We should make this more robust.
	var customAppConfig map[string]scheduler.AppConfig = make(map[string]scheduler.AppConfig)

	var enableStorkUpgrade bool
	var secretType string
	var pureVolumes bool
	var pureSANType string
	var runCSISnapshotAndRestoreManyTest bool
	var vaultAddress string
	var vaultToken string
	var schedUpgradeHops string
	var autopilotUpgradeImage string
	var csiGenericDriverConfigMapName string
	//dashboard fields
	var user, testBranch, testProduct, testType, testDescription, testTags string
	var testsetID int
	var torpedoJobName string
	var torpedoJobType string

	flag.StringVar(&s, schedulerCliFlag, defaultScheduler, "Name of the scheduler to use")
	flag.StringVar(&n, nodeDriverCliFlag, defaultNodeDriver, "Name of the node driver to use")
	flag.StringVar(&m, monitorDriverCliFlag, defaultMonitorDriver, "Name of the prometheus driver to use")
	flag.StringVar(&v, storageDriverCliFlag, defaultStorageDriver, "Name of the storage driver to use")
	flag.StringVar(&torpedoJobName, torpedoJobNameFlag, defaultTorpedoJob, "Name of the torpedo job")
	flag.StringVar(&torpedoJobType, torpedoJobTypeFlag, defaultTorpedoJobType, "Type of torpedo job")
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
	flag.StringVar(&upgradeStorageDriverEndpointList, upgradeStorageDriverEndpointListFlag, "", "Comma separated list of Spec Generator URLs for performing upgrade hops for StorageCluster")
	flag.BoolVar(&enableStorkUpgrade, enableStorkUpgradeFlag, false, "Enable stork upgrade during storage driver upgrade")
	flag.StringVar(&appListCSV, appListCliFlag, "", "Comma-separated list of apps to run as part of test. The names should match directories in the spec dir.")
	flag.StringVar(&secureAppsCSV, secureAppsCliFlag, "", "Comma-separated list of apps to deploy with secure volumes using storage class. The names should match directories in the spec dir.")
	flag.StringVar(&repl1AppsCSV, repl1AppsCliFlag, "", "Comma-separated list of apps to deploy with repl 1 volumes. The names should match directories in the spec dir.")
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
	flag.StringVar(&pureSANType, "pure-san-type", "ISCSI", "If using Pure volumes, which SAN type is being used. ISCSI, FC, and NVMEOF-RDMA are all valid values.")
	flag.BoolVar(&runCSISnapshotAndRestoreManyTest, "pure-fa-snapshot-restore-to-many-test", false, "If using Pure volumes, to enable Pure clone many tests")
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
	flag.StringVar(&jiraUserName, jiraUserNameFlag, "", "Username to be used for JIRA client")
	flag.StringVar(&jiraToken, jiraTokenFlag, "", "API token for accessing the JIRA")
	flag.StringVar(&jirautils.AccountID, jiraAccountIDFlag, "", "AccountID for issue assignment")
	flag.BoolVar(&hyperConverged, hyperConvergedFlag, true, "To enable/disable hyper-converged type of deployment")
	flag.BoolVar(&enableDash, enableDashBoardFlag, true, "To enable/disable aetos dashboard reporting")
	flag.StringVar(&user, userFlag, "nouser", "user name running the tests")
	flag.StringVar(&testDescription, testDescriptionFlag, "Torpedo Workflows", "test suite description")
	flag.StringVar(&testType, testTypeFlag, "system-test", "test types like system-test,functional,integration")
	flag.StringVar(&testTags, testTagsFlag, "", "tags running the tests. Eg: key1:val1,key2:val2")
	flag.IntVar(&testsetID, testSetIDFlag, 0, "testset id to post the results")
	flag.StringVar(&testBranch, testBranchFlag, "master", "branch of the product")
	flag.StringVar(&testProduct, testProductFlag, "PxEnp", "Portworx product under test")
	flag.StringVar(&pxRuntimeOpts, "px-runtime-opts", "", "comma separated list of run time options for cluster update")
	flag.BoolVar(&pxPodRestartCheck, failOnPxPodRestartCount, false, "Set it true for px pods restart check during test")
	flag.Parse()

	log.SetLoglevel(logLevel)
	tpLogPath = fmt.Sprintf("%s/%s", logLoc, "torpedo.log")
	suiteLogger = CreateLogger(tpLogPath)
	log.SetTorpedoFileOutput(suiteLogger)

	appList, err := splitCsv(appListCSV)
	if err != nil {
		log.Fatalf("failed to parse app list: %v. err: %v", appListCSV, err)
	}

	secureAppList := make([]string, 0)

	if secureAppsCSV == "all" {
		secureAppList = append(secureAppList, appList...)
	} else if len(secureAppsCSV) > 0 {
		apl, err := splitCsv(secureAppsCSV)
		log.FailOnError(err, fmt.Sprintf("failed to parse secure app list: %v", secureAppsCSV))
		secureAppList = append(secureAppList, apl...)
		log.Infof("Secure apps : %+v", secureAppList)
		//Adding secure apps as part of app list for deployment
		appList = append(appList, secureAppList...)
	}

	repl1AppList := make([]string, 0)

	if repl1AppsCSV == "all" {
		repl1AppList = append(repl1AppList, appList...)
	} else if len(repl1AppsCSV) > 0 {
		apl, err := splitCsv(repl1AppsCSV)
		log.FailOnError(err, fmt.Sprintf("failed to parse secure app list: %v", repl1AppsCSV))
		repl1AppList = append(repl1AppList, apl...)
		log.Infof("volume repl 1  apps : %+v", secureAppList)
		//Adding repl 1 apps as part of app list for deployment
		appList = append(appList, repl1AppList...)
	}

	sched.Init(time.Second)

	if schedulerDriver, err = scheduler.Get(s); err != nil {
		log.Fatalf("Cannot find scheduler driver for %v. Err: %v\n", s, err)
	} else if volumeDriver, err = volume.Get(v); err != nil {
		log.Fatalf("Cannot find volume driver for %v. Err: %v\n", v, err)
	} else if nodeDriver, err = node.Get(n); err != nil {
		log.Fatalf("Cannot find node driver for %v. Err: %v\n", n, err)
	} else if monitorDriver, err = monitor.Get(m); err != nil {
		log.Fatalf("Cannot find monitor driver for %v. Err: %v\n", m, err)
	} else if err = os.MkdirAll(logLoc, os.ModeDir); err != nil {
		log.Fatalf("Cannot create path %s for saving support bundle. Error: %v", logLoc, err)
	} else {
		if _, err = os.Stat(customConfigPath); err == nil {
			var data []byte

			log.Infof("Using custom app config file %s", customConfigPath)
			data, err = ioutil.ReadFile(customConfigPath)
			if err != nil {
				log.Fatalf("Cannot read file %s. Error: %v", customConfigPath, err)
			}
			err = yaml.Unmarshal(data, &customAppConfig)
			if err != nil {
				log.Fatalf("Cannot unmarshal yml %s. Error: %v", customConfigPath, err)
			}
			log.Infof("Parsed custom app config file: %+v", customAppConfig)
		}
		if len(repl1AppList) > 0 {
			for _, app := range repl1AppList {
				if appConfig, ok := customAppConfig[app]; ok {
					appConfig.Repl = "1"
				} else {
					var config = scheduler.AppConfig{Repl: "1"}
					customAppConfig[app] = config
				}
			}
		}
		log.Infof("Backup driver name %s", backupDriverName)
		if backupDriverName != "" {
			if backupDriver, err = backup.Get(backupDriverName); err != nil {
				log.Fatalf("cannot find backup driver for %s. Err: %v\n", backupDriverName, err)
			} else {
				log.Infof("Backup driver found %v", backupDriver)
			}
		}
		dash = aetosutil.Get()
		if enableDash && !isDashboardReachable() {
			enableDash = false
			log.Infof("Aetos Dashboard is not reachable. Disabling dashboard reporting.")
		}

		dash.IsEnabled = enableDash
		testSet := aetosutil.TestSet{
			User:        user,
			Product:     testProduct,
			Description: testDescription,
			Branch:      testBranch,
			TestType:    testType,
			Tags:        make(map[string]string),
			Status:      aetosutil.NOTSTARTED,
		}
		if testTags != "" {
			tags, err := splitCsv(testTags)
			if err != nil {
				log.Fatalf("failed to parse tags: %v. err: %v", testTags, err)
			} else {
				for _, tag := range tags {
					var key, value string
					if !strings.Contains(tag, ":") {
						log.Info("Invalid tag %s. Please provide tag in key:value format skipping provided tag", tag)
					} else {
						key = strings.SplitN(tag, ":", 2)[0]
						value = strings.SplitN(tag, ":", 2)[1]
						testSet.Tags[key] = value
					}
				}
			}
		}

		/*
			Get TestSetID based on below precedence
			1. Check if user has passed in command line and use
			2. Check if user has set it has an env variable and use
			3. Check if build.properties available with TestSetID
			4. If not present create a new one
		*/
		val, ok := os.LookupEnv("DASH_UID")
		if testsetID != 0 {
			dash.TestSetID = testsetID
		} else if ok && (val != "" && val != "0") {
			testsetID, err = strconv.Atoi(val)
			log.Infof(fmt.Sprintf("Using TestSetID: %s set as enviornment variable", val))
			if err != nil {
				log.Warnf("Failed to convert environment testset id  %v to int, err: %v", val, err)
			}
		} else {
			fileName := "/build.properties"
			readFile, err := os.Open(fileName)
			if err == nil {
				fileScanner := bufio.NewScanner(readFile)
				fileScanner.Split(bufio.ScanLines)
				for fileScanner.Scan() {
					line := fileScanner.Text()
					if strings.Contains(line, "DASH_UID") {
						testsetToUse := strings.Split(line, "=")[1]
						log.Infof("Using TestSetID: %s found in build.properties", testsetToUse)
						dash.TestSetID, err = strconv.Atoi(testsetToUse)
						if err != nil {
							log.Errorf("Error in getting DASH_UID variable, %v", err)
						}
						break
					}
				}
			}
		}
		if testsetID != 0 {
			dash.TestSetID = testsetID
			os.Setenv("DASH_UID", fmt.Sprint(testsetID))
		}

		dash.TestSet = &testSet

		once.Do(func() {
			instance = &Torpedo{
				InstanceID:                          time.Now().Format("01-02-15h04m05s"),
				S:                                   schedulerDriver,
				V:                                   volumeDriver,
				N:                                   nodeDriver,
				M:                                   monitorDriver,
				SpecDir:                             specDir,
				LogLoc:                              logLoc,
				LogLevel:                            logLevel,
				Logger:                              log.GetLogInstance(),
				GlobalScaleFactor:                   appScaleFactor,
				MinRunTimeMins:                      minRunTimeMins,
				ChaosLevel:                          chaosLevel,
				StorageDriverUpgradeEndpointURL:     volUpgradeEndpointURL,
				StorageDriverUpgradeEndpointVersion: volUpgradeEndpointVersion,
				UpgradeStorageDriverEndpointList:    upgradeStorageDriverEndpointList,
				EnableStorkUpgrade:                  enableStorkUpgrade,
				AppList:                             appList,
				SecureAppList:                       secureAppList,
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
				PureSANType:                         pureSANType,
				RunCSISnapshotAndRestoreManyTest:    runCSISnapshotAndRestoreManyTest,
				VaultAddress:                        vaultAddress,
				VaultToken:                          vaultToken,
				SchedUpgradeHops:                    schedUpgradeHops,
				AutopilotUpgradeImage:               autopilotUpgradeImage,
				CsiGenericDriverConfigMap:           csiGenericDriverConfigMapName,
				LicenseExpiryTimeoutHours:           licenseExpiryTimeoutHours,
				MeteringIntervalMins:                meteringIntervalMins,
				IsHyperConverged:                    hyperConverged,
				Dash:                                dash,
				JobName:                             torpedoJobName,
				JobType:                             torpedoJobType,
				PortworxPodRestartCheck:             pxPodRestartCheck,
			}
		})
	}
	printFlags()
}

func printFlags() {

	log.Info("********Torpedo Command********")
	log.Info(strings.Join(os.Args, " "))
	log.Info("******************************")

	log.Info("*********Parsed Args**********")
	flag.VisitAll(func(f *flag.Flag) {
		log.Infof("   %s: %s", f.Name, f.Value)
	})
	log.Info("******************************")
}

func isDashboardReachable() bool {
	timeout := 15 * time.Second
	client := http.Client{
		Timeout: timeout,
	}
	aboutURL := strings.Replace(aetosutil.DashBoardBaseURL, "dashboard", "datamodel/about", -1)
	log.Infof("Checking URL: %s", aboutURL)
	response, err := client.Get(aboutURL)

	if err != nil {
		log.Warn(err.Error())
		return false
	}
	if response.StatusCode == 200 {
		return true
	}
	return false
}

// CreateLogFile creates file and return the file object
func CreateLogFile(filename string) *os.File {
	var filePath string
	if strings.Contains(filename, "/") {
		filePath = "filename"
	} else {
		filePath = fmt.Sprintf("%s/%s", Inst().LogLoc, filename)
	}

	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Failed to create logfile torpedo.log")
		fmt.Println("Error: ", err)
	}
	return f

}

// CreateLogger creates file and return the file object
func CreateLogger(filename string) *lumberjack.Logger {
	var filePath string
	if strings.Contains(filename, "/") {
		filePath = filename
	} else {
		filePath = fmt.Sprintf("%s/%s", Inst().LogLoc, filename)
	}
	_, err := os.Create(filePath)
	if err != nil {
		log.Infof("Error creating log file. Err: %v", err)
		return nil
	}

	logger := &lumberjack.Logger{
		Filename:   filePath,
		MaxSize:    10, // megabytes
		MaxBackups: 10,
		MaxAge:     30,   //days
		Compress:   true, // disabled by default
		LocalTime:  true,
	}

	return logger

}

// CloseLogger ends testcase file object
func CloseLogger(testLogger *lumberjack.Logger) {
	if testLogger != nil {
		testLogger.Close()
		//Below steps are performed to remove current file from log output
		log.SetDefaultOutput(suiteLogger)
	}

}

func splitCsv(in string) ([]string, error) {
	r := csv.NewReader(strings.NewReader(in))
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil || len(records) < 1 {
		return []string{}, err
	} else if len(records) > 1 {
		return []string{}, fmt.Errorf("multiline CSV not supported")
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

// CreateJiraIssueWithLogs creates a jira issue and copy logs to nfs mount
func CreateJiraIssueWithLogs(issueDescription, issueSummary string) {
	issueKey, err := jirautils.CreateIssue(issueDescription, issueSummary)
	if err == nil && issueKey != "" {
		collectAndCopyDiagsOnWorkerNodes(issueKey)
		collectAndCopyStorkLogs(issueKey)
		collectAndCopyOperatorLogs(issueKey)
		collectAndCopyAutopilotLogs(issueKey)

	}

}

func collectAndCopyDiagsOnWorkerNodes(issueKey string) {
	isIssueDirCreated := false
	for _, currNode := range node.GetWorkerNodes() {
		err := runCmd("pwd", currNode)
		if err == nil {
			log.Infof("Creating directors logs in the node %v", currNode.Name)
			runCmd(fmt.Sprintf("mkdir -p %v", rootLogDir), currNode)
			log.Info("Mounting nfs diags directory")
			runCmd(fmt.Sprintf("mount -t nfs %v %v", diagsDirPath, rootLogDir), currNode)
			if !isIssueDirCreated {
				log.Infof("Creating PTX %v directory in the node %v", issueKey, currNode.Name)
				runCmd(fmt.Sprintf("mkdir -p %v/%v", rootLogDir, issueKey), currNode)
				isIssueDirCreated = true
			}

			log.Infof("collect diags on node: %s", currNode.Name)

			filePath := fmt.Sprintf("/var/cores/%s-diags-*.tar.gz", currNode.Name)

			config := &torpedovolume.DiagRequestConfig{
				DockerHost:    "unix:///var/run/docker.sock",
				OutputFile:    filePath,
				ContainerName: "",
				Profile:       false,
				Live:          false,
				Upload:        false,
				All:           true,
				Force:         true,
				OnHost:        true,
				Extra:         false,
			}
			err = Inst().V.CollectDiags(currNode, config, torpedovolume.DiagOps{Validate: false, Async: true})

			if err == nil {
				log.Infof("copying logs %v  on node: %s", filePath, currNode.Name)
				runCmd(fmt.Sprintf("cp %v %v/%v/", filePath, rootLogDir, issueKey), currNode)
			} else {
				log.Warnf("Error collecting diags on node: %v, Error: %v", currNode.Name, err)
			}

		}
	}
}

func collectAndCopyStorkLogs(issueKey string) {

	storkLabel := make(map[string]string)
	storkLabel["name"] = "stork"
	podList, err := core.Instance().GetPods(pxNamespace, storkLabel)
	if err == nil {
		logsByPodName := map[string]string{}
		for _, p := range podList.Items {
			logOptions := corev1.PodLogOptions{
				// Getting 250 lines from the pod logs to get the io_bytes
				TailLines: getInt64Address(250),
			}
			log.Info("Collecting stork logs")
			output, err := core.Instance().GetPodLog(p.Name, p.Namespace, &logOptions)
			if err != nil {
				log.Error(fmt.Errorf("failed to get logs for the pod %s/%s: %w", p.Namespace, p.Name, err))
			}
			logsByPodName[p.Name] = output
		}
		masterNode := node.GetMasterNodes()[0]
		err = runCmd("pwd", masterNode)
		if err == nil {
			log.Infof("Creating directors logs in the node %v", masterNode.Name)
			runCmd(fmt.Sprintf("mkdir -p %v", rootLogDir), masterNode)
			log.Info("Mounting nfs diags directory")
			runCmd(fmt.Sprintf("mount -t nfs %v %v", diagsDirPath, rootLogDir), masterNode)

			for k, v := range logsByPodName {
				cmnd := fmt.Sprintf("echo '%v' > /root/%v.log", v, k)
				runCmdWithNoSudo(cmnd, masterNode)
				runCmd(fmt.Sprintf("cp /root/%v.log %v/%v/", k, rootLogDir, issueKey), masterNode)
			}
		}

	} else {
		log.Errorf("Error in getting stork pods, Err: %v", err.Error())
	}

}

func collectAndCopyOperatorLogs(issueKey string) {
	podLabel := make(map[string]string)
	podLabel["name"] = "portworx-operator"
	podList, err := core.Instance().GetPods(pxNamespace, podLabel)
	if err == nil {
		logsByPodName := map[string]string{}
		for _, p := range podList.Items {
			logOptions := corev1.PodLogOptions{
				// Getting 250 lines from the pod logs to get the io_bytes
				TailLines: getInt64Address(250),
			}
			log.Info("Collecting portworx operator logs")
			output, err := core.Instance().GetPodLog(p.Name, p.Namespace, &logOptions)
			if err != nil {
				log.Error(fmt.Errorf("failed to get logs for the pod %s/%s: %w", p.Namespace, p.Name, err))
			}
			logsByPodName[p.Name] = output
		}
		masterNode := node.GetMasterNodes()[0]
		err = runCmd("pwd", masterNode)
		if err == nil {
			for k, v := range logsByPodName {
				cmnd := fmt.Sprintf("echo '%v' > /root/%v.log", v, k)
				runCmdWithNoSudo(cmnd, masterNode)
				runCmd(fmt.Sprintf("cp /root/%v.log %v/%v/", k, rootLogDir, issueKey), masterNode)
			}
		}

	} else {
		log.Errorf("Error in getting portworx-operator pods, Err: %v", err.Error())
	}

}

func collectAndCopyAutopilotLogs(issueKey string) {
	podLabel := make(map[string]string)
	podLabel["name"] = "autopilot"
	podList, err := core.Instance().GetPods(pxNamespace, podLabel)
	if err == nil {
		logsByPodName := map[string]string{}
		for _, p := range podList.Items {
			logOptions := corev1.PodLogOptions{
				// Getting 250 lines from the pod logs to get the io_bytes
				TailLines: getInt64Address(250),
			}
			log.Info("Collecting autopilot logs")
			output, err := core.Instance().GetPodLog(p.Name, p.Namespace, &logOptions)
			if err != nil {
				log.Error(fmt.Errorf("failed to get logs for the pod %s/%s: %w", p.Namespace, p.Name, err))
			}
			logsByPodName[p.Name] = output
		}
		masterNode := node.GetMasterNodes()[0]

		err = runCmd("pwd", masterNode)
		if err == nil {
			for k, v := range logsByPodName {
				cmnd := fmt.Sprintf("echo '%v' > /root/%v.log", v, k)
				runCmdWithNoSudo(cmnd, masterNode)
				runCmd(fmt.Sprintf("cp /root/%v.log %v/%v/", k, rootLogDir, issueKey), masterNode)
			}
		}
	} else {
		log.Errorf("Error in getting autopilot pods, Err: %v", err.Error())
	}

}

func getInt64Address(x int64) *int64 {
	return &x
}

// IsCloudDriveInitialised checks if cloud drive is initialised in the PX cluster
func IsCloudDriveInitialised(n node.Node) (bool, error) {

	_, err := Inst().N.RunCommandWithNoRetry(n, pxctlCDListCmd, node.ConnectionOpts{
		Timeout:         2 * time.Minute,
		TimeBeforeRetry: 10 * time.Second,
	})

	if err != nil && strings.Contains(err.Error(), "Cloud Drive is not initialized") {
		log.Warnf("cd list error : %v", err)
		return false, nil
	}
	if err == nil {
		return true, nil
	}
	return false, err
}

// WaitForExpansionToStart waits for pool expansion to trigger
func WaitForExpansionToStart(poolID string) error {
	f := func() (interface{}, bool, error) {
		expandedPool, err := GetStoragePoolByUUID(poolID)

		if err != nil {
			return nil, false, err
		}
		if expandedPool.LastOperation != nil {
			if expandedPool.LastOperation.Status == opsapi.SdkStoragePool_OPERATION_FAILED {
				return nil, false, fmt.Errorf("PoolResize has failed. Error: %s", expandedPool.LastOperation)
			}

			if expandedPool.LastOperation.Status == opsapi.SdkStoragePool_OPERATION_PENDING {
				return nil, true, fmt.Errorf("PoolResize is in pending state [%s]", expandedPool.LastOperation)
			}

			if expandedPool.LastOperation.Status == opsapi.SdkStoragePool_OPERATION_IN_PROGRESS {
				// storage pool resize has been triggered
				log.InfoD("Pool %s expansion started", poolID)
				return nil, false, nil
			}

		}
		return nil, true, fmt.Errorf("pool %s resize not triggered ", poolID)
	}

	_, err := task.DoRetryWithTimeout(f, 2*time.Minute, 10*time.Second)
	return err
}

// RebootNodeAndWait reboots node and waits for to be up
func RebootNodeAndWait(n node.Node) error {

	if &n == nil {
		return fmt.Errorf("no Node is provided to reboot")
	}

	err := Inst().N.RebootNode(n, node.RebootNodeOpts{
		Force: true,
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         1 * time.Minute,
			TimeBeforeRetry: 5 * time.Second,
		},
	})

	if err != nil {
		return err
	}
	err = Inst().N.TestConnection(n, node.ConnectionOpts{
		Timeout:         15 * time.Minute,
		TimeBeforeRetry: 10 * time.Second,
	})
	if err != nil {
		return err
	}
	err = Inst().V.WaitDriverDownOnNode(n)
	if err != nil {
		return err
	}
	err = Inst().S.IsNodeReady(n)
	if err != nil {
		return err
	}
	err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
	if err != nil {
		return err
	}

	return nil

}

// GetNodeWithGivenPoolID returns node having pool id
func GetNodeWithGivenPoolID(poolID string) (*node.Node, error) {
	if err := Inst().V.RefreshDriverEndpoints(); err != nil {
		return nil, err
	}

	pxNodes, err := GetStorageNodes()
	if err != nil {
		return nil, err
	}

	for _, n := range pxNodes {
		pools := n.Pools
		for _, p := range pools {
			if poolID == p.Uuid {
				return &n, nil
			}
		}
	}

	return nil, fmt.Errorf("no storage node found with given Pool UUID : %s", poolID)
}

// GetStoragePoolByUUID reruns storage pool based on ID
func GetStoragePoolByUUID(poolUUID string) (*opsapi.StoragePool, error) {
	pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
	if err != nil {
		return nil, err
	}

	if len(pools) == 0 {
		return nil, fmt.Errorf("Got 0 pools listed")
	}

	pool := pools[poolUUID]
	if pool == nil {
		return nil, fmt.Errorf("unable to find pool with given ID: %s", poolUUID)
	}

	return pool, nil
}

// ValidateUserRole will validate if a given user has the provided PxBackupRole mapped to it
func ValidateUserRole(userName string, role backup.PxBackupRole) (bool, error) {
	roleMapping, err := backup.GetRolesForUser(userName)
	log.FailOnError(err, "Failed to get roles for user")
	roleID, err := backup.GetRoleID(role)
	log.FailOnError(err, "Failed to get role ID")
	for _, r := range roleMapping {
		if r.ID == roleID {
			break
		}
	}
	return true, nil
}

func Contains(app_list []string, app string) bool {
	for _, v := range app_list {
		if v == app {
			return true
		}
	}
	return false
}

// ValidatePoolRebalance checks rebalnce state of pools if running
func ValidatePoolRebalance(stNode node.Node, poolID int32) error {

	rebalanceFunc := func() (interface{}, bool, error) {

		rebalanceJobs, err := Inst().V.GetRebalanceJobs()
		if err != nil {
			return nil, true, err
		}

		for _, job := range rebalanceJobs {
			jobResponse, err := Inst().V.GetRebalanceJobStatus(job.GetId())

			if err != nil {
				return nil, true, err
			}

			previousDone := uint64(0)
			jobState := jobResponse.GetJob().GetState()
			if jobState == opsapi.StorageRebalanceJobState_CANCELLED {
				return nil, false, fmt.Errorf("job %v has cancelled, Summary: %+v", job.GetId(), jobResponse.GetSummary().GetWorkSummary())
			}

			if jobState == opsapi.StorageRebalanceJobState_PAUSED || jobState == opsapi.StorageRebalanceJobState_PENDING {
				return nil, true, fmt.Errorf("Job %v is in paused/pending state", job.GetId())
			}

			if jobState == opsapi.StorageRebalanceJobState_DONE {
				log.InfoD("Job %v is in DONE state", job.GetId())
				return nil, false, nil
			}

			if jobState == opsapi.StorageRebalanceJobState_RUNNING {
				log.InfoD("Job %v is in Running state", job.GetId())

				currentDone, total := getReblanceWorkSummary(jobResponse)
				//checking for rebalance progress
				for currentDone < total && previousDone < currentDone {
					time.Sleep(2 * time.Minute)
					log.InfoD("Waiting for job %v to complete current state: %v, checking again in 2 minutes", job.GetId(), jobState)
					jobResponse, err = Inst().V.GetRebalanceJobStatus(job.GetId())
					if err != nil {
						return nil, true, err
					}
					previousDone = currentDone
					currentDone, total = getReblanceWorkSummary(jobResponse)
				}

				if previousDone == currentDone {
					return nil, false, fmt.Errorf("job %v is in running state but not progressing further", job.GetId())
				}
				if currentDone == total {
					log.InfoD("Rebalance for job %v completed", job.GetId())
					return nil, false, nil
				}
			}
		}
		return nil, false, nil
	}

	_, err := task.DoRetryWithTimeout(rebalanceFunc, time.Minute*60, time.Minute*2)
	if err != nil {
		return err
	}

	nodePoolsToValidate := make([]node.StoragePool, 0)
	if poolID != -1 {
		for _, p := range stNode.StoragePools {
			if p.ID == poolID {
				nodePoolsToValidate = append(nodePoolsToValidate, p)
				break
			}
		}
	} else {
		//A new pool might be created due to add drive,hence 2 min wait for pool to associate with node
		time.Sleep(2 * time.Minute)
		err = Inst().V.RefreshDriverEndpoints()
		log.FailOnError(err, "error refreshing end points")
		for _, n := range node.GetStorageNodes() {
			if n.Id == stNode.Id {
				stNode = n
				break
			}
		}
		nodePoolsToValidate = append(nodePoolsToValidate, stNode.StoragePools...)

	}

	for _, nodePool := range nodePoolsToValidate {
		currentLastMsg := ""
		f := func() (interface{}, bool, error) {
			expandedPool, err := GetStoragePoolByUUID(nodePool.Uuid)
			if err != nil {
				return nil, true, fmt.Errorf("error getting pool by using id %s from node %s", nodePool.Uuid, stNode.Name)
			}

			if expandedPool == nil {
				return nil, false, fmt.Errorf("expanded pool value is nil")
			}
			if expandedPool.LastOperation != nil {
				log.Infof("Node [%s] Pool [%s] Status : %v, Message : %s", stNode.Name, nodePool.Uuid, expandedPool.LastOperation.Status, expandedPool.LastOperation.Msg)
				if expandedPool.LastOperation.Status == opsapi.SdkStoragePool_OPERATION_FAILED {
					return nil, false, fmt.Errorf("Pool is failed state. Error: %s", expandedPool.LastOperation)
				}
				if expandedPool.LastOperation.Status == opsapi.SdkStoragePool_OPERATION_IN_PROGRESS {
					if strings.Contains(expandedPool.LastOperation.Msg, "Rebalance in progress") || strings.Contains(expandedPool.LastOperation.Msg, "rebalance is running") {
						if currentLastMsg == expandedPool.LastOperation.Msg {
							return nil, false, fmt.Errorf("pool reblance is not progressing")
						} else {
							currentLastMsg = expandedPool.LastOperation.Msg
							return nil, true, fmt.Errorf("wait for pool rebalance to complete")
						}
					}
					if strings.Contains(expandedPool.LastOperation.Msg, "No pending operation pool status: Maintenance") {
						return nil, false, nil
					}
				}
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(f, time.Minute*180, time.Minute*2)
	}
	return err
}

func getReblanceWorkSummary(jobResponse *opsapi.SdkGetRebalanceJobStatusResponse) (uint64, uint64) {
	status := jobResponse.GetJob().GetStatus()
	if status != "" {
		log.Infof(" Job Status: %s", status)
	}

	currentDone := uint64(0)
	currentPending := uint64(0)
	total := uint64(0)
	rebalWorkSummary := jobResponse.GetSummary().GetWorkSummary()

	for _, summary := range rebalWorkSummary {
		currentDone += summary.GetDone()
		currentPending += summary.GetPending()
		log.Infof("WorkSummary --> Type: %v,Done : %v, Pending: %v", summary.GetType(), currentDone, currentPending)

	}
	total = currentDone + currentPending

	return currentDone, total
}

func updatePxRuntimeOpts() error {
	if pxRuntimeOpts != "" {
		log.InfoD("Setting run time options: %s", pxRuntimeOpts)
		optionsMap := make(map[string]string)
		runtimeOpts, err := splitCsv(pxRuntimeOpts)
		log.FailOnError(err, "Error parsing run time options")

		for _, opt := range runtimeOpts {
			if !strings.Contains(opt, "=") {
				log.Fatalf("Given run time option is not in expected format key=val, Actual : %v", opt)
			}
			optArr := strings.Split(opt, "=")
			optionsMap[optArr[0]] = optArr[1]
		}
		currNode := node.GetWorkerNodes()[0]
		return Inst().V.SetClusterRunTimeOpts(currNode, optionsMap)
	} else {
		log.Info("No run time options provided to update")
	}
	return nil

}

// GetCloudDriveDeviceSpecs returns Cloud drive specs on the storage cluster
func GetCloudDriveDeviceSpecs() ([]string, error) {
	log.InfoD("Getting cloud drive specs")
	deviceSpecs := make([]string, 0)
	IsOperatorBasedInstall, err := Inst().V.IsOperatorBasedInstall()
	if err != nil {
		return deviceSpecs, err
	}

	if !IsOperatorBasedInstall {
		return deviceSpecs, fmt.Errorf("it is not operator based install, cannot get device spec")
	}
	stc, err := Inst().V.GetDriver()
	if err != nil {
		return deviceSpecs, err
	}
	deviceSpecs = *stc.Spec.CloudStorage.DeviceSpecs
	return deviceSpecs, nil
}

// StartTorpedoTest starts the logging for torpedo test
func StartTorpedoTest(testName, testDescription string, tags map[string]string, testRepoID int) {
	TestLogger = CreateLogger(fmt.Sprintf("%s.log", testName))
	log.SetTorpedoFileOutput(TestLogger)
	if tags == nil {
		tags = make(map[string]string, 0)
	}
	tags["apps"] = strings.Join(Inst().AppList, ",")
	tags["storageProvisioner"] = Inst().Provisioner
	tags["pureVolume"] = fmt.Sprintf("%t", Inst().PureVolumes)
	tags["pureSANType"] = Inst().PureSANType
	dash.TestCaseBegin(testName, testDescription, strconv.Itoa(testRepoID), tags)
}

// enableAutoFSTrim on supported PX version.
func EnableAutoFSTrim() {
	nodes := node.GetWorkerNodes()
	var isPXNodeAvailable bool
	for _, pxNode := range nodes {
		isPxInstalled, err := Inst().V.IsDriverInstalled(pxNode)
		if err != nil {
			log.Debugf("Could not get PX status on %s", pxNode.Name)
		}
		if isPxInstalled {
			isPXNodeAvailable = true
			pxVersion, err := Inst().V.GetDriverVersionOnNode(pxNode)

			log.FailOnError(err, "Unable to get driver version on node [%s]", pxNode.Name)
			log.Infof("PX version %s", pxVersion)
			pxVersionList := []string{}
			pxVersionList = strings.Split(pxVersion, ".")
			majorVer, err := strconv.Atoi(pxVersionList[0])
			minorVer, err := strconv.Atoi(pxVersionList[1])
			if majorVer < 2 || (majorVer == 2 && minorVer < 10) {
				log.Warnf("Auto FSTrim cannot be enabled on PX version %s", pxVersion)
			} else {
				err = Inst().V.SetClusterOpts(pxNode, map[string]string{
					"--auto-fstrim": "on"})
				log.FailOnError(err, "Autofstrim is enabled on the cluster ?")
				log.Infof("Auto FSTrim enabled on the cluster")
			}
			break
		}
	}
	dash.VerifyFatal(isPXNodeAvailable, true, "No PX node available in the cluster")
}

// EndTorpedoTest ends the logging for torpedo test
func EndTorpedoTest() {
	CloseLogger(TestLogger)
	dash.TestCaseEnd()
}

func CreateMultiVolumesAndAttach(wg *sync.WaitGroup, count int, nodeName string) (map[string]string, error) {
	createdVolIDs := make(map[string]string)
	defer wg.Done()
	for count > 0 {
		volName := fmt.Sprintf("%s-%d", VolumeCreatePxRestart, count)
		log.Infof("Creating volume : %s", volName)
		volCreateRequest := &opsapi.SdkVolumeCreateRequest{
			Name: volName,
			Spec: &opsapi.VolumeSpec{
				Size:    1000,
				HaLevel: 1,
				Format:  opsapi.FSType_FS_TYPE_EXT4,
				ReplicaSet: &opsapi.ReplicaSet{
					Nodes: []string{nodeName},
				},
			}}
		t := func() (interface{}, bool, error) {
			out, err := Inst().V.CreateVolumeUsingRequest(volCreateRequest)
			return out, true, err
		}

		out, err := task.DoRetryWithTimeout(t, 5*time.Minute, 30*time.Second)

		var volPath string
		var volId string
		if err == nil {
			volId = fmt.Sprintf("%v", out)
			log.Infof("Volume %s created", volId)
			t := func() (interface{}, bool, error) {
				out, err := Inst().V.AttachVolume(volId)
				return out, true, err
			}
			out, err = task.DoRetryWithTimeout(t, 5*time.Minute, 30*time.Second)
		}
		if err != nil {
			return createdVolIDs, fmt.Errorf("failed to creared volume %s, due to error : %v ", volName, err)
		}
		volPath = fmt.Sprintf("%v", out)
		createdVolIDs[volId] = volPath
		log.Infof("Volume %s attached to path %s", volId, volPath)
		count--
	}
	return createdVolIDs, nil
}

// GetPoolIDWithIOs returns the pools with IOs happening
func GetPoolIDWithIOs(contexts []*scheduler.Context) (string, error) {
	// pick a  pool doing some IOs from a pools list
	var err error
	err = Inst().V.RefreshDriverEndpoints()
	if err != nil {
		return "", err
	}

	for _, ctx := range contexts {
		vols, err := Inst().S.GetVolumes(ctx)
		if err != nil {
			return "", err
		}

		node := node.GetStorageDriverNodes()[0]
		for _, vol := range vols {
			appVol, err := Inst().V.InspectVolume(vol.ID)
			if err != nil {
				return "", err
			}
			isIOsInProgress, err := Inst().V.IsIOsInProgressForTheVolume(&node, appVol.Id)
			if err != nil {
				return "", err
			}
			if isIOsInProgress {
				log.Infof("IOs are in progress for [%v]", vol.Name)
				poolUuids := appVol.ReplicaSets[0].PoolUuids
				for _, p := range poolUuids {
					n, err := GetNodeWithGivenPoolID(p)
					if err != nil {
						return "", err
					}
					eligibilityMap, err := GetPoolExpansionEligibility(n)
					if err != nil {
						return "", err
					}
					if eligibilityMap[n.Id] && eligibilityMap[p] {
						return p, nil
					}

				}
			}
		}

	}

	return "", fmt.Errorf("no pools have IOs running,Err: %v", err)
}

// GetPoolWithIOsInGivenNode returns the poolID in the given node with IOs happening
func GetPoolWithIOsInGivenNode(stNode node.Node, contexts []*scheduler.Context) (*opsapi.StoragePool, error) {

	eligibilityMap, err := GetPoolExpansionEligibility(&stNode)
	if err != nil {
		return nil, err
	}

	if !eligibilityMap[stNode.Id] {
		return nil, fmt.Errorf("node [%s] is not eligible for expansion", stNode.Name)
	}
	nodePools := make([]string, 0)
	for _, np := range stNode.StoragePools {
		nodePools = append(nodePools, np.Uuid)
	}

	var selectedNodePoolID string
	var selectedPool *opsapi.StoragePool

outer:
	for _, ctx := range contexts {
		vols, err := Inst().S.GetVolumes(ctx)
		if err != nil {
			return nil, err
		}

		for _, vol := range vols {
			appVol, err := Inst().V.InspectVolume(vol.ID)
			if err != nil {
				return nil, err
			}
			isIOsInProgress, err := Inst().V.IsIOsInProgressForTheVolume(&stNode, appVol.Id)
			if err != nil {
				return nil, err
			}
			if isIOsInProgress {
				log.Infof("IOs are in progress for [%v]", vol.Name)
				poolUuids := appVol.ReplicaSets[0].PoolUuids
				for _, p := range poolUuids {
					if Contains(nodePools, p) && eligibilityMap[p] {
						selectedNodePoolID = p
						break outer
					}
				}
			}
		}
	}

	selectedPool, err = GetStoragePoolByUUID(selectedNodePoolID)

	if err != nil {
		return nil, err
	}
	return selectedPool, nil
}

// GetRandomNodeWithPoolIOs returns node with IOs running
func GetRandomNodeWithPoolIOs(contexts []*scheduler.Context) (node.Node, error) {
	// pick a storage node with pool having IOs

	poolID, err := GetPoolIDWithIOs(contexts)
	if err != nil {
		return node.Node{}, err
	}

	n, err := GetNodeWithGivenPoolID(poolID)
	return *n, err
}

func GetRandomStorageLessNode(slNodes []node.Node) node.Node {
	// pick a random storageless node
	randomIndex := rand.Intn(len(slNodes))
	for _, slNode := range slNodes {
		if randomIndex == 0 {
			return slNode
		}
		randomIndex--
	}
	return node.Node{}
}

// GetPoolIDsFromVolName returns list of pool IDs associated with a given volume name
func GetPoolIDsFromVolName(volName string) ([]string, error) {
	var poolUuids []string
	volDetails, err := Inst().V.InspectVolume(volName)
	if err != nil {
		return nil, err
	}
	for _, each := range volDetails.ReplicaSets {
		for _, uuids := range each.PoolUuids {
			if len(poolUuids) == 0 {
				poolUuids = append(poolUuids, uuids)
			} else {
				isPresent := false
				for i := 0; i < len(poolUuids); i++ {
					if uuids == poolUuids[i] {
						isPresent = true
					}
				}
				if isPresent == false {
					poolUuids = append(poolUuids, uuids)
				}
			}
		}

	}
	return poolUuids, err
}

// GetPoolExpansionEligibility identifying the nodes and pools in it if they are eligible for expansion
func GetPoolExpansionEligibility(stNode *node.Node) (map[string]bool, error) {
	var err error

	namespace, err := Inst().V.GetVolumeDriverNamespace()
	if err != nil {
		return nil, err
	}

	var maxCloudDrives int32

	if _, err := core.Instance().GetSecret(PX_VSPHERE_SCERET_NAME, namespace); err == nil {
		maxCloudDrives = VSPHERE_MAX_CLOUD_DRIVES
	} else if _, err := core.Instance().GetSecret(PX_PURE_SECRET_NAME, namespace); err == nil {
		maxCloudDrives = FA_MAX_CLOUD_DRIVES
	} else {
		maxCloudDrives = CLOUD_PROVIDER_MAX_CLOUD_DRIVES
	}

	if err != nil {
		return nil, err
	}

	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         2 * time.Minute,
			TimeBeforeRetry: defaultRetryInterval,
		},
		Action: "start",
	}
	drivesMap, err := Inst().N.GetBlockDrives(*stNode, systemOpts)
	if err != nil {
		return nil, fmt.Errorf("error getting block drives from node %s, Err :%v", stNode.Name, err)
	}
	var currentNodeDrives int32

	driveCountMap := make(map[string]int32, 0)

	for _, b := range drivesMap {
		labels := b.Labels
		for k, v := range labels {
			if k == "pxpool" {
				if c, ok := driveCountMap[v]; ok {
					driveCountMap[v] += c
				} else {
					driveCountMap[v] = 1
				}
			}
		}
	}

	for _, vals := range driveCountMap {
		currentNodeDrives += vals
	}
	eligibilityMap := make(map[string]bool)

	log.Infof("Node %s has total drives %d", stNode.Name, currentNodeDrives)
	eligibilityMap[stNode.Id] = true
	if currentNodeDrives == maxCloudDrives {
		eligibilityMap[stNode.Id] = false
	}

	for _, pool := range stNode.StoragePools {
		eligibilityMap[pool.Uuid] = true

		d := driveCountMap[fmt.Sprintf("%d", pool.ID)]
		log.Infof("pool %s has %d drives", pool.Uuid, d)
		if d == POOL_MAX_CLOUD_DRIVES {
			eligibilityMap[pool.Uuid] = false
		}
	}

	return eligibilityMap, nil
}

// WaitTillEnterMaintenanceMode wait until the node enters maintenance mode
func WaitTillEnterMaintenanceMode(n node.Node) error {
	t := func() (interface{}, bool, error) {
		nodeState, err := Inst().V.IsNodeInMaintenance(n)
		if err != nil {
			return nil, false, err
		}
		if nodeState == true {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("Not in Maintenance mode")
	}

	_, err := task.DoRetryWithTimeout(t, 20*time.Minute, 2*time.Minute)
	if err != nil {
		return err
	}
	return nil
}

// ExitFromMaintenanceMode wait until the node exits from maintenance mode
func ExitFromMaintenanceMode(n node.Node) error {
	log.InfoD("Exiting maintenance mode on Node %s", n.Name)
	t := func() (interface{}, bool, error) {
		if err := Inst().V.ExitMaintenance(n); err != nil {
			nodeState, err := Inst().V.IsNodeInMaintenance(n)
			if err != nil {
				return nil, false, err
			}
			if nodeState == true {
				return nil, true, nil
			}
			return nil, true, err
		}
		return nil, false, nil
	}
	_, err := task.DoRetryWithTimeout(t, 15*time.Minute, 2*time.Minute)
	if err != nil {
		return err
	}
	return nil
}

// ExitNodesFromMaintenanceMode waits till all nodes to exit from maintenance mode
// Checks for all the storage nodes present in the cluster, in case if any node is in maintenance mode
// Function will attempt bringing back the node out of maintenance
func ExitNodesFromMaintenanceMode() error {
	Nodes := node.GetStorageNodes()
	for _, eachNode := range Nodes {
		nodeState, err := Inst().V.IsNodeInMaintenance(eachNode)
		if err == nil && nodeState == true {
			errExit := ExitFromMaintenanceMode(eachNode)
			if errExit != nil {
				return errExit
			}
		}
	}
	return nil
}

// GetPoolsDetailsOnNode returns all pools present in the Nodes
func GetPoolsDetailsOnNode(n node.Node) ([]*opsapi.StoragePool, error) {
	var poolDetails []*opsapi.StoragePool

	if node.IsStorageNode(n) == false {
		return nil, fmt.Errorf("Node [%s] is not Storage Node", n.Id)
	}

	nodes := node.GetStorageNodes()

	for _, eachNode := range nodes {
		if eachNode.Id == n.Id {
			for _, eachPool := range eachNode.Pools {
				poolInfo, err := GetStoragePoolByUUID(eachPool.Uuid)
				if err != nil {
					return nil, err
				}
				poolDetails = append(poolDetails, poolInfo)
			}
		}
	}
	return poolDetails, nil
}

// IsEksPxOperator returns true if current operator installation is on an EKS cluster
func IsEksPxOperator() bool {
	if stc, err := Inst().V.GetDriver(); err == nil {
		if oputil.IsEKS(stc) {
			logrus.Infof("EKS installation with PX operator detected.")
			return true
		}
	}
	return false
}

/*
 * GetSubsetOfSlice selects a random subset of unique items from the input slice,
 * with the given length. It returns a new slice with the selected items in random order.
 * If length is zero or negative or greater than the length of the input slice, it also returns an error.
 *
 * Parameters:
 * - items: a slice of any type to select from.
 * - length: the number of items to select from the input slice.
 *
 * Returns:
 * - a new slice of type T with the selected items in random order.
 * - an error if the length parameter is zero or negative, or if it is greater than the length of the input slice.
 */
func GetSubsetOfSlice[T any](items []T, length int) ([]T, error) {
	if length <= 0 {
		return nil, fmt.Errorf("length must be greater than zero")
	}
	if length > len(items) {
		return nil, fmt.Errorf("length cannot be greater than the length of the input items")
	}
	randomItems := make([]T, length)
	selected := make(map[int]bool)
	for i := 0; i < length; i++ {
		j := rand.Intn(len(items))
		for selected[j] {
			j = rand.Intn(len(items))
		}
		selected[j] = true
		randomItems[i] = items[j]
	}
	return randomItems, nil
}

func GetAutoFsTrimStatusForCtx(ctx *scheduler.Context) (map[string]opsapi.FilesystemTrim_FilesystemTrimStatus, error) {

	appVolumes, err := Inst().S.GetVolumes(ctx)
	if err != nil {
		return nil, err
	}

	ctxAutoFsTrimStatus := make(map[string]opsapi.FilesystemTrim_FilesystemTrimStatus)

	for _, v := range appVolumes {
		// Skip autofs trim status on Pure DA volumes
		isPureVol, err := Inst().V.IsPureVolume(v)
		if err != nil {
			return nil, err
		}
		if isPureVol {
			return nil, fmt.Errorf("autofstrim is not supported for Pure DA volume")
		}
		//skipping fstrim check for log PVCs
		if strings.Contains(v.Name, "log") {
			continue
		}
		log.Infof("inspecting volume [%s]", v.Name)
		appVol, err := Inst().V.InspectVolume(v.ID)
		if err != nil {
			return nil, fmt.Errorf("error inspecting volume: %v", err)
		}
		attachedNode := appVol.AttachedOn
		fsTrimStatuses, err := Inst().V.GetAutoFsTrimStatus(attachedNode)
		if err != nil {
			return nil, err
		}

		val, ok := fsTrimStatuses[appVol.Id]
		var fsTrimStatus opsapi.FilesystemTrim_FilesystemTrimStatus

		if !ok {
			fsTrimStatus, err = waitForFsTrimStatus(nil, attachedNode, appVol.Id)
			if err != nil {
				return nil, err
			}
		} else {
			fsTrimStatus = val
		}

		if fsTrimStatus != -1 {
			ctxAutoFsTrimStatus[appVol.Id] = fsTrimStatus
		} else {
			return nil, fmt.Errorf("autofstrim for volume [%v] not started on node [%s]", v.ID, attachedNode)
		}

	}
	return ctxAutoFsTrimStatus, nil
}

func GetAutoFstrimUsageForCtx(ctx *scheduler.Context) (map[string]*opsapi.FstrimVolumeUsageInfo, error) {
	appVolumes, err := Inst().S.GetVolumes(ctx)
	if err != nil {
		return nil, err
	}

	ctxAutoFsTrimStatus := make(map[string]*opsapi.FstrimVolumeUsageInfo)

	for _, v := range appVolumes {
		// Skip autofs trim status on Pure DA volumes
		isPureVol, err := Inst().V.IsPureVolume(v)
		if err != nil {
			return nil, err
		}
		if isPureVol {
			return nil, fmt.Errorf("autofstrim is not supported for Pure DA volume")
		}
		//skipping fstrim check for log PVCs
		if strings.Contains(v.Name, "log") {
			continue
		}
		log.Infof("Getting info: %s", v.ID)
		appVol, err := Inst().V.InspectVolume(v.ID)
		if err != nil {
			return nil, fmt.Errorf("error inspecting volume: %v", err)
		}
		attachedNode := appVol.AttachedOn
		fsTrimUsages, err := Inst().V.GetAutoFsTrimUsage(attachedNode)
		if err != nil {
			return nil, err
		}

		val, ok := fsTrimUsages[appVol.Id]
		var fsTrimStatus *opsapi.FstrimVolumeUsageInfo

		if !ok {
			log.Errorf("usage not found for %s", appVol.Id)
		} else {
			fsTrimStatus = val
		}

		if fsTrimStatus != nil {
			ctxAutoFsTrimStatus[appVol.Id] = fsTrimStatus
		} else {
			return nil, fmt.Errorf("autofstrim for volume [%v] has no usage on node [%s]", v.ID, attachedNode)
		}

	}
	return ctxAutoFsTrimStatus, nil
}
