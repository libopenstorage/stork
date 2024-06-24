package tests

import (
	"bufio"
	context1 "context"
	"crypto/tls"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Masterminds/semver/v3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/devans10/pugo/flasharray"
	"github.com/hashicorp/go-version"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	tektoncdv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"

	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	opsapi "github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/pkg/sched"
	pxapi "github.com/libopenstorage/operator/api/px"
	"github.com/libopenstorage/operator/drivers/storage/portworx/util"
	oputil "github.com/libopenstorage/operator/drivers/storage/portworx/util"
	optest "github.com/libopenstorage/operator/pkg/util/test"
	"github.com/libopenstorage/stork/pkg/storkctl"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/kubevirt"
	"github.com/portworx/sched-ops/k8s/operator"
	policyops "github.com/portworx/sched-ops/k8s/policy"
	k8sStorage "github.com/portworx/sched-ops/k8s/storage"
	schedstorage "github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/sched-ops/k8s/stork"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"

	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/natefinch/lumberjack.v2"
	yaml "gopkg.in/yaml.v2"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	pdsv1 "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsapi "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	networkingv1beta1 "k8s.io/api/networking/v1beta1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	rbacv1 "k8s.io/api/rbac/v1"
	storageapi "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	rest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/strings/slices"
	kubevirtv1 "kubevirt.io/api/core/v1"

	"github.com/portworx/torpedo/drivers"
	appType "github.com/portworx/torpedo/drivers/applications/apptypes"
	appDriver "github.com/portworx/torpedo/drivers/applications/driver"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/monitor"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/vsphere"
	"github.com/portworx/torpedo/drivers/pds"
	"github.com/portworx/torpedo/drivers/scheduler/aks"
	"github.com/portworx/torpedo/drivers/scheduler/anthos"
	"github.com/portworx/torpedo/drivers/scheduler/openshift"
	appUtils "github.com/portworx/torpedo/drivers/utilities"
	"github.com/portworx/torpedo/drivers/volume"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/aetosutil"
	"github.com/portworx/torpedo/pkg/asyncdr"
	"github.com/portworx/torpedo/pkg/jirautils"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/osutils"
	"github.com/portworx/torpedo/pkg/pureutils"
	"github.com/portworx/torpedo/pkg/s3utils"
	"github.com/portworx/torpedo/pkg/stats"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	"github.com/portworx/torpedo/pkg/units"

	// import ssh driver to invoke it's init
	"github.com/portworx/torpedo/drivers/node/ssh"

	// import backup driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/backup/portworx"
	// import aws driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/aws"
	// import vsphere driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/vsphere"
	// import ibm driver to invoke it's init
	"github.com/portworx/torpedo/drivers/node/ibm"
	_ "github.com/portworx/torpedo/drivers/node/ibm"

	// import oracle driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/oracle"

	// import ssh driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/scheduler"

	// import scheduler drivers to invoke it's init
	_ "github.com/portworx/torpedo/drivers/scheduler/dcos"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/scheduler/spec"

	// import ocp scheduler driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/scheduler/openshift"

	// import aks scheduler driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/scheduler/aks"

	// import scheduler drivers to invoke it's init
	_ "github.com/portworx/torpedo/drivers/scheduler/eks"

	// import gke scheduler driver to invoke it's init
	"github.com/portworx/torpedo/drivers/scheduler/gke"
	_ "github.com/portworx/torpedo/drivers/scheduler/gke"

	_ "github.com/portworx/torpedo/drivers/scheduler/oke"

	// import rke scheduler drivers to invoke it's init
	"github.com/portworx/torpedo/drivers/scheduler/rke"

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

	// import driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/pds/dataservice"

	// import scheduler drivers to invoke it's init
	_ "github.com/portworx/torpedo/drivers/scheduler/anthos"

	// import pso driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/volume/pso"

	// import ibm driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/volume/ibm"

	// import scheduler drivers to invoke it's init
	_ "github.com/portworx/torpedo/drivers/scheduler/iks"

	// import ocp driver to invoke it's init
	_ "github.com/portworx/torpedo/drivers/volume/ocp"
)

const (
	// SkipClusterScopedObjects describes option for skipping deletion of cluster wide objects
	SkipClusterScopedObjects   = "skipClusterScopedObjects"
	CreateCloudCredentialError = "PermissionDenied desc = Access denied for [Resource: cloudcredential]"
)

// PDS params
const (
	deployPDSAppsFlag = "deploy-pds-apps"
	pdsDriveCliFlag   = "pds-driver"
)

var (
	clusterProviders       = []string{"k8s"}
	GlobalCredentialConfig backup.BackupCloudConfig
	GlobalGkeSecretString  string
)

var (
	NamespaceAppWithDataMap    = make(map[string][]appDriver.ApplicationDriver)
	IsReplacePolicySetToDelete = false // To check if the policy in the test is set to delete - Skip data continuity validation in this case
)

var (
	// PDBValidationMinOpVersion specifies the minimum PX Operator version required to enable PDB validation in the UpgradeCluster
	PDBValidationMinOpVersion, _ = version.NewVersion("24.1.0-")
)

type OwnershipAccessType int32

const (
	Invalid OwnershipAccessType = 0
	// Read access only and cannot affect the resource.
	Read = 1
	// Write access and can affect the resource.
	// This type automatically provides Read access also.
	Write = 2
	// Admin access
	// This type automatically provides Read and Write access also.
	Admin = 3
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
	csiAppCliFlag                        = "csi-app-list"
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
	scaleAppTimeoutFlag                  = "scale-app-timeout"

	pxbackupDeploymentName             = "px-backup"
	pxbackupDeploymentNamespace        = "px-backup"
	pxbackupMongodbDeploymentName      = "pxc-backup-mongodb"
	pxbackupMongodbDeploymentNamespace = "px-backup"
	defaultnamespace                   = "default"

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
	metromigrationKey      = "metro-dr-"
	migrationRetryTimeout  = 10 * time.Minute
	migrationRetryInterval = 10 * time.Second
	defaultClusterPairDir  = "cluster-pair"

	envSkipDiagCollection = "SKIP_DIAG_COLLECTION"

	torpedoJobNameFlag       = "torpedo-job-name"
	torpedoJobTypeFlag       = "torpedo-job-type"
	clusterCreationTimeout   = 5 * time.Minute
	clusterCreationRetryTime = 10 * time.Second

	// Anthos
	anthosWsNodeIpCliFlag            = "anthos-ws-node-ip"
	anthosInstPathCliFlag            = "anthos-inst-path"
	skipSystemCheckCliFlag           = "torpedo-skip-system-checks"
	dataIntegrityValidationTestsFlag = "data-integrity-validation-tests"
	faSecretCliFlag                  = "fa-secret"

	// PSA Specific
	kubeApiServerConfigFilePath     = "/etc/kubernetes/manifests/kube-apiserver.yaml"
	kubeApiServerConfigFilePathBkp  = "/etc/kubernetes/kube-apiserver.yaml.bkp"
	KubeAdmissionControllerFilePath = "/etc/kubernetes/admission/admissioncontroller.yaml"
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
	StorkNamePrefix                   = "stork-namespace"
	BackupRestoreCompletionTimeoutMin = 20
	clusterDeleteTimeout              = 60 * time.Minute
	clusterDeleteRetryTime            = 30 * time.Second
	poolExpandApplyTimeOut            = 15 * time.Minute
	poolExpandApplyRetryTime          = 30 * time.Second
	backupLocationDeleteTimeoutMin    = 60
	CredName                          = "tp-backup-cred"
	KubeconfigDirectory               = "/tmp"
	RetrySeconds                      = 10
	BackupScheduleAllName             = "-all"
	SchedulePolicyAllName             = "schedule-policy-all"
	SchedulePolicyScaleName           = "schedule-policy-scale"
	BucketNamePrefix                  = "tp-backup-bucket"
	mongodbStatefulset                = "pxc-backup-mongodb"
	AwsS3encryptionPolicy             = "s3:x-amz-server-side-encryption=AES256"
	AwsS3Sid                          = "DenyNonAES256Uploads"
)

const (
	oneMegabytes                          = 1024 * 1024
	defaultScheduler                      = "k8s"
	defaultNodeDriver                     = "ssh"
	defaultMonitorDriver                  = "prometheus"
	defaultStorageDriver                  = "pxd"
	defaultPdsDriver                      = "pds"
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
	serviceURL                            = "https://us-east.iaas.cloud.ibm.com/v1"
)

const (
	waitResourceCleanup         = 2 * time.Minute
	defaultTimeout              = 5 * time.Minute
	defaultVolScaleTimeout      = 4 * time.Minute
	defaultIbmVolScaleTimeout   = 8 * time.Minute
	defaultRetryInterval        = 10 * time.Second
	defaultCmdTimeout           = 20 * time.Second
	defaultCmdRetryInterval     = 5 * time.Second
	defaultDriverStartTimeout   = 10 * time.Minute
	defaultKvdbRetryInterval    = 5 * time.Minute
	addDriveUpTimeOut           = 15 * time.Minute
	podDestroyTimeout           = 5 * time.Minute
	kubeApiServerBringUpTimeout = 20 * time.Minute
	KubeApiServerWait           = 15 * time.Minute
	NSWaitTimeout               = 10 * time.Minute
	NSWaitTimeoutRetry          = 20 * time.Second
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

const (
	IBMHelmRepoName   = "ibm-helm-portworx"
	IBMHelmRepoURL    = "https://raw.githubusercontent.com/portworx/ibm-helm/master/repo/stable"
	IBMHelmValuesFile = "/tmp/values.yaml"
)

const (
	ValidateStorageClusterTimeout = 40 * time.Minute
)

// LabLabel used to name the licensing features
type LabLabel string

const (
	IBMTestLicenseSKU   = "PX-Enterprise IBM Cloud (test)"
	IBMTestLicenseDRSKU = "PX-Enterprise IBM Cloud DR (test)"
	IBMProdLicenseSKU   = "PX-Enterprise IBM Cloud"
	IBMProdLicenseDRSKU = "PX-Enterprise IBM Cloud DR"
)

const (
	// LabNodes - Number of nodes maximum
	LabNodes LabLabel = "Nodes"
	// LabVolumeSize - Volume capacity [TB] maximum
	LabVolumeSize LabLabel = "VolumeSize"
	// LabVolumes - Number of volumes per cluster maximum
	LabVolumes LabLabel = "Volumes"
	// LabSnapshots - Number of snapshots per volume maximum
	LabSnapshots LabLabel = "Snapshots"
	// LabHaLevel - Volume replica count
	LabHaLevel LabLabel = "HaLevel"
	// LabSharedVol - Shared volumes
	LabSharedVol LabLabel = "SharedVolume"
	// LabEncryptedVol - BYOK data encryption
	LabEncryptedVol LabLabel = "EncryptedVolume"
	// LabScaledVol - Volume sets
	LabScaledVol LabLabel = "ScaledVolume"
	// LabAggregatedVol - Storage aggregation
	LabAggregatedVol LabLabel = "AggregatedVolume"
	// LabResizeVolume - Resize volumes on demand
	LabResizeVolume LabLabel = "ResizeVolume"
	// LabCloudSnap - Snapshot to object store [CloudSnap]
	LabCloudSnap LabLabel = "SnapshotToObjectStore"
	// LabCloudSnapDaily - Number of CloudSnaps daily per volume maximum
	LabCloudSnapDaily LabLabel = "SnapshotToObjectStoreDaily"
	// LabCloudMigration -Cluster-level migration [Kube-motion/Data Migration]
	LabCloudMigration LabLabel = "CloudMigration"
	// LabDisasterRecovery - Disaster Recovery [PX-DR]
	LabDisasterRecovery LabLabel = "DisasterRecovery"
	// LabAUTCapacityMgmt - Autopilot Capacity Management
	LabAUTCapacityMgmt LabLabel = "AUTCapacityManagement"
	// LabPlatformBare - Bare-metal hosts
	LabPlatformBare LabLabel = "EnablePlatformBare"
	// LabPlatformVM - Virtual machine hosts
	LabPlatformVM LabLabel = "EnablePlatformVM"
	// LabNodeCapacity - Node disk capacity [TB] maximum
	LabNodeCapacity LabLabel = "NodeCapacity"
	// LabNodeCapacityExtend - Node disk capacity extension
	LabNodeCapacityExtend LabLabel = "NodeCapacityExtension"
	// LabLocalAttaches - Number of attached volumes per node maximum
	LabLocalAttaches LabLabel = "LocalVolumeAttaches"
	// LabOIDCSecurity - OIDC Security
	LabOIDCSecurity LabLabel = "OIDCSecurity"
	// LabGlobalSecretsOnly - Limit BYOK encryption to cluster-wide secrets
	LabGlobalSecretsOnly LabLabel = "GlobalSecretsOnly"
	// LabFastPath - FastPath extension [PX-FAST]
	LabFastPath LabLabel = "FastPath"
	// UnlimitedNumber represents the unlimited number of licensed resource.
	// note - the max # Flex counts handle, is actually 999999999999999990
	UnlimitedNumber = int64(0x7FFFFFFF) // C.FLX_FEATURE_UNCOUNTED_VALUE = 0x7FFFFFFF  (=2147483647)
	Unlimited       = int64(0x7FFFFFFFFFFFFFFF)

	// -- Testing maximums below

	// MaxNumNodes is a maximum nodes in a cluster
	MaxNumNodes = int64(1000)
	// MaxNumVolumes is a maximum number of volumes in a cluster
	MaxNumVolumes = int64(100000)
	// MaxVolumeSize is a maximum volume size for single volume [in TB]
	MaxVolumeSize = int64(40)
	// MaxNodeCapacity defines the maximum node's disk capacity [in TB]
	MaxNodeCapacity = int64(256)
	// MaxLocalAttachCount is a maximum number of local volume attaches
	MaxLocalAttachCount = int64(256)
	// MaxHaLevel is a maximum replication factor
	MaxHaLevel = int64(3)
	// MaxNumSnapshots is a maximum number of snapshots
	MaxNumSnapshots = int64(64)
)

var (
	IBMLicense = map[LabLabel]interface{}{
		LabNodes:              &pxapi.LicensedFeature_Count{Count: 1000},
		LabVolumeSize:         &pxapi.LicensedFeature_CapacityTb{CapacityTb: 40},
		LabVolumes:            &pxapi.LicensedFeature_Count{Count: 16384},
		LabHaLevel:            &pxapi.LicensedFeature_Count{Count: MaxHaLevel},
		LabSnapshots:          &pxapi.LicensedFeature_Count{Count: 64},
		LabAggregatedVol:      &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabSharedVol:          &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabEncryptedVol:       &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabGlobalSecretsOnly:  &pxapi.LicensedFeature_Enabled{Enabled: false},
		LabScaledVol:          &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabResizeVolume:       &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabCloudSnap:          &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabCloudSnapDaily:     &pxapi.LicensedFeature_Count{Count: Unlimited},
		LabCloudMigration:     &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabDisasterRecovery:   &pxapi.LicensedFeature_Enabled{Enabled: false},
		LabPlatformBare:       &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabPlatformVM:         &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabNodeCapacity:       &pxapi.LicensedFeature_CapacityTb{CapacityTb: 256},
		LabNodeCapacityExtend: &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabLocalAttaches:      &pxapi.LicensedFeature_Count{Count: 256},
		LabOIDCSecurity:       &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabAUTCapacityMgmt:    &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabFastPath:           &pxapi.LicensedFeature_Enabled{Enabled: false},
	}
)

var pxRuntimeOpts string
var pxClusterOpts string
var PxBackupVersion string

var (
	RunIdForSuite             int
	TestRailSetupSuccessful   bool
	CurrentTestRailTestCaseId int
)

var (
	errPureFileSnapshotNotSupported    = errors.New("snapshot feature is not supported for pure_file volumes")
	errPureCloudsnapNotSupported       = errors.New("not supported")
	errPureGroupsnapNotSupported       = errors.New("not supported")
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
	k8sCore       = core.Instance()
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
	CurrentClusterConfigPath             = ""
	clusterProvider                      = "aws"
)

var (
	includeResourcesFlag        = true
	includeVolumesFlag          = true
	startApplicationsFlag       = true
	tempDir                     = "/tmp"
	bidirectionalClusterPairDir = "bidirectional-cluster-pair"
	migrationList               []*storkapi.Migration
)

var (
	// ClusterConfigPathMap maps cluster name registered in px-backup to the path to the kubeconfig
	ClusterConfigPathMap = make(map[string]string, 2)
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
	rootLogDir            = "/root/logs"
	diagsDirPath          = "diags.pwx.dev.purestorage.com:/var/lib/osd/pxns/688230076034934618"
	pxbLogDirPath         = "/tmp/px-backup-test-logs"
	KubevirtNamespace     = "kubevirt"
	LatestKubevirtVersion = "v1.0.0"
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

var dataIntegrityValidationTests string

// TestLogger for logging test logs
var TestLogger *lumberjack.Logger
var dash *aetosutil.Dashboard
var post_rule_uid string
var pre_rule_uid string

type PlatformCredentialStruct struct {
	credName string
	credUID  string
}

type (
	// TestcaseAuthor represents the owner of a Testcase
	TestcaseAuthor string
	// TestcaseQuarter represents the fiscal quarter during which the Testcase is automated
	TestcaseQuarter string
)

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
		PureFADAPod:                      Inst().PureFADAPod,
		RunCSISnapshotAndRestoreManyTest: Inst().RunCSISnapshotAndRestoreManyTest,
		HelmValuesConfigMapName:          Inst().HelmValuesConfigMap,
		SecureApps:                       Inst().SecureAppList,
		AnthosAdminWorkStationNodeIP:     Inst().AnthosAdminWorkStationNodeIP,
		AnthosInstancePath:               Inst().AnthosInstPath,
		UpgradeHops:                      Inst().SchedUpgradeHops,
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
	SetupTestRail()

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

	PrintPxctlStatus()
	ns, err := Inst().V.GetVolumeDriverNamespace()
	log.FailOnError(err, "Error occured while getting volume driver namespace")
	installGrafana(ns)
	err = updatePxClusterOpts()
	log.Errorf("%v", err)
}

func PrintPxctlStatus() {
	PrintCommandOutput("pxctl status")
}

func PrintInspectVolume(volID string) {
	PrintCommandOutput(fmt.Sprintf("pxctl volume inspect %s", volID))
}

func PrintCommandOutput(cmnd string) {
	output, err := Inst().N.RunCommand(node.GetStorageNodes()[0], cmnd, node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
		Sudo:            true,
	})
	if err != nil {
		log.Errorf("failed to run command [%s], Err: %v", cmnd, err)
	}
	log.Infof(output)

}

func PrintSvPoolStatus(node node.Node) {
	output, err := runCmdGetOutput("pxctl sv pool show", node)
	if err != nil {
		log.Warnf("error getting pool data on node [%s], cause: %v", node.Name, err)
		return
	}
	log.Infof(output)
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
		updateChannel(err, errChan...)
	} else {
		log.FailOnError(err, "processError")
	}
}

func updateChannel(err error, errChan ...*chan error) {
	if len(errChan) > 0 && err != nil {
		log.Errorf(fmt.Sprintf("%v", err))
		*errChan[0] <- err
	}
}

func ValidatePDSDataServices(ctx *scheduler.Context, errChan ...*chan error) {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()

	Step(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {
		stepLog := fmt.Sprintf("check health status of %s app", ctx.App.Key)

		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, specObj := range ctx.App.SpecList {
				if pdsobj, ok := specObj.(*pdsv1.ModelsDeployment); ok {
					err := Inst().Pds.ValidateDataServiceDeployment(pdsobj, *pdsobj.Namespace.Name)
					if err != nil {
						PrintDescribeContext(ctx)
						processError(err, errChan...)
						return
					}
				}
			}
		})
	})
}

func IsPoolAddDiskSupported() bool {
	DMthin, err := IsDMthin()
	log.FailOnError(err, "Error occured while checking if DMthin is enabled")
	if DMthin {
		dmthinSupportedPxVersion, px_err := semver.NewVersion("3.1.0")
		if px_err != nil {
			log.FailOnError(px_err, "Error occured :%s")
		}
		driverVersion, version_err := Inst().V.GetDriverVersion()
		if version_err != nil {
			log.FailOnError(version_err, "Error occured while fetching current version")
		}
		var new_trimmedVersion string
		parts := strings.Split(driverVersion, "-")
		trimmedVersion := strings.Split(parts[0], ".")
		if len(trimmedVersion) > 3 {
			new_trimmedVersion = strings.Join(trimmedVersion[:3], ".")
		} else {
			new_trimmedVersion = parts[0]
		}
		currentPxVersionOnCluster, semver_err := semver.NewVersion(new_trimmedVersion)
		if semver_err != nil {
			log.FailOnError(semver_err, "Error occured while comparing the current and expected version")
		}
		log.InfoD(fmt.Sprintf("The current version on the cluster is :%s", currentPxVersionOnCluster))
		if currentPxVersionOnCluster.GreaterThan(dmthinSupportedPxVersion) {
			log.Errorf("drive add to existing pool not supported for px-storev2 or px-cache pools as the current version is:%s", currentPxVersionOnCluster)
			return false
		}
	}
	return true
}

// ValidateContext is the ginkgo spec for validating a scheduled context
func ValidateContext(ctx *scheduler.Context, errChan ...*chan error) {
	// Apps for which we have to skip volume validation due to various limitations
	excludeAppContextList := []string{"tektoncd", "pxb-singleapp-multivol", "pxb-multipleapp-multivol"}
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	Step(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {
		var timeout time.Duration
		log.InfoD(fmt.Sprintf("Validating %s app", ctx.App.Key))
		appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
		if Inst().ScaleAppTimeout != time.Duration(0) {
			timeout = Inst().ScaleAppTimeout
		} else if ctx.ReadinessTimeout == time.Duration(0) {
			timeout = appScaleFactor * defaultTimeout
		} else {
			timeout = appScaleFactor * ctx.ReadinessTimeout
		}

		Step(fmt.Sprintf("validate %s app's volumes", ctx.App.Key), func() {
			// Check whether the given app should be excluded from volume validation.
			if IsPresent(excludeAppContextList, ctx.App.Key) {
				ctx.SkipVolumeValidation = true
			}
			if !ctx.SkipVolumeValidation {
				log.InfoD(fmt.Sprintf("Validating %s app's volumes", ctx.App.Key))
				ValidateVolumes(ctx, errChan...)
			}
		})

		stepLog := fmt.Sprintf("wait for %s app to start running", ctx.App.Key)

		Step(stepLog, func() {
			log.InfoD(stepLog)
			if !ctx.SkipPodValidation {
				err := Inst().S.WaitForRunning(ctx, timeout, defaultRetryInterval)
				if err != nil {
					PrintDescribeContext(ctx)
					processError(err, errChan...)
					return
				}
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

			var vols []*volume.Volume
			var err error
			t := func() (interface{}, bool, error) {
				vols, err = Inst().S.GetVolumes(ctx)
				if err != nil {
					return "", true, err
				}
				return "", false, nil
			}

			if _, err = task.DoRetryWithTimeout(t, 2*time.Minute, 5*time.Second); err != nil {
				log.Errorf("Failed to get app %s's volumes", ctx.App.Key)
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

		// Validating px pod restart count only for portworx volume driver
		if Inst().V.String() == "pxd" {
			Step("Validate Px pod restart count", func() {
				ValidatePxPodRestartCount(ctx, errChan...)
			})
		}
	})
}

func ValidatePDB(pdbValue int, allowedDisruptions int, initialNumNodes int, isClusterParallelyUpgraded *bool, errChan ...*chan error) {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	t := func() (interface{}, bool, error) {
		currentPdbValue, _ := GetPDBValue()
		if currentPdbValue == -1 {
			return -1, true, fmt.Errorf("failed to get PDB value")
		}
		return currentPdbValue, false, nil
	}
	currentPdbValue, _ := task.DoRetryWithTimeout(t, defaultTimeout, defaultRetryInterval)
	if currentPdbValue == -1 {
		err := fmt.Errorf("failed to get PDB value")
		processError(err, errChan...)
	}

	Step("Validate PDB minAvailable for px storage", func() {
		if currentPdbValue != pdbValue {
			err := fmt.Errorf("PDB minAvailable value has changed. Expected: %d, Actual: %d", pdbValue, currentPdbValue)
			processError(err, errChan...)
		}

	})
	Step("Validate number of disruptions ", func() {
		t := func() (interface{}, bool, error) {
			nodes, err := Inst().V.GetDriverNodes()
			if err != nil {
				return nil, true, fmt.Errorf("failed to get portworx nodes due to %v. Retrying with timeout", err)
			} else {
				return nodes, false, nil
			}
		}
		nodes, err := task.DoRetryWithTimeout(t, defaultTimeout, defaultRetryInterval)
		if err != nil {
			processError(err, errChan...)
		} else {
			currentNumNodes := len(nodes.([]*opsapi.StorageNode))
			if allowedDisruptions < initialNumNodes-currentNumNodes {
				err := fmt.Errorf("number of nodes down is more than allowed disruptions . Expected: %d, Actual: %d", allowedDisruptions, initialNumNodes-currentNumNodes)
				processError(err, errChan...)
			}
			if initialNumNodes-currentNumNodes > 1 {
				*isClusterParallelyUpgraded = true

			}
		}

	})

}

func GetPDBValue() (int, int) {
	stc, err := Inst().V.GetDriver()
	if err != nil {
		return -1, -1
	}
	pdb, err := policyops.Instance().GetPodDisruptionBudget("px-storage", stc.Namespace)
	if err != nil {
		return -1, -1
	}
	return pdb.Spec.MinAvailable.IntValue(), int(pdb.Status.DisruptionsAllowed)
}

func ValidatePureCloudDriveTopologies() error {
	nodes, err := Inst().V.GetDriverNodes()
	if err != nil {
		return err
	}
	nodesMap := node.GetNodesByName()

	driverNamespace, err := Inst().V.GetVolumeDriverNamespace()
	if err != nil {
		return err
	}

	pxPureSecret, err := pureutils.GetPXPureSecret(driverNamespace)
	if err != nil {
		return err
	}

	endpointToZoneMap := pxPureSecret.GetArrayToZoneMap()
	if len(endpointToZoneMap) == 0 {
		return fmt.Errorf("parsed px-pure-secret endpoint to zone map, but no arrays in map (len==0)")
	}

	log.Infof("Endpoint to zone map: %v", endpointToZoneMap)

	for _, node := range nodes {
		log.Infof("Inspecting drive sets on node %v", node.SchedulerNodeName)
		nodeFromMap, ok := nodesMap[node.SchedulerNodeName]
		if !ok {
			return fmt.Errorf("Failed to find node %s in node map", node.SchedulerNodeName)
		}

		var nodeZone string
		if nodeFromMap.SchedulerTopology != nil && nodeFromMap.SchedulerTopology.Labels != nil {
			if z, ok := nodeFromMap.SchedulerTopology.Labels["topology.portworx.io/zone"]; ok {
				nodeZone = z
			}
		}

		if nodeZone == "" {
			log.Warnf("Node %s has no zone (missing the topology.portworx.io/zone label), skipping drive set checks for it", node.SchedulerNodeName)
			continue
		}

		driveSet, err := Inst().V.GetDriveSet(&nodeFromMap)
		if err != nil {
			return err
		}

		for configID, driveConfig := range driveSet.Configs {
			err = nil
			if len(driveConfig.Labels) == 0 {
				return fmt.Errorf("drive config %s has no labels: validate that you're running on PX master or 3.0+ and using FlashArray cloud drives with topology enabled", configID)
			}

			var arrayEndpoint string
			if arrayEndpoint, ok = driveConfig.Labels[pureutils.CloudDriveFAMgmtLabel]; !ok {
				return fmt.Errorf("drive config %s is missing the '%s' label: validate that you're running PX master or 3.0+ and using FlashArray cloud drives with topology enabled", configID, pureutils.CloudDriveFAMgmtLabel)
			}

			var driveZone string
			if driveZone, ok = endpointToZoneMap[arrayEndpoint]; !ok {
				return fmt.Errorf("drive config %s is on array with endpoint '%s', which is not listed in px-pure-secret", configID, arrayEndpoint)
			}

			if driveZone != nodeZone {
				return fmt.Errorf("drive config %s is provisioned on array in zone %s, but node '%s' is in zone %s, which is not topologically correct", configID, driveZone, node.SchedulerNodeName, nodeZone)
			}
		}
	}

	return nil
}

// ValidateContextForPureVolumesSDK is the ginkgo spec for validating a scheduled context
func ValidateContextForPureVolumesSDK(ctx *scheduler.Context, errChan ...*chan error) {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	Step(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {
		var timeout time.Duration
		var isRaw bool
		appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
		if ctx.ReadinessTimeout == time.Duration(0) {
			timeout = appScaleFactor * defaultTimeout
		} else {
			timeout = appScaleFactor * ctx.ReadinessTimeout
		}

		// For raw block volumes resize is failing hence skipping test for it. defect filed - PWX-32793
		for _, specObj := range ctx.App.SpecList {
			if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {
				isRaw = *obj.Spec.VolumeMode == corev1.PersistentVolumeBlock
			}
		}

		Step(fmt.Sprintf("validate %s app's volumes", ctx.App.Key), func() {
			if !ctx.SkipVolumeValidation {
				ValidatePureSnapshotsSDK(ctx, errChan...)
			}
		})

		Step(fmt.Sprintf("validate %s app's volumes resizing ", ctx.App.Key), func() {
			// For raw block volumes resize is failing hence skipping test for it. defect filed - PWX-32793
			if !ctx.SkipVolumeValidation && !isRaw {
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

		driverVersion, err := Inst().V.GetDriverVersion()
		if err != nil {
			processError(err, errChan...)
		}

		// Ignore mount path check if current version is < 3.0.0 (https://portworx.atlassian.net/browse/PWX-34000)
		log.InfoD("Validate current Version [%v]", driverVersion)
		re := regexp.MustCompile(`2\.\d+\.\d+.*`)
		if !re.MatchString(driverVersion) {
			Step("validate mount options for pure volumes", func() {
				if !ctx.SkipVolumeValidation {
					ValidateMountOptionsWithPureVolumes(ctx, errChan...)
				}
			})

			Step(fmt.Sprintf("validate %s app's volumes are created with the file system options specified in the sc", ctx.App.Key), func() {
				if !ctx.SkipVolumeValidation {
					ValidateCreateOptionsWithPureVolumes(ctx, errChan...)
				}
			})
		}
	})
}

// ValidateContextForPureVolumesPXCTL is the ginkgo spec for validating a scheduled context
func ValidateContextForPureVolumesPXCTL(ctx *scheduler.Context, errChan ...*chan error) {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	Step(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {
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
	Step("For validation of an app's volumes", func() {
		var err error
		Step(fmt.Sprintf("inspect %s app's volumes", ctx.App.Key), func() {
			var vols []*volume.Volume
			t := func() (interface{}, bool, error) {
				vols, err = Inst().S.GetVolumes(ctx)
				if err != nil {
					return "", true, err
				}
				return "", false, nil
			}

			if _, err := task.DoRetryWithTimeout(t, 2*time.Minute, 5*time.Second); err != nil {
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
			// If provisioner is IBM increase the timeout to 8 min
			if Inst().Provisioner == "ibm" {
				err = Inst().S.ValidateVolumes(ctx, scaleFactor*defaultIbmVolScaleTimeout, defaultRetryInterval, nil)
			} else {
				err = Inst().S.ValidateVolumes(ctx, scaleFactor*defaultVolScaleTimeout, defaultRetryInterval, nil)
			}
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
	Step("For validation of an app's volumes", func() {
		var err error
		var snapshotVolNames []string
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
				snapshotVolName, err := Inst().V.ValidateCreateSnapshot(vol, params)
				if params["backend"] == k8s.PureBlock {
					expect(err).To(beNil(), "unexpected error creating pure_block snapshot")
				} else if params["backend"] == k8s.PureFile {
					expect(err).NotTo(beNil(), "error expected but no error received while creating pure_file snapshot")
					if err != nil {
						expect(err.Error()).To(contain(errPureFileSnapshotNotSupported.Error()), "incorrect error received creating pure_file snapshot")
					}
				}
				snapshotVolNames = append(snapshotVolNames, snapshotVolName)
			})
			// Temporarily disabled: PWX-37628
			// Step(fmt.Sprintf("get %s app's volume: %s then create cloudsnap", ctx.App.Key, vol), func() {
			// 	err = Inst().V.ValidateCreateCloudsnap(vol, params)
			// 	expect(err).NotTo(beNil(), "error expected but no error received while creating Pure cloudsnap")
			// 	if err != nil {
			// 		expect(err.Error()).To(contain(errPureCloudsnapNotSupported.Error()), "incorrect error received creating Pure cloudsnap")
			// 	}
			// })
		}

		// PWX-37645: Disabled while fixing partition edge cases
		// Step("validate Pure local volume paths", func() {
		// 	err = Inst().V.ValidatePureLocalVolumePaths()
		// 	processError(err, errChan...)
		// })
		Step("Delete the snapshot that is created ", func() {
			for _, vol := range snapshotVolNames {
				err = Inst().V.DeleteVolume(vol)
			}
		})
	})
}

// ValidatePureVolumesPXCTL is the ginkgo spec for validating FA/FB DA volumes using PXCTL for a context
func ValidatePureVolumesPXCTL(ctx *scheduler.Context, errChan ...*chan error) {
	Step("For validation of an app's volumes", func() {
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
	var SnapshotVolumes []string
	Step("For validation of an app's volumes", func() {
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
				snapshotVolName, err := Inst().V.ValidateCreateSnapshotUsingPxctl(vol)
				if params["backend"] == k8s.PureBlock {
					expect(err).To(beNil(), "unexpected error creating pure_block snapshot")
				} else if params["backend"] == k8s.PureFile {
					expect(err).NotTo(beNil(), "error expected but no error received while creating pure_file snapshot")
					if err != nil {
						expect(err.Error()).To(contain(errPureFileSnapshotNotSupported.Error()), "incorrect error received creating pure_file snapshot")
					}
				}
				SnapshotVolumes = append(SnapshotVolumes, snapshotVolName)
			})
			// Temporarily disabled: PWX-37628
			// Step(fmt.Sprintf("get %s app's volume: %s then create cloudsnap using pxctl", ctx.App.Key, vol), func() {
			// 	err = Inst().V.ValidateCreateCloudsnapUsingPxctl(vol)
			// 	expect(err).NotTo(beNil(), "error expected but no error received while creating Pure cloudsnap")
			// 	if err != nil {
			// 		expect(err.Error()).To(contain(errPureCloudsnapNotSupported.Error()), "incorrect error received creating Pure cloudsnap")
			// 	}
			// })
			// Step("validating groupsnap for using pxctl", func() {
			// 	err = Inst().V.ValidateCreateGroupSnapshotUsingPxctl(vol)
			// 	expect(err).NotTo(beNil(), "error expected but no error received while creating Pure groupsnap")
			// 	if err != nil {
			// 		expect(err.Error()).To(contain(errPureGroupsnapNotSupported.Error()), "incorrect error received creating Pure groupsnap")
			// 	}
			// })
		}
		Step("Delete the cloudsnaps created ", func() {
			for _, vol := range SnapshotVolumes {
				err = Inst().V.DeleteVolume(vol)
			}
		})
	})
}

// ValidateResizePurePVC is the ginkgo spec for validating resize of volumes
func ValidateResizePurePVC(ctx *scheduler.Context, errChan ...*chan error) {
	Step("For validation of an resizing pvc", func() {
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

		// PWX-37645: Disabled while fixing partition edge cases
		// Step("validate Pure local volume paths", func() {
		// 	err = Inst().V.ValidatePureLocalVolumePaths()
		// 	processError(err, errChan...)
		// })
	})
}

// ValidatePureVolumeNoReplicaSet is the ginko spec for validating empty replicaset for pure volumes
func ValidatePureVolumeNoReplicaSet(ctx *scheduler.Context, errChan ...*chan error) {
	Step("For validation of an resizing pvc", func() {
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
	Step("For validation of a resizing pvc", func() {
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
		// skiping ValidatePureVolumeStatisticsDynamicUpdate test for raw block volumes. Need to change getStats method
		if !vols[0].Raw {
			byteUsedInitial, err := Inst().V.ValidateGetByteUsedForVolume(vols[0].ID, make(map[string]string))
			fmt.Printf("initially the byteUsed is %v\n", byteUsedInitial)

			// get the pod for this pvc
			pods, err := Inst().S.GetPodsForPVC(vols[0].Name, vols[0].Namespace)
			processError(err, errChan...)

			mountPath, bytesToWrite := pureutils.GetAppDataDir(pods[0].Namespace)
			mountPath = mountPath + "/myfile"

			// write to the Direct Access volume
			ddCmd := fmt.Sprintf("dd bs=512 count=%d if=/dev/urandom of=%s", bytesToWrite/512, mountPath)
			cmdArgs := []string{"exec", "-it", pods[0].Name, "-n", pods[0].Namespace, "--", "bash", "-c", ddCmd}
			err = osutils.Kubectl(cmdArgs)
			processError(err, errChan...)
			fmt.Println("sleeping to let volume usage get reflected")

			// wait until the backends size is reflected before making the REST call, Max time for FBDA Update is 15 min
			time.Sleep(time.Minute * 16)

			byteUsedAfter, err := Inst().V.ValidateGetByteUsedForVolume(vols[0].ID, make(map[string]string))
			fmt.Printf("after writing random bytes to the file the byteUsed in volume %s is %v\n", vols[0].ID, byteUsedAfter)
			expect(byteUsedAfter > byteUsedInitial).To(beTrue(), "bytes used did not increase after writing random bytes to the file")
		}
	})
}

// ValidateCSISnapshotAndRestore is the ginkgo spec for validating actually creating a FADA snapshot, restoring and verifying the content
func ValidateCSISnapshotAndRestore(ctx *scheduler.Context, errChan ...*chan error) {
	Step("For validation of an snapshot and restoring", func() {
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
				SnapName:          "basic-csi" + timestamp + "-snapshot",
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
						err = Inst().V.ValidateVolumeInPxctlList(k)
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
	Step("For validation of an cloning", func() {
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
			log.Infof("==== Cloning volume %s\n", vols[0].Name)
			timestamp := strconv.Itoa(int(time.Now().Unix()))
			request := scheduler.CSICloneRequest{
				Timestamp:       timestamp,
				Namespace:       vols[0].Namespace,
				OriginalPVCName: vols[0].Name,
				RestoredPVCName: "csi-cloned-" + timestamp,
			}

			err = Inst().S.CSICloneTest(ctx, request)
			processError(err, errChan...)

			// PWX-37645: Disabled while fixing partition edge cases
			// err = Inst().V.ValidatePureLocalVolumePaths()
			// processError(err, errChan...)
		}
	})
}

// ValidatePureVolumeLargeNumOfClones is the ginkgo spec for restoring a snapshot to many volumes
func ValidatePureVolumeLargeNumOfClones(ctx *scheduler.Context, errChan ...*chan error) {
	Step("For validation of an restoring large number of volumes from a snapshot", func() {
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

			// Note: the above only creates PVCs, it does not attach them to pods, so no extra care needs to be taken for local paths

			// PWX-37645: Disabled while fixing partition edge cases
			// err = Inst().V.ValidatePureLocalVolumePaths()
			// processError(err, errChan...)
		}
	})
}

// ValidatePoolExpansionWithPureVolumes is the ginkgo spec for executing a pool expansion when FA/FB volumes is attached
func ValidatePoolExpansionWithPureVolumes(ctx *scheduler.Context, errChan ...*chan error) {
	Step("For validation of an expanding storage pools while FA/FB volumes are attached", func() {
		var vols []*volume.Volume
		var err error
		Step(fmt.Sprintf("get %s app's pure volumes", ctx.App.Key), func() {
			vols, err = Inst().S.GetPureVolumes(ctx, "pure_block")
			processError(err, errChan...)
		})
		if len(vols) == 0 {
			log.Warnf("No FlashArray DirectAccess volumes, skipping")
		} else {

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
		}
	})
}

// ValidateMountOptionsWithPureVolumes is the ginkgo spec for executing a check for mountOptions flag
func ValidateMountOptionsWithPureVolumes(ctx *scheduler.Context, errChan ...*chan error) {
	var requiredMountOptions = []string{"nosuid"}
	vols, err := Inst().S.GetVolumes(ctx)
	processError(err, errChan...)
	for _, vol := range vols {
		pvcObj, err := k8sCore.GetPersistentVolumeClaim(vol.Name, vol.Namespace)
		if err != nil {
			log.FailOnError(err, " Failed to get pvc for volume %s", vol)
		}
		sc, err := k8sCore.GetStorageClassForPVC(pvcObj)
		if err != nil {
			log.FailOnError(err, " Error Occured while getting storage class for pvc %s", pvcObj)
		}
		if strings.Contains(strings.Join(sc.MountOptions, ""), "nosuid") {
			// Ignore mount path check if the volume type is purefile, https://purestorage.atlassian.net/issues/PWX-37040
			isPureFile, err := Inst().V.IsPureFileVolume(vol)
			log.FailOnError(err, "Failed to get details about PureFile")
			log.Infof("Given Volume is [%v] and PureFile [%v]", vol.Name, isPureFile)

			if !isPureFile {
				attachedNode, err := Inst().V.GetNodeForVolume(vol, defaultCmdTimeout*3, defaultCmdRetryInterval)
				log.FailOnError(err, "Failed to get app %s's attachednode", ctx.App.Key)

				err = Inst().V.ValidatePureFaFbMountOptions(vol.ID, requiredMountOptions, attachedNode)
				dash.VerifySafely(err, nil, "Testing mount options are properly applied on pure volumes")
			}
		} else {
			log.Infof("There is no nosuid mount option in this volume %s", vol)
		}
	}

}

// ValidateCreateOptionsWithPureVolumes is the ginkgo spec for executing file system options check for the given volume
func ValidateCreateOptionsWithPureVolumes(ctx *scheduler.Context, errChan ...*chan error) {
	vols, err := Inst().S.GetVolumes(ctx)
	log.FailOnError(err, "Failed to get app %s's volumes", ctx.App.Key)
	log.Infof("volumes of app %s are %s", ctx.App.Key, vols)
	for _, v := range vols {
		pvcObj, err := k8sCore.GetPersistentVolumeClaim(v.Name, v.Namespace)
		if err != nil {
			err = fmt.Errorf("Failed to get pvc for volume %s. Err: %v", v, err)
			processError(err, errChan...)
		}

		sc, err := k8sCore.GetStorageClassForPVC(pvcObj)
		if err != nil {
			err = fmt.Errorf("Error Occured while getting storage class for pvc %s. Err: %v", pvcObj, err)
			processError(err, errChan...)
		}

		isPureFile, err := Inst().V.IsPureFileVolume(v)
		log.FailOnError(err, "Failed to get details about PureFile")
		log.Infof("Given Volume is [%v] and PureFile [%v]", v.Name, isPureFile)

		if !isPureFile {
			attachedNode, err := Inst().V.GetNodeForVolume(v, defaultCmdTimeout*3, defaultCmdRetryInterval)
			if err != nil {
				err = fmt.Errorf("Failed to get app %s's attachednode. Err: %v", ctx.App.Key, err)
				processError(err, errChan...)
			}
			if strings.Contains(fmt.Sprint(sc.Parameters), "-b ") {
				FSType, ok := sc.Parameters["csi.storage.k8s.io/fstype"]
				if ok {
					err = Inst().V.ValidatePureFaCreateOptions(v.ID, FSType, attachedNode)
					dash.VerifySafely(err, nil, "File system create options specified in the storage class are properly applied to the pure volumes")
				} else {
					log.Infof("Storage class doesn't have key 'csi.storage.k8s.io/fstype' in parameters")
				}
			} else {
				log.Infof("Storage class doesn't have createoption -b of size 2048 added to it")
			}
		}
	}
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
		Step(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {

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
	Step("For tearing down of an app context", func() {
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
			err = ValidateVolumesDeleted(ctx.App.Key, vols)
			log.FailOnError(err, "Failed to delete volumes for app %s", ctx.App.Key)
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
func ValidateVolumesDeleted(appName string, vols []*volume.Volume) error {
	for _, vol := range vols {
		var err error
		Step(fmt.Sprintf("validate %s app's volume %s has been deleted in the volume driver",
			appName, vol.Name), func() {
			log.InfoD("validate %s app's volume %s has been deleted in the volume driver",
				appName, vol.Name)
			err = Inst().V.ValidateDeleteVolume(vol)
			if err != nil {
				log.Errorf(fmt.Sprintf("%s's volume %s deletion failed", appName, vol.Name))
				return
			}

		})
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteVolumesAndWait deletes volumes of given context and waits till they are deleted
func DeleteVolumesAndWait(ctx *scheduler.Context, options *scheduler.VolumeOptions) error {
	vols := DeleteVolumes(ctx, options)
	err := ValidateVolumesDeleted(ctx.App.Key, vols)
	return err
}

// GetAppNamespace returns namespace in which context is created
func GetAppNamespace(ctx *scheduler.Context, taskName string) string {
	if ctx.ScheduleOptions.Namespace == "" {
		return ctx.App.GetID(fmt.Sprintf("%s-%s", taskName, Inst().InstanceID))
	}
	return ctx.ScheduleOptions.Namespace
}

// GetAppStorageClasses gets the storage classes belonging to an app's PVCs
func GetAppStorageClasses(appCtx *scheduler.Context) (*[]string, error) {
	vols, err := Inst().S.GetVolumes(appCtx)
	if err != nil {
		return nil, fmt.Errorf("Failed to get volumes")
	}
	storageClasses := make([]string, 0)
	for _, vol := range vols {
		pvcObj, err := k8sCore.GetPersistentVolumeClaim(vol.Name, vol.Namespace)
		if err != nil {
			return nil, fmt.Errorf("Failed to get pvc for volume %s", vol)
		}
		sc, err := k8sCore.GetStorageClassForPVC(pvcObj)
		if err != nil {
			return nil, fmt.Errorf("Error Occured while getting storage class for pvc %s", pvcObj)
		}
		storageClasses = append(storageClasses, sc.Name)
	}
	return &storageClasses, nil
}

// CreateScheduleOptions uses the current Context (kubeconfig) to generate schedule options
// NOTE: When using a ScheduleOption that was created during a context (kubeconfig)
// that is different from the current context, make sure to re-generate ScheduleOptions
func CreateScheduleOptions(namespace string, errChan ...*chan error) scheduler.ScheduleOptions {
	log.Infof("Creating ScheduleOptions")

	//if not hyper converged set up deploy apps only on storageless nodes
	if !Inst().IsHyperConverged {
		var err error

		log.Infof("ScheduleOptions: Scheduling apps only on storageless nodes")
		storagelessNodes := node.GetStorageLessNodes()
		if len(storagelessNodes) == 0 {
			log.Info("ScheduleOptions: No storageless nodes available in the PX Cluster. Setting HyperConverges as true")
			Inst().IsHyperConverged = true
		}
		for _, storagelessNode := range storagelessNodes {
			if err = Inst().S.AddLabelOnNode(storagelessNode, "storage", "NO"); err != nil {
				err = fmt.Errorf("ScheduleOptions: failed to add label key [%s] and value [%s] in node [%s]. Error:[%v]",
					"storage", "NO", storagelessNode.Name, err)
				processError(err, errChan...)
			}
		}
		storageLessNodeLabels := make(map[string]string)
		storageLessNodeLabels["storage"] = "NO"

		options := scheduler.ScheduleOptions{
			AppKeys:            Inst().AppList,
			CsiAppKeys:         Inst().CsiAppList,
			StorageProvisioner: Inst().Provisioner,
			Nodes:              storagelessNodes,
			Labels:             storageLessNodeLabels,
			Namespace:          namespace,
		}
		return options

	} else {
		options := scheduler.ScheduleOptions{
			AppKeys:            Inst().AppList,
			CsiAppKeys:         Inst().CsiAppList,
			StorageProvisioner: Inst().Provisioner,
			Namespace:          namespace,
		}
		log.Infof("ScheduleOptions: Scheduling Apps with hyper-converged")
		return options
	}
}

// ScheduleApplications schedules *the* applications and returns the scheduler.Contexts for each app (corresponds to a namespace). NOTE: does not wait for applications
func ScheduleApplications(testname string, errChan ...*chan error) []*scheduler.Context {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	var contexts []*scheduler.Context
	var taskName string
	var err error
	Step("schedule applications", func() {
		if Inst().IsPDSApps {
			log.InfoD("Scheduling PDS Apps...")
			pdsapps, err := Inst().Pds.DeployPDSDataservices()
			if err != nil {
				processError(err, errChan...)
			}
			contexts, err = Inst().Pds.CreateSchedulerContextForPDSApps(pdsapps)
			if err != nil {
				processError(err, errChan...)
			}
		} else {
			options := CreateScheduleOptions("", errChan...)
			taskName = fmt.Sprintf("%s-%v", testname, Inst().InstanceID)
			contexts, err = Inst().S.Schedule(taskName, options)
			// Need to check err != nil before calling processError
			if err != nil {
				processError(err, errChan...)
			}
		}
		if len(contexts) == 0 {
			processError(fmt.Errorf("list of contexts is empty for [%s]", taskName), errChan...)
		}
	})

	return contexts
}

// ScheduleApplicationsWithScheduleOptions schedules *the* applications taking scheduleOptions as input and returns the scheduler.Contexts for each app (corresponds to a namespace). NOTE: does not wait for applications
func ScheduleApplicationsWithScheduleOptions(testname string, appSpec string, provisioner string, errChan ...*chan error) []*scheduler.Context {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	var contexts []*scheduler.Context
	var taskName string
	var err error
	options := scheduler.ScheduleOptions{
		AppKeys:            []string{appSpec},
		StorageProvisioner: provisioner,
	}
	taskName = fmt.Sprintf("%s", testname)
	contexts, err = Inst().S.Schedule(taskName, options)
	// Need to check err != nil before calling processError
	if err != nil {
		processError(err, errChan...)
	}
	if len(contexts) == 0 {
		processError(fmt.Errorf("list of contexts is empty for [%s]", taskName), errChan...)
	}

	return contexts
}

// ScheduleApplicationsOnNamespace ScheduleApplications schedules *the* applications and returns
// the scheduler.Contexts for each app (corresponds to given namespace). NOTE: does not wait for applications
func ScheduleApplicationsOnNamespace(namespace string, testname string, errChan ...*chan error) []*scheduler.Context {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	var contexts []*scheduler.Context
	var err error

	Step("schedule applications", func() {
		options := CreateScheduleOptions(namespace, errChan...)
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

// ValidateApplicationsStartData validates applications and start continous data injection to the same

func ValidateApplicationsStartData(contexts []*scheduler.Context, context context1.Context) (chan string, *errgroup.Group) {

	log.Infof("Is backup longevity run [%v]", IsBackupLongevityRun)
	// Skipping map reset in case of longevity run
	if !IsBackupLongevityRun {
		// Resetting the global map before starting the new App Validations
		NamespaceAppWithDataMap = make(map[string][]appDriver.ApplicationDriver)
	}

	log.InfoD("Validate applications")
	for _, ctx := range contexts {
		ValidateContext(ctx)
		appInfo, err := appUtils.ExtractConnectionInfo(ctx, context)
		if err != nil {
			log.InfoD("Some error occurred - [%s]", err)
		}
		log.InfoD("App Info - [%+v]", appInfo)
		if context == nil {
			log.Warnf("App Context is not proper - [%v]", context)
			continue
		}
		if appInfo.StartDataSupport {
			appHandler, err := appDriver.GetApplicationDriver(
				appInfo.AppType,
				appInfo.Hostname,
				appInfo.User,
				appInfo.Password,
				appInfo.Port,
				appInfo.DBName,
				appInfo.NodePort,
				appInfo.Namespace,
				appInfo.IPAddress,
				Inst().N)
			if err != nil {
				log.Infof("Error - %s", err.Error())
			}
			if appInfo.AppType == appType.Kubevirt && appInfo.StartDataSupport {
				err = appHandler.WaitForVMToBoot()
				log.FailOnError(err, "Some error occured while starting the VM")
			}
			log.InfoD("App handler created for [%s]", appInfo.Hostname)
			NamespaceAppWithDataMap[appInfo.Namespace] = append(NamespaceAppWithDataMap[appInfo.Namespace], appHandler)
		}
	}

	controlChannel := make(chan string)
	errGroup := errgroup.Group{}

	for _, allhandler := range NamespaceAppWithDataMap {
		for _, handler := range allhandler {
			currentHandler := handler
			errGroup.Go(func() error {
				err := currentHandler.StartData(controlChannel, context)
				return err
			})
		}
	}

	log.InfoD("Channel - [%v], errGroup - [%v]", controlChannel, &errGroup)

	return controlChannel, &errGroup
}

// StartVolDriverAndWait starts volume driver on given app nodes
func StartVolDriverAndWait(appNodes []node.Node, errChan ...*chan error) {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	Step(fmt.Sprintf("starting volume driver %s", Inst().V.String()), func() {
		stepLog := fmt.Sprintf("start volume driver on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.Infof(stepLog)
			for _, n := range appNodes {
				err := Inst().V.StartDriver(n)
				processError(err, errChan...)
			}
		})

		stepLog = fmt.Sprintf("wait for volume driver to start on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.Infof(stepLog)
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
	Step(fmt.Sprintf("stopping volume driver %s", Inst().V.String()), func() {
		stepLog := fmt.Sprintf("stop volume driver on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.Infof(stepLog)
			err := Inst().V.StopDriver(appNodes, false, nil)
			processError(err, errChan...)
		})

		stepLog = fmt.Sprintf("wait for volume driver to stop on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.Infof(stepLog)
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
	Step(fmt.Sprintf("crashing volume driver %s", Inst().V.String()), func() {
		stepLog := fmt.Sprintf("crash volume driver on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := Inst().V.StopDriver(appNodes, true, nil)
			processError(err, errChan...)
		})

		stepLog = fmt.Sprintf("wait for volume driver to start on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, n := range appNodes {
				err := Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
				processError(err, errChan...)
			}
		})

	})
}

// CrashPXDaemonAndWait crashes px daemon service on given app nodes and waits till driver is back up
func CrashPXDaemonAndWait(appNodes []node.Node, errChan ...*chan error) {
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()
	Step(fmt.Sprintf("crashing px daemon %s", Inst().V.String()), func() {
		stepLog := fmt.Sprintf("crash px daemon  on nodes: %v", appNodes)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := Inst().V.KillPXDaemon(appNodes, nil)
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

// RestartKubelet stops kubelet service on given app nodes and waits till kubelet is back up
func RestartKubelet(appNodes []node.Node, errChan ...*chan error) {
	if Inst().S.String() == openshift.SchedName && len(os.Getenv("TORPEDO_SSH_KEY")) == 0 {
		ginkgo.Skip("Cannot perform kubelet restart on openshift cluster without ssh key")
	}
	defer func() {
		if len(errChan) > 0 {
			close(*errChan[0])
		}
	}()

	nodeList, err := core.Instance().GetNodes()
	processError(err, errChan...)
	nodeSchedulableStatus := make(map[string]corev1.ConditionStatus)
	for _, k8sNode := range nodeList.Items {
		for _, status := range k8sNode.Status.Conditions {
			if status.Type == corev1.NodeReady {
				nodeSchedulableStatus[k8sNode.Name] = status.Status
			}
		}
	}

	for _, appNode := range appNodes {

		log.InfoD("Stopping kubelet service on node %s", appNode.Name)

		err := Inst().S.StopKubelet(appNode, node.SystemctlOpts{
			ConnectionOpts: node.ConnectionOpts{
				Timeout:         1 * time.Minute,
				TimeBeforeRetry: 10 * time.Second,
			}})
		processError(err, errChan...)
	}

	log.InfoD("Waiting for kubelet service to stop on the give nodes %v", appNodes)

	t := func() (interface{}, bool, error) {
		nodeList, err = core.Instance().GetNodes()
		if err != nil {
			return "", true, err
		}

		for _, appNode := range appNodes {
			for _, k8sNode := range nodeList.Items {
				if k8sNode.Name == appNode.Name {
					log.InfoD("Waiting for node [%s] in Not Ready state", appNode.Name)
					for _, status := range k8sNode.Status.Conditions {
						if status.Type == corev1.NodeReady {
							if status.Status == corev1.ConditionTrue {
								return "", true, fmt.Errorf("node [%s] is in Ready state, waiting for node to go down", appNode.Name)
							} else {
								if nodeSchedulableStatus[k8sNode.Name] == corev1.ConditionTrue {
									log.Infof("Node [%s] is in Not Ready state with status [%s]", appNode.Name, status.Status)
									nodeSchedulableStatus[k8sNode.Name] = status.Status
								}
								break
							}
						}
					}
				}
			}
		}

		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(t, 3*time.Minute, 10*time.Second)
	processError(err, errChan...)
	log.Infof("waiting for 5 mins before starting kubelet")
	time.Sleep(5 * time.Minute)

	waitTime := 3 * time.Minute

	if Inst().S.String() != aks.SchedName {
		for _, appNode := range appNodes {

			log.InfoD("Starting kubelet service on node %s", appNode.Name)
			err := Inst().S.StartKubelet(appNode, node.SystemctlOpts{
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         1 * time.Minute,
					TimeBeforeRetry: 10 * time.Second,
				}})
			processError(err, errChan...)
		}
	} else {
		waitTime = 20 * time.Minute
	}

	log.InfoD("Waiting for kubelet service to start on nodes %v", appNodes)
	t = func() (interface{}, bool, error) {
		nodeList, err = core.Instance().GetNodes()
		if err != nil {
			return "", true, err
		}

		for _, appNode := range appNodes {
			for _, k8sNode := range nodeList.Items {
				if k8sNode.Name == appNode.Name {
					log.InfoD("Waiting for node [%s] in Ready state", appNode.Name)
					for _, status := range k8sNode.Status.Conditions {
						if status.Type == corev1.NodeReady {
							if status.Status != corev1.ConditionTrue {
								return "", true, fmt.Errorf("node [%s] is in [%s] state, waiting for node to be Ready", appNode.Name, status.Status)
							} else {
								if nodeSchedulableStatus[k8sNode.Name] != corev1.ConditionTrue {
									log.Infof("Node [%s] is in Ready state with status [%s]", appNode.Name, status.Status)
									nodeSchedulableStatus[k8sNode.Name] = status.Status
								}
								break
							}
						}
					}
				}
			}
		}

		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(t, waitTime, 10*time.Second)
	processError(err, errChan...)

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

// DestroyApps destroy applications without validating them
func DestroyApps(contexts []*scheduler.Context, opts map[string]bool) {
	Step("destroy apps", func() {
		log.InfoD("Destroying apps")
		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})
}

// DestroyApps destroy applications with data validation
func DestroyAppsWithData(contexts []*scheduler.Context, opts map[string]bool, controlChannel chan string, errGroup *errgroup.Group) error {

	defer ginkgo.GinkgoRecover()
	var allErrors string

	log.InfoD("Validating apps data continuity")

	// Stopping all data flow to apps

	for _, appList := range NamespaceAppWithDataMap {
		for range appList {
			controlChannel <- "Stop"
		}
	}

	if err := errGroup.Wait(); err != nil {
		allErrors += err.Error()
	}

	close(controlChannel)

	log.InfoD("Destroying apps")
	for _, ctx := range contexts {
		// In case of tektoncd skip the volume validation
		if Contains(Inst().AppList, "tektoncd") {
			ctx.SkipVolumeValidation = true
		}
		TearDownContext(ctx, opts)
	}

	/* Removing Data error validation till PB-6271 is resolved.
	   if allErrors != "" {
	   	if IsReplacePolicySetToDelete {
	   		log.Infof("Skipping data continuity check as the replace policy was set to delete in this scenario")
	   		IsReplacePolicySetToDelete = false // Resetting replace policy for next testcase
	   		return nil
	   	} else {
	   		return fmt.Errorf("Data validation failed for apps. Error - [%s]", allErrors)
	   	}
	   }
	*/

	return nil
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

// ValidateRuleNotApplied is validating storage pool, but expects the error to occur
func ValidateRuleNotApplied(contexts []*scheduler.Context, name string) {

	strExpansionEnabled, err := Inst().V.IsStorageExpansionEnabled()
	expect(err).NotTo(haveOccurred())

	if strExpansionEnabled {
		log.InfoD("Validating Error shows up in events")
		fields := fmt.Sprintf("involvedObject.kind=AutopilotRule,involvedObject.name=%s", name)
		waitForActiveAction := func() (interface{}, bool, error) {
			events, err := k8sCore.ListEvents(defaultnamespace, metav1.ListOptions{FieldSelector: fields})
			expect(err).NotTo(haveOccurred())
			for _, e := range events.Items {
				if strings.Contains(e.Message, "ActiveActionsInProgress") {
					log.InfoD("Found Active action is in progress in autopilot events")
					return e.Message, false, nil
				}
			}
			return "", true, fmt.Errorf("Autopilot rule not applied yet")
		}
		_, err := task.DoRetryWithTimeout(waitForActiveAction, poolExpandApplyTimeOut, poolExpandApplyRetryTime)

		checkRuleFailed := func() (interface{}, bool, error) {
			events, err := k8sCore.ListEvents(defaultnamespace, metav1.ListOptions{FieldSelector: fields})
			expect(err).NotTo(haveOccurred())
			for _, e := range events.Items {
				if strings.Contains(e.Message, "cloud drive is not initialized") {
					log.InfoD("Found the error, stating cloud drive is not initialized")
					log.InfoD("Message in log is: %s", e.Message)
					return e.Message, false, nil
				}
			}
			return "", true, fmt.Errorf("Autopilot rule did not fail yet")
		}
		_, err = task.DoRetryWithTimeout(checkRuleFailed, poolExpandApplyTimeOut, poolExpandApplyRetryTime)
		expect(err).NotTo(haveOccurred())
	}
}

// ValidateRuleNotTriggered is validating PVC to see if rule is not triggered
func ValidateRuleNotTriggered(contexts []*scheduler.Context, name string) {
	log.InfoD("Validating rule is active")
	fields := fmt.Sprintf("involvedObject.kind=AutopilotRule,involvedObject.name=%s", name)
	waitForActiveAction := func() (interface{}, bool, error) {
		events, err := k8sCore.ListEvents(defaultnamespace, metav1.ListOptions{FieldSelector: fields})
		expect(err).NotTo(haveOccurred())
		for _, e := range events.Items {
			if strings.Contains(e.Message, "Initializing => Normal") {
				log.InfoD("Found rule which is initialized")
				return e.Message, false, nil
			}
		}
		return "", true, fmt.Errorf("Autopilot rule not applied yet")
	}
	_, err := task.DoRetryWithTimeout(waitForActiveAction, poolExpandApplyTimeOut, poolExpandApplyRetryTime)

	checkCount := 0
	checkRuleTriggered := func() (interface{}, bool, error) {
		events, err := k8sCore.ListEvents(defaultnamespace, metav1.ListOptions{FieldSelector: fields})
		expect(err).NotTo(haveOccurred())
		for _, e := range events.Items {
			if strings.Contains(e.Message, "Triggered") {
				log.InfoD("Message in log is: %s", e.Message)
				return e.Message, false, fmt.Errorf("Triggered found in Autopilot rule.")
			}
		}
		if checkCount <= 10 {
			checkCount += 1
			return "", true, fmt.Errorf("Autopilot rule did not trigger yet")
		} else {
			log.InfoD("Rule not triggered yet, which is expected")
			return "", false, nil
		}
	}
	_, err = task.DoRetryWithTimeout(checkRuleTriggered, poolExpandApplyTimeOut, poolExpandApplyRetryTime)
	expect(err).NotTo(haveOccurred())
}

func ToggleAutopilotInStc() error {
	stc, err := Inst().V.GetDriver()
	if err != nil {
		return err
	}
	log.Infof("is autopilot enabled?: %t", stc.Spec.Autopilot.Enabled)
	stc.Spec.Autopilot.Enabled = !stc.Spec.Autopilot.Enabled
	pxOperator := operator.Instance()
	_, err = pxOperator.UpdateStorageCluster(stc)
	if err != nil {
		return err
	}
	log.InfoD("Validating autopilot pod is deleted")
	checkPodIsDeleted := func() (interface{}, bool, error) {
		autopilotLabels := make(map[string]string)
		autopilotLabels["name"] = "autopilot"
		pods, err := k8sCore.GetPods(pxNamespace, autopilotLabels)
		expect(err).NotTo(haveOccurred())
		if stc.Spec.Autopilot.Enabled {
			log.Infof("autopilot is active, checking is pod is present.")
			if len(pods.Items) == 0 {
				return "", true, fmt.Errorf("autopilot pod is still not deployed")
			}
			return "autopilot pod deployed", false, nil
		} else {
			log.Infof("autopilot is inactive, checking if pod is deleted.")
			if len(pods.Items) > 0 {
				return "", true, fmt.Errorf("autopilot pod is still present")
			}
			return "autopilot pod is deleted", false, nil
		}
	}
	_, err = task.DoRetryWithTimeout(checkPodIsDeleted, poolExpandApplyTimeOut, poolExpandApplyRetryTime)
	expect(err).NotTo(haveOccurred())
	log.InfoD("Update STC, is AutopilotEnabled Now?: %t", stc.Spec.Autopilot.Enabled)
	return nil
}

func TogglePrometheusInStc() error {
	stc, err := Inst().V.GetDriver()
	if err != nil {
		return err
	}
	log.Infof("is prometheus enabled?: %t", stc.Spec.Monitoring.Prometheus.Enabled)
	stc.Spec.Monitoring.Prometheus.Enabled = !stc.Spec.Monitoring.Prometheus.Enabled
	pxOperator := operator.Instance()
	_, err = pxOperator.UpdateStorageCluster(stc)
	if err != nil {
		return err
	}
	log.InfoD("Validating prometheus pod is deleted")
	checkPodIsDeleted := func() (interface{}, bool, error) {
		prometheusLabels := make(map[string]string)
		prometheusLabels["app.kubernetes.io/name"] = "prometheus"
		pods, err := k8sCore.GetPods(pxNamespace, prometheusLabels)
		expect(err).NotTo(haveOccurred())
		if stc.Spec.Monitoring.Prometheus.Enabled {
			log.Infof("prometheus is active, checking is pod is present.")
			if len(pods.Items) == 0 {
				return "", true, fmt.Errorf("prometheus pod is still not deployed")
			}
			return "prometheus pod deployed", false, nil
		} else {
			log.Infof("prometheus is inactive, checking if pod is deleted.")
			if len(pods.Items) > 0 {
				return "", true, fmt.Errorf("prometheus pod is still present")
			}
			return "prometheus pod is deleted", false, nil
		}
	}
	_, err = task.DoRetryWithTimeout(checkPodIsDeleted, poolExpandApplyTimeOut, poolExpandApplyRetryTime)
	expect(err).NotTo(haveOccurred())
	log.InfoD("Update STC, is PrometheusEnabled Now?: %t", stc.Spec.Monitoring.Prometheus.Enabled)
	return nil
}

// ValidatePxPodRestartCount validates portworx restart count
func ValidatePxPodRestartCount(ctx *scheduler.Context, errChan ...*chan error) {
	Step("Validating portworx pods restart count ...", func() {
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
	Step("generating namespace info...", func() {
		Step(fmt.Sprintf("Describe Namespace objects for test %s \n", ginkgo.CurrentSpecReport().LeafNodeText), func() {
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
	zones, err := Inst().S.GetZones()
	log.FailOnError(err, "Zones empty")
	log.InfoD("ASG is running in [%+v] zones\n", zones)

	volDriverSpec, err := Inst().V.GetDriver()
	log.FailOnError(err, "error getting storage cluster volDriverSpec")
	perZoneCount := *volDriverSpec.Spec.CloudStorage.MaxStorageNodesPerZone

	// Validate total node count
	currentNodeCount, err := Inst().S.GetASGClusterSize()
	log.FailOnError(err, "Failed to Get ASG Cluster Size")

	if Inst().S.String() == openshift.SchedName || Inst().S.String() == anthos.SchedName {
		isPxOnMaster, err := IsPxRunningOnMaster()
		log.FailOnError(err, "Failed to check if px is running on master")
		if !isPxOnMaster {
			node.GetMasterNodes()
			//Removing master nodes for currentNodeCount
			currentNodeCount = currentNodeCount - int64(len(node.GetMasterNodes()))
		}
	}

	dash.VerifyFatal(currentNodeCount, count, "ASG cluster size is as expected?")

	// Validate storage node count
	totalStorageNodesAllowed := int(perZoneCount) * len(zones)
	expectedStoragesNodes := totalStorageNodesAllowed

	if totalStorageNodesAllowed > int(count) {
		expectedStoragesNodes = int(count)
	}
	storageNodes := node.GetStorageNodes()
	dash.VerifyFatal(len(storageNodes), expectedStoragesNodes, "Storage nodes matches the expected number?")
}

func IsPxRunningOnMaster() (bool, error) {

	var namespace string
	var err error
	if namespace, err = Inst().S.GetPortworxNamespace(); err != nil {
		log.Errorf("Failed to get portworx namespace. Error : %v", err)
		return false, nil
	}
	var isPXOnControlplane = false
	pxOperator := operator.Instance()
	stcList, err := pxOperator.ListStorageClusters(namespace)
	if err == nil {
		stc, err := pxOperator.GetStorageCluster(stcList.Items[0].Name, stcList.Items[0].Namespace)
		if err != nil {
			return false, fmt.Errorf("failed to get StorageCluster [%s] from namespace [%s], Err: %v", stcList.Items[0].Name, stcList.Items[0].Namespace, err.Error())
		}
		isPXOnControlplane, _ = strconv.ParseBool(stc.Annotations["portworx.io/run-on-master"])
	}

	return isPXOnControlplane, nil

}

// GetStorageNodes get storage nodes in the cluster
func GetStorageNodes() ([]node.Node, error) {

	var storageNodes []node.Node
	nodes := node.GetStorageDriverNodes()

	for _, n := range nodes {
		devices, err := Inst().V.GetStorageDevices(n)
		if err != nil {
			return nil, err
		}
		if len(devices) > 0 {
			storageNodes = append(storageNodes, n)
		}
	}
	return storageNodes, nil
}

// CollectSupport creates a support bundle
func CollectSupport() {
	Step("generating support bundle...", func() {
		log.InfoD("generating support bundle...")
		skipStr := os.Getenv(envSkipDiagCollection)
		skipSystemCheck := false

		if skipStr != "" {
			if skip, err := strconv.ParseBool(skipStr); err == nil && skip {
				skipSystemCheck = true
			}
		}

		if skipSystemCheck || Inst().SkipSystemChecks {
			log.Infof("skipping diag collection because env for skipping the check has been set to true")
			return
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

func runCmdOnce(cmd string, n node.Node) (string, error) {
	output, err := Inst().N.RunCommandWithNoRetry(n, cmd, node.ConnectionOpts{
		Timeout:         defaultCmdTimeout,
		TimeBeforeRetry: defaultCmdRetryInterval,
		Sudo:            true,
	})
	if err != nil {
		log.Warnf("failed to run cmd: %s. err: %v", cmd, err)
	}

	return output, err

}

func runCmdGetOutput(cmd string, n node.Node) (string, error) {
	output, err := Inst().N.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         defaultCmdTimeout,
		TimeBeforeRetry: defaultCmdRetryInterval,
		Sudo:            true,
	})
	if err != nil {
		log.Warnf("failed to run cmd: %s. err: %v", cmd, err)
	}
	return output, err
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

// runCmdOnceNonRoot runs a command once on given node as non-root user
func runCmdOnceNonRoot(cmd string, n node.Node) (string, error) {
	output, err := Inst().N.RunCommandWithNoRetry(n, cmd, node.ConnectionOpts{
		Timeout:         defaultCmdTimeout,
		TimeBeforeRetry: defaultCmdRetryInterval,
		Sudo:            false,
	})
	if err != nil {
		log.Warnf("failed to run cmd: %s. err: %v", cmd, err)
	}

	return output, err

}

// PerformSystemCheck check if core files are present on each node
func PerformSystemCheck() {
	Step("checking for core files...", func() {
		log.Info("checking for core files...")
		Step("verifying if core files are present on each node", func() {
			log.InfoD("verifying if core files are present on each node")
			nodes := node.GetNodes()
			dash.VerifyFatal(len(nodes) > 0, true, "verify nodes list is not empty")
			coreNodes := make([]string, 0)
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
					log.FailOnError(err, "error checking for cores in node %s", n.Name)
					log.Errorf("Core file was found on node %s, Core Path: %s", n.Name, file)
					coreNodes = append(coreNodes, n.Name)
				}
			}
			if len(coreNodes) > 0 {
				// Collect Support Bundle only once
				CollectSupport()
				log.Warn("Cores are generated. Please check logs for more details")
			}
			dash.VerifyFatal(len(coreNodes), 0, "verify if cores are generated in one or more nodes")
		})
	})
}

// ChangeNamespaces updates the namespace in supplied in-memory contexts.
// It does not apply changes on scheduler
func ChangeNamespaces(contexts []*scheduler.Context,
	namespaceMapping map[string]string) error {

	for _, ctx := range contexts {
		for _, spec := range ctx.App.SpecList {
			err := UpdateNamespace(spec, namespaceMapping)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// CloneSpec clones a given spec and returns it. It returns an error if the object (spec) provided is not supported by this function
func CloneSpec(spec interface{}) (interface{}, error) {
	if specObj, ok := spec.(*appsapi.Deployment); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*appsapi.StatefulSet); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*appsapi.DaemonSet); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*corev1.Service); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*corev1.PersistentVolumeClaim); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storageapi.StorageClass); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*snapv1.VolumeSnapshot); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storkapi.GroupVolumeSnapshot); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*corev1.Secret); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*corev1.ConfigMap); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storkapi.Rule); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*corev1.Pod); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storkapi.ClusterPair); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storkapi.Migration); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storkapi.MigrationSchedule); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storkapi.BackupLocation); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storkapi.ApplicationBackup); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storkapi.SchedulePolicy); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storkapi.ApplicationRestore); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storkapi.ApplicationClone); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storkapi.VolumeSnapshotRestore); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*apapi.AutopilotRule); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*corev1.ServiceAccount); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*rbacv1.Role); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*rbacv1.RoleBinding); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*rbacv1.ClusterRole); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*rbacv1.ClusterRoleBinding); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*batchv1beta1.CronJob); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*batchv1.Job); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*corev1.LimitRange); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*networkingv1beta1.Ingress); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*monitoringv1.Prometheus); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*monitoringv1.PrometheusRule); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*monitoringv1.ServiceMonitor); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*corev1.Namespace); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*apiextensionsv1beta1.CustomResourceDefinition); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*kubevirtv1.VirtualMachine); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*tektoncdv1.Pipeline); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*tektoncdv1.Task); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*tektoncdv1.PipelineRun); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*tektoncdv1.TaskRun); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*apiextensionsv1.CustomResourceDefinition); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*policyv1beta1.PodDisruptionBudget); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*netv1.NetworkPolicy); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*corev1.Endpoints); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*storkapi.ResourceTransformation); ok {
		clone := *specObj
		return &clone, nil
	} else if specObj, ok := spec.(*admissionregistrationv1.ValidatingWebhookConfiguration); ok {
		clone := *specObj
		webhooks := make([]admissionregistrationv1.ValidatingWebhook, 0)
		for i := range specObj.Webhooks {
			webhook := specObj.Webhooks[i]
			serviceClone := *specObj.Webhooks[i].ClientConfig.Service
			webhook.ClientConfig.Service = &serviceClone
			webhooks = append(webhooks, webhook)
		}
		clone.Webhooks = webhooks
		return &clone, nil
	}

	return nil, fmt.Errorf("unsupported object while cloning spec: %v", reflect.TypeOf(spec))
}

// UpdateNamespace updates the namespace for a given `spec` based on the `namespaceMapping` (which is a map of the map[old]new namespace). It returns an error if the object (spec) provided is not supported by this function. It silently fails if the `namespaceMapping` does not contain the mapping for the namespace present.
func UpdateNamespace(in interface{}, namespaceMapping map[string]string) error {
	if specObj, ok := in.(*appsapi.Deployment); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*appsapi.StatefulSet); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*appsapi.DaemonSet); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*corev1.Service); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*corev1.PersistentVolumeClaim); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*storageapi.StorageClass); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*snapv1.VolumeSnapshot); ok {
		namespace := namespaceMapping[specObj.Metadata.GetNamespace()]
		specObj.Metadata.SetNamespace(namespace)
		return nil
	} else if specObj, ok := in.(*storkapi.GroupVolumeSnapshot); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*corev1.Secret); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*corev1.ConfigMap); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*storkapi.Rule); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*corev1.Pod); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*storkapi.ClusterPair); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*storkapi.Migration); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*storkapi.MigrationSchedule); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*storkapi.BackupLocation); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*storkapi.ApplicationBackup); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*storkapi.SchedulePolicy); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*storkapi.ApplicationRestore); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*storkapi.ApplicationClone); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*storkapi.VolumeSnapshotRestore); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*apapi.AutopilotRule); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*corev1.ServiceAccount); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*rbacv1.Role); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*rbacv1.RoleBinding); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*rbacv1.ClusterRole); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*rbacv1.ClusterRoleBinding); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*batchv1beta1.CronJob); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*batchv1.Job); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*corev1.LimitRange); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*networkingv1beta1.Ingress); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*monitoringv1.Prometheus); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*monitoringv1.PrometheusRule); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*monitoringv1.ServiceMonitor); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*corev1.Namespace); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*apiextensionsv1beta1.CustomResourceDefinition); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*apiextensionsv1.CustomResourceDefinition); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*policyv1beta1.PodDisruptionBudget); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*netv1.NetworkPolicy); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*corev1.Endpoints); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*storkapi.ResourceTransformation); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*admissionregistrationv1.ValidatingWebhookConfiguration); ok {
		for i := range specObj.Webhooks {
			oldns := specObj.Webhooks[i].ClientConfig.Service.Namespace
			if namespace, ok := namespaceMapping[oldns]; ok {
				specObj.Webhooks[i].ClientConfig.Service.Namespace = namespace
			}
		}
		return nil
	} else if specObj, ok := in.(*kubevirtv1.VirtualMachine); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*tektoncdv1.Pipeline); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*tektoncdv1.PipelineRun); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*tektoncdv1.Task); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	} else if specObj, ok := in.(*tektoncdv1.TaskRun); ok {
		if namespace, ok := namespaceMapping[specObj.GetNamespace()]; ok {
			specObj.SetNamespace(namespace)
		}
		return nil
	}

	return fmt.Errorf("unsupported object while setting namespace: %v", reflect.TypeOf(in))
}

// GetSpecNameKindNamepace returns the (name, kind, namespace) for a given `spec`. It returns an error if the object (spec) provided is not supported by this function
func GetSpecNameKindNamepace(specObj interface{}) (string, string, string, error) {
	if obj, ok := specObj.(*appsapi.Deployment); ok {
		return obj.GetName(), obj.Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*appsapi.DaemonSet); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*corev1.Service); ok {
		return obj.GetName(), obj.Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {
		return obj.GetName(), obj.Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storageapi.StorageClass); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storkapi.GroupVolumeSnapshot); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*corev1.Secret); ok {
		return obj.GetName(), obj.Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*corev1.ConfigMap); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storkapi.Rule); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*corev1.Pod); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storkapi.ClusterPair); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storkapi.Migration); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storkapi.MigrationSchedule); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storkapi.BackupLocation); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storkapi.ApplicationBackup); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storkapi.SchedulePolicy); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storkapi.ApplicationRestore); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storkapi.ApplicationClone); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storkapi.VolumeSnapshotRestore); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
		return obj.Metadata.GetName(), obj.GroupVersionKind().Kind, obj.Metadata.GetNamespace(), nil
	} else if obj, ok := specObj.(*apapi.AutopilotRule); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*corev1.ServiceAccount); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*rbacv1.ClusterRole); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*rbacv1.ClusterRoleBinding); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*rbacv1.Role); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*rbacv1.RoleBinding); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*batchv1beta1.CronJob); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*batchv1.Job); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*corev1.LimitRange); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*networkingv1beta1.Ingress); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*monitoringv1.Prometheus); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*monitoringv1.PrometheusRule); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*monitoringv1.ServiceMonitor); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*corev1.Namespace); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*apiextensionsv1beta1.CustomResourceDefinition); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*apiextensionsv1.CustomResourceDefinition); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*policyv1beta1.PodDisruptionBudget); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*netv1.NetworkPolicy); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*corev1.Endpoints); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*storkapi.ResourceTransformation); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*admissionregistrationv1.ValidatingWebhookConfiguration); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*kubevirtv1.VirtualMachine); ok {
		return obj.GetName(), obj.GroupVersionKind().Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*tektoncdv1.Task); ok {
		return obj.GetName(), obj.Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*tektoncdv1.Pipeline); ok {
		return obj.GetName(), obj.Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*tektoncdv1.PipelineRun); ok {
		return obj.GetName(), obj.Kind, obj.GetNamespace(), nil
	} else if obj, ok := specObj.(*tektoncdv1.TaskRun); ok {
		return obj.GetName(), obj.Kind, obj.GetNamespace(), nil
	}

	return "", "", "", fmt.Errorf("unsupported object while obtaining spec details: %v", reflect.TypeOf(specObj))
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

// DeleteCloudCredentialWithContext deletes the cloud credential with the given context
func DeleteCloudCredentialWithContext(cloudCredName string, orgID string, cloudCredUID string, ctx context1.Context) error {
	backupDriver := Inst().Backup
	credDeleteRequest := &api.CloudCredentialDeleteRequest{
		Name:  cloudCredName,
		OrgId: orgID,
		Uid:   cloudCredUID,
	}
	_, err := backupDriver.DeleteCloudCredential(ctx, credDeleteRequest)
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
	currentSpecReport := ginkgo.CurrentSpecReport()
	if currentSpecReport.Failed() {
		log.Infof(">>>> FAILED TEST: %s", currentSpecReport.FullText())
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
			Status:          testStatus,
			TestID:          ids[0],
			RunID:           ids[1],
			DriverVersion:   driverVersion,
			PxBackupVersion: PxBackupVersion,
		}
		testrailuttils.AddTestEntry(testrailObject)
		log.Infof("Testrail testrun url: %s/index.php?/runs/view/%d&group_by=cases:custom_automated&group_order=asc&group_id=%d", testRailHostname, ids[1], testrailuttils.PwxProjectID)
	}
}

// SetClusterContext sets context to clusterConfigPath
func SetClusterContext(clusterConfigPath string) error {
	// an empty string indicates the default kubeconfig.
	// This variable is used to clearly indicate that in logs
	var clusterConfigPathForLog string
	var err error
	if clusterConfigPath == "" {
		clusterConfigPathForLog = "default"
	} else {
		clusterConfigPathForLog = clusterConfigPath
	}

	if clusterConfigPath == CurrentClusterConfigPath {
		log.InfoD("Switching context: The context is already [%s]", clusterConfigPathForLog)
		return nil
	}
	log.InfoD("Switching context to [%s]", clusterConfigPathForLog)
	provider := GetClusterProvider()
	if clusterConfigPath != "" {
		switch provider {
		case drivers.ProviderGke:
			err = Inst().S.SetGkeConfig(clusterConfigPath, GlobalGkeSecretString)
			if err != nil {
				return fmt.Errorf("failed to switch to context. Set Config Error: [%v]", err)
			}
		default:
			err = Inst().S.SetConfig(clusterConfigPath)
			if err != nil {
				return fmt.Errorf("failed to switch to context. Set Config Error: [%v]", err)
			}
		}
	} else {
		err = Inst().S.SetConfig(clusterConfigPath)
		if err != nil {
			return fmt.Errorf("failed to switch to context. Set Config Error: [%v]", err)
		}
	}
	if err != nil {
		return fmt.Errorf("failed to switch to context. Set Config Error: [%v]", err)
	}
	err = Inst().S.RefreshNodeRegistry()
	if err != nil {
		return fmt.Errorf("failed to switch to context. RefreshNodeRegistry Error: [%v]", err)
	}
	err = Inst().V.RefreshDriverEndpoints()
	if err != nil {
		return fmt.Errorf("failed to switch to context. RefreshDriverEndpoints Error: [%v]", err)
	}

	if sshNodeDriver, ok := Inst().N.(*ssh.SSH); ok {
		err = ssh.RefreshDriver(sshNodeDriver)
		if err != nil {
			return fmt.Errorf("failed to switch to context. RefreshDriver (Node) Error: [%v]", err)
		}
	} else if ibmNodeDriver, ok := Inst().N.(*ibm.Ibm); ok {
		err = ssh.RefreshDriver(&ibmNodeDriver.SSH)
		if err != nil {
			return fmt.Errorf("failed to switch to context. RefreshDriver (Node) Error: [%v]", err)
		}
	} else if gkeSchedDriver, ok := Inst().S.(*gke.Gke); ok {
		err = ssh.RefreshDriver(&gkeSchedDriver.SSH)
		if err != nil {
			return fmt.Errorf("failed to switch to context. RefreshDriver (gkeSchedDriver.SSH) Error: [%v]", err)
		}
	}

	CurrentClusterConfigPath = clusterConfigPath
	log.InfoD("Switched context to [%s]", clusterConfigPathForLog)
	// To update the rancher client for current cluster context
	if os.Getenv("CLUSTER_PROVIDER") == drivers.ProviderRke {
		if !strings.HasPrefix(clusterConfigPath, "/tmp/") {
			if clusterConfigPath == "" {
				err = Inst().S.(*rke.Rancher).UpdateRancherClient("source-config")
				if err != nil {
					return fmt.Errorf("failed to update rancher client for default source cluster context with error: [%v]", err)
				}
				return nil
			}
			return fmt.Errorf("invalid clusterConfigPath: %s for %s cluster provider", clusterConfigPath, drivers.ProviderRke)
		}
		err = Inst().S.(*rke.Rancher).UpdateRancherClient(strings.Split(clusterConfigPath, "/tmp/")[1])
		if err != nil {
			return fmt.Errorf("failed to update rancher client for %s with error: [%v]", clusterConfigPath, err)
		}
	}
	return nil
}

// SetSourceKubeConfig sets current context to the kubeconfig passed as source to the torpedo test
func SetSourceKubeConfig() error {
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	if err != nil {
		return err
	}
	return SetClusterContext(sourceClusterConfigPath)
}

// SetDestinationKubeConfig sets current context to the kubeconfig passed as destination to the torpedo test
func SetDestinationKubeConfig() error {
	destClusterConfigPath, err := GetDestinationClusterConfigPath()
	if err != nil {
		return err
	}
	return SetClusterContext(destClusterConfigPath)
}

func SetCustomKubeConfig(clusterConfigIndex int) error {
	customClusterConfigPath, err := GetCustomClusterConfigPath(clusterConfigIndex)
	if err != nil {
		return err
	}
	return SetClusterContext(customClusterConfigPath)
}

// ScheduleValidateClusterPair Schedule a clusterpair by creating a yaml file and validate it
func ScheduleValidateClusterPair(ctx *scheduler.Context, skipStorage, resetConfig bool, clusterPairDir string, reverse bool) error {
	var kubeConfigPath string
	var err error
	if reverse {
		err = SetSourceKubeConfig()
		if err != nil {
			return err
		}
		// get the kubeconfig path to get the correct pairing info
		kubeConfigPath, err = GetSourceClusterConfigPath()
		if err != nil {
			return err
		}
	} else {
		err = SetDestinationKubeConfig()
		if err != nil {
			return err
		}
		// get the kubeconfig path to get the correct pairing info
		kubeConfigPath, err = GetDestinationClusterConfigPath()
		if err != nil {
			return err
		}
	}

	pairInfo, err := Inst().V.GetClusterPairingInfo(kubeConfigPath, "", IsEksCluster(), reverse)
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
		err = SetDestinationKubeConfig()
		if err != nil {
			return err
		}
	} else {
		err = SetSourceKubeConfig()
		if err != nil {
			return err
		}
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
		err = SetSourceKubeConfig()
		if err != nil {
			return err
		}
	} else {
		// Change kubeconfig to destination cluster config
		err = SetDestinationKubeConfig()
		if err != nil {
			return err
		}
	}

	if skipStorage {
		log.Info("cluster-pair.yml created")
		return nil
	}

	return addStorageOptions(pairInfo, clusterPairFileName)
}

func ScheduleBidirectionalClusterPair(cpName, cpNamespace, projectMappings string, objectStoreType storkv1.BackupLocationType, secretName string, mode string, sourceCluster int, destCluster int) (err error) {
	//var token string
	// Setting kubeconfig to source because we will create bidirectional cluster pair based on source as reference
	err = SetCustomKubeConfig(sourceCluster)
	if err != nil {
		return err
	}

	// Create namespace for the cluster pair on source cluster
	_, err = core.Instance().CreateNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: cpNamespace,
			Labels: map[string]string{
				"creator": "stork-test",
			},
		},
	})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return fmt.Errorf("Failed to create namespace %s on source cluster", cpNamespace)
	}

	srcKubeConfigPath, err := GetCustomClusterConfigPath(sourceCluster)
	if err != nil {
		return fmt.Errorf("Failed to get config path for source cluster")
	}

	defer func() {
		var config *rest.Config
		config, err = clientcmd.BuildConfigFromFlags("", srcKubeConfigPath)
		if err != nil {
			return
		}
		core.Instance().SetConfig(config)
		apps.Instance().SetConfig(config)
		stork.Instance().SetConfig(config)
	}()

	err = SetCustomKubeConfig(destCluster)
	if err != nil {
		return err
	}

	// Create namespace for the cluster pair on destination cluster
	_, err = core.Instance().CreateNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: cpNamespace,
			Labels: map[string]string{
				"creator": "stork-test",
			},
		},
	})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return fmt.Errorf("Failed to create namespace %s on destination cluster", cpNamespace)
	}

	destKubeConfigPath, err := GetCustomClusterConfigPath(destCluster)
	if err != nil {
		return fmt.Errorf("Failed to get config path for destination cluster")
	}

	err = SetCustomKubeConfig(sourceCluster)
	if err != nil {
		return err
	}

	// Create source --> destination and destination --> cluster pairs using storkctl
	factory := storkctl.NewFactory()
	cmd := storkctl.NewCommand(factory, os.Stdin, os.Stdout, os.Stderr)
	cmdArgs := []string{"create", "clusterpair", "-n", cpNamespace, cpName,
		"--kubeconfig", srcKubeConfigPath,
		"--src-kube-file", srcKubeConfigPath,
		"--dest-kube-file", destKubeConfigPath,
	}

	if mode == "sync-dr" {
		cmdArgs = []string{"create", "clusterpair", "-n", cpNamespace, cpName,
			"--kubeconfig", srcKubeConfigPath,
			"--src-kube-file", srcKubeConfigPath,
			"--dest-kube-file", destKubeConfigPath,
			"--mode", "sync-dr",
		}
	}

	if projectMappings != "" {
		cmdArgs = append(cmdArgs, "--project-mappings")
		cmdArgs = append(cmdArgs, projectMappings)
	}

	// Get external object store details and append to the command accordingily
	if objectStoreType != "" {
		// Get external object store details and append to the command accordingily
		objectStoreArgs, err := getObjectStoreArgs(objectStoreType, secretName)
		if err != nil {
			return fmt.Errorf("failed to get  %s secret in configmap secret-config in default namespace", objectStoreType)
		}
		cmdArgs = append(cmdArgs, objectStoreArgs...)
	}

	cmd.SetArgs(cmdArgs)
	log.InfoD("Following is the bidirectional command: %v", cmdArgs)
	if err := cmd.Execute(); err != nil {
		return fmt.Errorf("Creation of bidirectional cluster pair using storkctl failed: %v", err)
	}

	return nil
}

func getObjectStoreArgs(objectStoreType storkv1.BackupLocationType, secretName string) ([]string, error) {
	var objectStoreArgs []string
	secretData, err := core.Instance().GetSecret(secretName, "default")
	if err != nil {
		return objectStoreArgs, fmt.Errorf("error getting secret %s in default namespace: %v", secretName, err)
	}
	if objectStoreType == storkv1.BackupLocationS3 {
		objectStoreArgs = append(objectStoreArgs,
			[]string{"--provider", "s3",
				"--s3-access-key", string(secretData.Data["accessKeyID"]),
				"--s3-secret-key", string(secretData.Data["secretAccessKey"]),
				"--s3-region", string(secretData.Data["region"]),
				"--s3-endpoint", string(secretData.Data["endpoint"]),
			}...)
		if val, ok := secretData.Data["disableSSL"]; ok && string(val) == "true" {
			objectStoreArgs = append(objectStoreArgs, "--disable-ssl")
		}
		if val, ok := secretData.Data["encryptionKey"]; ok && len(val) > 0 {
			objectStoreArgs = append(objectStoreArgs, "--encryption-key")
			objectStoreArgs = append(objectStoreArgs, string(val))
		}
	} else if objectStoreType == storkv1.BackupLocationAzure {
		objectStoreArgs = append(objectStoreArgs,
			[]string{"--provider", "azure", "--azure-account-name", string(secretData.Data["storageAccountName"]),
				"--azure-account-key", string(secretData.Data["storageAccountKey"])}...)
		if val, ok := secretData.Data["encryptionKey"]; ok && len(val) > 0 {
			objectStoreArgs = append(objectStoreArgs, "--encryption-key")
			objectStoreArgs = append(objectStoreArgs, string(val))
		}
	} else if objectStoreType == storkv1.BackupLocationGoogle {
		objectStoreArgs = append(objectStoreArgs,
			[]string{"--provider", "google", "--google-project-id", string(secretData.Data["projectID"]), "--google-key-file-path", string(secretData.Data["accountKey"])}...)
		if val, ok := secretData.Data["encryptionKey"]; ok && len(val) > 0 {
			objectStoreArgs = append(objectStoreArgs, "--encryption-key")
			objectStoreArgs = append(objectStoreArgs, string(val))
		}
	}

	return objectStoreArgs, nil
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
			namespace := ctx.App.SpecList[0].(*corev1.PersistentVolumeClaim).Namespace
			if err, ok := bkpErrors[namespace]; ok {
				log.Infof("Skipping validating namespace %s because %s", namespace, err)
			} else {
				Step(fmt.Sprintf("For validation of %s app", ctx.App.Key), func() {

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
	if obj, ok := spec.(*corev1.PersistentVolumeClaim); ok {
		if obj.Labels != nil {
			_, ok := obj.Labels[key]
			if ok {
				log.Infof("Deleting label with key [%s] from PVC %s", key, obj.Name)
				delete(obj.Labels, key)
				core.Instance().UpdatePersistentVolumeClaim(obj)
			}
		}
	} else if obj, ok := spec.(*corev1.ConfigMap); ok {
		if obj.Labels != nil {
			_, ok := obj.Labels[key]
			if ok {
				log.Infof("Deleting label with key [%s] from ConfigMap %s", key, obj.Name)
				delete(obj.Labels, key)
				core.Instance().UpdateConfigMap(obj)
			}
		}
	} else if obj, ok := spec.(*corev1.Secret); ok {
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

// DeleteRestore deletes restore
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

// DeleteRestoreWithUID deletes restore with the given restore name and uid
func DeleteRestoreWithUID(restoreName string, restoreUID string, orgID string, ctx context1.Context) error {
	deleteRestoreReq := &api.RestoreDeleteRequest{
		Name:  restoreName,
		Uid:   restoreUID,
		OrgId: orgID,
	}
	_, err := Inst().Backup.DeleteRestore(ctx, deleteRestoreReq)
	return err
}

// DeleteBackup deletes a backup with the given backup reference without checking the cluster reference, suitable for normal backup deletion where the cluster reference is not needed.
func DeleteBackup(backupName string, backupUID string, orgID string, ctx context1.Context) (*api.BackupDeleteResponse, error) {
	var err error
	var backupObj *api.BackupObject
	var backupDeleteResponse *api.BackupDeleteResponse

	backupDriver := Inst().Backup

	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID}
	curBackups, err := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	if err != nil {
		return backupDeleteResponse, err
	}
	for _, bkp := range curBackups.GetBackups() {
		if bkp.Uid == backupUID {
			backupObj = bkp
			break
		}
	}
	if backupObj == nil {
		return nil, fmt.Errorf("unable to find backup [%s] with uid [%s]", backupName, backupUID)
	}

	bkpDeleteRequest := &api.BackupDeleteRequest{
		Name:  backupName,
		OrgId: orgID,
		Uid:   backupUID,
	}
	backupDeleteResponse, err = backupDriver.DeleteBackup(ctx, bkpDeleteRequest)
	return backupDeleteResponse, err
}

// DeleteBackupWithClusterUID deletes a backup using the specified cluster name and UID, ensuring the cluster reference is checked before deletion, suitable for cases where the cluster reference is necessary (e.g., for same-name or deleted clusters).
func DeleteBackupWithClusterUID(backupName string, backupUID string, clusterName string, clusterUid string, orgID string, ctx context1.Context) (*api.BackupDeleteResponse, error) {
	backupDeleteRequest := &api.BackupDeleteRequest{
		Name:  backupName,
		OrgId: orgID,
		Uid:   backupUID,
		ClusterRef: &api.ObjectRef{
			Name: clusterName,
			Uid:  clusterUid,
		},
	}
	backupDeleteResponse, err := Inst().Backup.DeleteBackup(ctx, backupDeleteRequest)
	if err != nil {
		return nil, err
	}
	return backupDeleteResponse, nil
}

// DeleteCluster deletes/de-registers cluster from px-backup
func DeleteCluster(name string, orgID string, ctx context1.Context, cleanupBackupsRestores bool) error {
	backupDriver := Inst().Backup
	clusterUid, err := backupDriver.GetClusterUID(ctx, orgID, name)
	if err != nil {
		return err
	}
	clusterDeleteReq := &api.ClusterDeleteRequest{
		OrgId:          orgID,
		Name:           name,
		DeleteBackups:  cleanupBackupsRestores,
		DeleteRestores: cleanupBackupsRestores,
		Uid:            clusterUid,
	}
	_, err = backupDriver.DeleteCluster(ctx, clusterDeleteReq)
	return err
}

// DeleteClusterWithUID deletes cluster with the given cluster name and uid
func DeleteClusterWithUID(name string, uid string, orgID string, ctx context1.Context, cleanupBackupsRestores bool) error {
	backupDriver := Inst().Backup
	clusterDeleteReq := &api.ClusterDeleteRequest{
		OrgId:          orgID,
		Name:           name,
		Uid:            uid,
		DeleteBackups:  cleanupBackupsRestores,
		DeleteRestores: cleanupBackupsRestores,
	}
	_, err := backupDriver.DeleteCluster(ctx, clusterDeleteReq)
	if err != nil {
		return err
	}
	err = backupDriver.WaitForClusterDeletionWithUID(ctx, name, uid, orgID, clusterDeleteTimeout, clusterDeleteRetryTime)
	if err != nil {
		return err
	}
	return nil
}

// DeleteBackupLocation deletes backup location
func DeleteBackupLocation(name string, backupLocationUID string, orgID string, DeleteExistingBackups bool) error {

	backupDriver := Inst().Backup
	bLocationDeleteReq := &api.BackupLocationDeleteRequest{
		Name:          name,
		OrgId:         orgID,
		DeleteBackups: DeleteExistingBackups,
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

// DeleteBackupLocationWithContext deletes backup location with the given context
func DeleteBackupLocationWithContext(name string, backupLocationUID string, orgID string, DeleteExistingBackups bool, ctx context1.Context) error {
	backupDriver := Inst().Backup
	bLocationDeleteReq := &api.BackupLocationDeleteRequest{
		Name:          name,
		Uid:           backupLocationUID,
		OrgId:         orgID,
		DeleteBackups: DeleteExistingBackups,
	}
	_, err := backupDriver.DeleteBackupLocation(ctx, bLocationDeleteReq)
	if err != nil {
		return err
	}
	return nil
}

// DeleteSchedule deletes backup schedule
func DeleteSchedule(backupScheduleName string, clusterName string, orgID string, ctx context1.Context) error {
	backupDriver := Inst().Backup
	backupScheduleInspectRequest := &api.BackupScheduleInspectRequest{
		Name:  backupScheduleName,
		Uid:   "",
		OrgId: orgID,
	}
	resp, err := backupDriver.InspectBackupSchedule(ctx, backupScheduleInspectRequest)
	if err != nil {
		return err
	}
	backupScheduleUID := resp.GetBackupSchedule().GetUid()
	bkpScheduleDeleteRequest := &api.BackupScheduleDeleteRequest{
		OrgId: orgID,
		Name:  backupScheduleName,
		// DeleteBackups indicates whether the cloud backup files need to
		// be deleted or retained.
		DeleteBackups: true,
		Uid:           backupScheduleUID,
	}
	_, err = backupDriver.DeleteBackupSchedule(ctx, bkpScheduleDeleteRequest)
	if err != nil {
		return err
	}
	clusterUID, err := backupDriver.GetClusterUID(ctx, orgID, clusterName)
	if err != nil {
		return err
	}
	clusterReq := &api.ClusterInspectRequest{OrgId: orgID, Name: clusterName, IncludeSecrets: true, Uid: clusterUID}
	clusterResp, err := backupDriver.InspectCluster(ctx, clusterReq)
	if err != nil {
		return err
	}
	clusterObj := clusterResp.GetCluster()
	namespace := "*"
	err = backupDriver.WaitForBackupScheduleDeletion(ctx, backupScheduleName, namespace, orgID,
		clusterObj,
		backupLocationDeleteTimeoutMin*time.Minute,
		RetrySeconds*time.Second)
	if err != nil {
		return err
	}
	return nil
}

// DeleteScheduleWithUID deletes backup schedule with the given backup schedule name and uid
func DeleteScheduleWithUID(backupScheduleName string, backupScheduleUid string, orgID string, ctx context1.Context) error {
	backupDriver := Inst().Backup
	bkpScheduleDeleteRequest := &api.BackupScheduleDeleteRequest{
		OrgId: orgID,
		Name:  backupScheduleName,
		// DeleteBackups indicates whether the cloud backup files need to
		// be deleted or retained.
		DeleteBackups: true,
		Uid:           backupScheduleUid,
	}
	_, err := backupDriver.DeleteBackupSchedule(ctx, bkpScheduleDeleteRequest)
	if err != nil {
		return err
	}
	return nil
}

// DeleteScheduleWithUIDAndWait deletes backup schedule with the given backup schedule name and uid and waits for its deletion
func DeleteScheduleWithUIDAndWait(backupScheduleName string, backupScheduleUid string, clusterName string, clusterUid string, orgID string, ctx context1.Context) error {
	backupDriver := Inst().Backup
	bkpScheduleDeleteRequest := &api.BackupScheduleDeleteRequest{
		OrgId: orgID,
		Name:  backupScheduleName,
		// DeleteBackups indicates whether the cloud backup files need to
		// be deleted or retained.
		DeleteBackups: true,
		Uid:           backupScheduleUid,
	}
	_, err := backupDriver.DeleteBackupSchedule(ctx, bkpScheduleDeleteRequest)
	clusterReq := &api.ClusterInspectRequest{
		OrgId:          orgID,
		Name:           clusterName,
		Uid:            clusterUid,
		IncludeSecrets: true,
	}
	clusterResp, err := backupDriver.InspectCluster(ctx, clusterReq)
	if err != nil {
		return err
	}
	clusterObj := clusterResp.GetCluster()
	namespace := "*"
	err = backupDriver.WaitForBackupScheduleDeletion(
		ctx, backupScheduleName, namespace, orgID, clusterObj,
		backupLocationDeleteTimeoutMin*time.Minute,
		RetrySeconds*time.Second,
	)
	if err != nil {
		return err
	}
	return nil
}

// CreateApplicationClusters adds application clusters to backup
// 1st cluster in KUBECONFIGS ENV var is source cluster while
// 2nd cluster is destination cluster
func CreateApplicationClusters(orgID string, cloudName string, uid string, ctx context1.Context) error {
	var clusterCredName string
	var clusterCredUid string
	kubeconfigs := os.Getenv("KUBECONFIGS")
	dash.VerifyFatal(kubeconfigs != "", true, "Getting KUBECONFIGS Environment variable")
	kubeconfigList := strings.Split(kubeconfigs, ",")
	// Validate user has provided at least 2 kubeconfigs for source and destination cluster
	if len(kubeconfigList) < 2 {
		return fmt.Errorf("minimum 2 kubeconfigs are required for source and destination cluster")
	}
	err := dumpKubeConfigs(configMapName, kubeconfigList)
	if err != nil {
		return err
	}
	log.InfoD("Create cluster [%s] in org [%s]", SourceClusterName, orgID)
	srcClusterConfigPath, err := GetSourceClusterConfigPath()
	if err != nil {
		return err
	}
	log.Infof("Save cluster %s kubeconfig to %s", SourceClusterName, srcClusterConfigPath)

	log.InfoD("Create cluster [%s] in org [%s]", destinationClusterName, orgID)
	dstClusterConfigPath, err := GetDestinationClusterConfigPath()
	if err != nil {
		return err
	}
	log.Infof("Save cluster %s kubeconfig to %s", destinationClusterName, dstClusterConfigPath)

	ClusterConfigPathMap[SourceClusterName] = srcClusterConfigPath
	ClusterConfigPathMap[destinationClusterName] = dstClusterConfigPath

	clusterCreation := func(clusterCredName string, clusterCredUid string, clusterName string) error {
		clusterStatus := func() (interface{}, bool, error) {
			err = CreateCluster(clusterName, ClusterConfigPathMap[clusterName], orgID, clusterCredName, clusterCredUid, ctx)
			if err != nil && !strings.Contains(err.Error(), "already exists with status: Online") {
				return "", true, err
			}
			srcClusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			if err != nil {
				return "", true, err
			}
			if srcClusterStatus == api.ClusterInfo_StatusInfo_Online {
				return "", false, nil
			}
			return "", true, fmt.Errorf("the %s cluster state is not Online yet", SourceClusterName)
		}
		_, err = task.DoRetryWithTimeout(clusterStatus, clusterCreationTimeout, clusterCreationRetryTime)
		if err != nil {
			return err
		}
		return nil
	}
	clusterProvider := GetClusterProviders()
	for _, provider := range clusterProvider {
		switch provider {
		case drivers.ProviderRke:
			for _, kubeconfig := range kubeconfigList {
				clusterCredName = fmt.Sprintf("%v-%v-cloud-cred-%v", provider, kubeconfig, RandomString(5))
				clusterCredUid = uuid.New()
				log.Infof("Creating cloud credential for cluster")
				err = CreateCloudCredential(provider, clusterCredName, clusterCredUid, orgID, ctx, kubeconfig)
				if err != nil {
					if strings.Contains(err.Error(), CreateCloudCredentialError) {
						log.Infof("The error is - %v", err.Error())
						adminCtx, err := backup.GetAdminCtxFromSecret()
						if err != nil {
							return fmt.Errorf("failed to fetch px-central-admin ctx with error %v", err)
						}
						log.Infof("Creating cloud credential %s from admin context and sharing with all the users", clusterCredName)
						err = CreateCloudCredential(provider, clusterCredName, clusterCredUid, orgID, adminCtx, kubeconfig)
						if err != nil {
							return fmt.Errorf("failed to create cloud cred %s with error %v", clusterCredName, err)
						}
						err = AddCloudCredentialOwnership(clusterCredName, clusterCredUid, nil, nil, Invalid, Read, adminCtx, orgID)
						if err != nil {
							return fmt.Errorf("failed to share the cloud cred with error %v", err)
						}
					} else {
						return fmt.Errorf("failed to create cloud cred with error =%v", err)
					}
				}
				clusterName := strings.Split(kubeconfig, "-")[0] + "-cluster"
				err = clusterCreation(clusterCredName, clusterCredUid, clusterName)
				if err != nil {
					return err
				}
			}
		case drivers.ProviderAzure:
			for _, kubeconfig := range kubeconfigList {
				clusterCredName = fmt.Sprintf("%v-%v-cloud-cred-%v", provider, kubeconfig, RandomString(5))
				clusterCredUid = uuid.New()
				log.Infof("Creating cloud credential for cluster")
				err = CreateCloudCredential(provider, clusterCredName, clusterCredUid, orgID, ctx)
				if err != nil {
					if strings.Contains(err.Error(), CreateCloudCredentialError) {
						log.Infof("The error is - %v", err.Error())
						adminCtx, err := backup.GetAdminCtxFromSecret()
						if err != nil {
							return fmt.Errorf("failed to fetch px-central-admin ctx with error %v", err)
						}
						log.Infof("Creating cloud credential %s from admin context and sharing with all the users", clusterCredName)
						err = CreateCloudCredential(provider, clusterCredName, clusterCredUid, orgID, adminCtx, kubeconfig)
						if err != nil {
							return fmt.Errorf("failed to create cloud cred %s with error %v", clusterCredName, err)
						}
						err = AddCloudCredentialOwnership(clusterCredName, clusterCredUid, nil, nil, 0, Read, adminCtx, orgID)
						if err != nil {
							return fmt.Errorf("failed to share the cloud cred with error %v", err)
						}
					} else {
						return fmt.Errorf("failed to create cloud cred with error =%v", err)
					}
				} else {
					log.Infof("Created cloud cred %s for cluster creation", clusterCredName)
				}
				clusterName := strings.Split(kubeconfig, "-")[0] + "-cluster"
				err = clusterCreation(clusterCredName, clusterCredUid, clusterName)
				if err != nil {
					return err
				}
			}
		case drivers.ProviderAws:
			for _, kubeconfig := range kubeconfigList {
				clusterCredName = fmt.Sprintf("%v-%v-cloud-cred-%v", provider, kubeconfig, RandomString(5))
				clusterCredUid = uuid.New()
				log.Infof("Creating cloud credential for cluster")
				err = CreateCloudCredential(provider, clusterCredName, clusterCredUid, orgID, ctx)
				if err != nil {
					if strings.Contains(err.Error(), CreateCloudCredentialError) {
						log.Infof("The error is - %v", err.Error())
						adminCtx, err := backup.GetAdminCtxFromSecret()
						if err != nil {
							return fmt.Errorf("failed to fetch px-central-admin ctx with error %v", err)
						}
						log.Infof("Creating cloud credential %s from admin context and sharing with all the users", clusterCredName)
						err = CreateCloudCredential(provider, clusterCredName, clusterCredUid, orgID, adminCtx, kubeconfig)
						if err != nil {
							return fmt.Errorf("failed to create cloud cred %s with error %v", clusterCredName, err)
						}
						err = AddCloudCredentialOwnership(clusterCredName, clusterCredUid, nil, nil, 0, Read, adminCtx, orgID)
						if err != nil {
							return fmt.Errorf("failed to share the cloud cred with error %v", err)
						}
					} else {
						return fmt.Errorf("failed to create cloud cred with error =%v", err)
					}
				}
				clusterName := strings.Split(kubeconfig, "-")[0] + "-cluster"
				err = clusterCreation(clusterCredName, clusterCredUid, clusterName)
				if err != nil {
					return err
				}
			}
		case drivers.ProviderGke:
			for _, kubeconfig := range kubeconfigList {
				clusterCredName = fmt.Sprintf("%v-%v-cloud-cred-%v", provider, kubeconfig, RandomString(5))
				clusterCredUid = uuid.New()
				log.Infof("Creating cloud credential for cluster")
				err = CreateCloudCredential(provider, clusterCredName, clusterCredUid, orgID, ctx, kubeconfig)
				if err != nil {
					if strings.Contains(err.Error(), CreateCloudCredentialError) {
						log.Infof("The error is - %v", err.Error())
						adminCtx, err := backup.GetAdminCtxFromSecret()
						if err != nil {
							return fmt.Errorf("failed to fetch px-central-admin ctx with error %v", err)
						}
						log.Infof("Creating cloud credential %s from admin context and sharing with all the users", clusterCredName)
						err = CreateCloudCredential(provider, clusterCredName, clusterCredUid, orgID, adminCtx, kubeconfig)
						if err != nil {
							return fmt.Errorf("failed to create cloud cred %s with error %v", clusterCredName, err)
						}
						err = AddCloudCredentialOwnership(clusterCredName, clusterCredUid, nil, nil, 0, Read, adminCtx, orgID)
						if err != nil {
							return fmt.Errorf("failed to share the cloud cred with error %v", err)
						}
					} else {
						return fmt.Errorf("failed to create cloud cred with error =%v", err)
					}
				}
				clusterName := strings.Split(kubeconfig, "-")[0] + "-cluster"
				err = clusterCreation(clusterCredName, clusterCredUid, clusterName)
				if err != nil {
					return err
				}
			}
		case drivers.ProviderIbm:
			for _, kubeconfig := range kubeconfigList {
				clusterCredName = fmt.Sprintf("%v-%v-cloud-cred-%v", provider, kubeconfig, RandomString(5))
				clusterCredUid = uuid.New()
				log.Infof("Cluster credential with name [%s] for IBM", clusterCredName)
				err = CreateCloudCredential(provider, clusterCredName, clusterCredUid, orgID, ctx, kubeconfig)
				if err != nil {
					if strings.Contains(err.Error(), CreateCloudCredentialError) {
						log.Infof("The error is - %v", err.Error())
						adminCtx, err := backup.GetAdminCtxFromSecret()
						if err != nil {
							return fmt.Errorf("failed to fetch px-central-admin ctx with error %v", err)
						}
						log.Infof("Creating cloud credential %s from admin context and sharing with all the users", clusterCredName)
						err = CreateCloudCredential(provider, clusterCredName, clusterCredUid, orgID, adminCtx, kubeconfig)
						if err != nil {
							return fmt.Errorf("failed to create cloud cred %s with error %v", clusterCredName, err)
						}
						err = AddCloudCredentialOwnership(clusterCredName, clusterCredUid, nil, nil, 0, Read, adminCtx, orgID)
						if err != nil {
							return fmt.Errorf("failed to share the cloud cred with error %v", err)
						}
					} else {
						return fmt.Errorf("failed to create cloud cred with error =%v", err)
					}
				}
				clusterName := strings.Split(kubeconfig, "-")[0] + "-cluster"
				err = clusterCreation(clusterCredName, clusterCredUid, clusterName)
				if err != nil {
					return err
				}
			}
		default:
			for _, kubeconfig := range kubeconfigList {
				clusterName := strings.Split(kubeconfig, "-")[0] + "-cluster"
				err = clusterCreation(clusterCredName, clusterCredUid, clusterName)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// CreateBackupLocation creates backup location
func CreateBackupLocation(provider, name, uid, credName, credUID, bucketName, orgID, encryptionKey string, validate bool) error {
	var err error
	switch provider {
	case drivers.ProviderAws:
		err = CreateS3BackupLocation(name, uid, credName, credUID, bucketName, orgID, encryptionKey, validate)
	case drivers.ProviderAzure:
		err = CreateAzureBackupLocation(name, uid, credName, credUID, bucketName, orgID, validate)
	case drivers.ProviderGke:
		err = CreateGCPBackupLocation(name, uid, credName, credUID, bucketName, orgID, validate)
	case drivers.ProviderNfs:
		err = CreateNFSBackupLocation(name, uid, orgID, encryptionKey, bucketName, validate)
	}
	return err
}

// CreateBackupLocationWithContext creates backup location using the given context
func CreateBackupLocationWithContext(provider, name, uid, credName, credUID, bucketName, orgID, encryptionKey string, ctx context1.Context, validate bool) error {
	var err error
	switch provider {
	case drivers.ProviderAws:
		err = CreateS3BackupLocationWithContext(name, uid, credName, credUID, bucketName, orgID, encryptionKey, ctx, validate)
	case drivers.ProviderAzure:
		err = CreateAzureBackupLocationWithContext(name, uid, credName, credUID, bucketName, orgID, encryptionKey, ctx, validate)
	case drivers.ProviderGke:
		err = CreateGCPBackupLocationWithContext(name, uid, credName, credUID, bucketName, orgID, ctx, validate)
	case drivers.ProviderNfs:
		err = CreateNFSBackupLocationWithContext(name, uid, bucketName, orgID, encryptionKey, ctx, validate)
	}
	return err
}

// UpdateBackupLocation updates s3 backup location with the provided values
func UpdateBackupLocation(provider string, name string, uid string, orgID string, cloudCred string, cloudCredUID string, ctx context1.Context, sseS3EncryptionType api.S3Config_Sse) error {
	var err error
	switch provider {
	case drivers.ProviderAws:
		err = UpdateS3BackupLocation(name, uid, orgID, cloudCred, cloudCredUID, ctx, sseS3EncryptionType, false)
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
		clusterProvider := GetClusterProviders()
		for _, provider := range clusterProvider {
			switch provider {
			case drivers.ProviderRke:
				clusterCreateReq = &api.ClusterCreateRequest{
					CreateMetadata: &api.CreateMetadata{
						Name:  name,
						OrgId: orgID,
					},
					Kubeconfig: base64.StdEncoding.EncodeToString(kubeconfigRaw),
					PlatformCredentialRef: &api.ObjectRef{
						Name: cloud_name,
						Uid:  uid,
					},
				}
			case drivers.ProviderGke:
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
			default:
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
			}
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

// CreateCloudCredential creates cloud credentials
func CreateCloudCredential(provider, credName string, uid, orgID string, ctx context1.Context, kubeconfig ...string) error {
	log.Infof("Create cloud credential with name [%s] for org [%s] with [%s] as provider", credName, orgID, provider)
	var credCreateRequest *api.CloudCredentialCreateRequest
	switch provider {
	case drivers.ProviderAws:
		log.Infof("Create creds for Aws")
		id := os.Getenv("AWS_ACCESS_KEY_ID")
		if id == "" {
			return fmt.Errorf("environment variable AWS_ACCESS_KEY_ID should not be empty")
		}
		secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
		if secret == "" {
			return fmt.Errorf("environment variable AWS_SECRET_ACCESS_KEY should not be empty")
		}
		credCreateRequest = &api.CloudCredentialCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  credName,
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

	case drivers.ProviderAzure:
		log.Infof("Create creds for azure")
		tenantID, clientID, clientSecret, subscriptionID, accountName, accountKey := GetAzureCredsFromEnv()
		credCreateRequest = &api.CloudCredentialCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  credName,
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

	case drivers.ProviderNfs:
		log.Warnf("provider [%s] does not require creating cloud credential", provider)
		return nil

	case drivers.ProviderRke:
		credCreateRequest = &api.CloudCredentialCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  credName,
				Uid:   uid,
				OrgId: orgID,
			},
			CloudCredential: &api.CloudCredentialInfo{
				Type: api.CloudCredentialInfo_Rancher,
				Config: &api.CloudCredentialInfo_RancherConfig{
					RancherConfig: &api.RancherConfig{
						Endpoint: rke.RancherMap[kubeconfig[0]].Endpoint,
						Token:    rke.RancherMap[kubeconfig[0]].Token,
					},
				},
			},
		}

	case drivers.ProviderIbm:
		log.Infof("Create creds for IBM")
		apiKey, err := GetIBMApiKey("default")
		if err != nil {
			return err
		}
		credCreateRequest = &api.CloudCredentialCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  credName,
				Uid:   uid,
				OrgId: orgID,
			},
			CloudCredential: &api.CloudCredentialInfo{
				Type: api.CloudCredentialInfo_IBM,
				Config: &api.CloudCredentialInfo_IbmConfig{
					IbmConfig: &api.IBMConfig{
						ApiKey: apiKey,
					},
				},
			},
		}
	case drivers.ProviderGke:
		credCreateRequest = &api.CloudCredentialCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  credName,
				Uid:   uid,
				OrgId: orgID,
			},
			CloudCredential: &api.CloudCredentialInfo{
				Type: api.CloudCredentialInfo_Google,
				Config: &api.CloudCredentialInfo_GoogleConfig{
					GoogleConfig: &api.GoogleConfig{
						JsonKey: GlobalGkeSecretString,
					},
				},
			},
		}
	default:
		return fmt.Errorf("provider [%s] not supported for creating cloud credential", provider)
	}
	_, err := Inst().Backup.CreateCloudCredential(ctx, credCreateRequest)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		log.Warnf("failed to create cloud credential with name [%s] in org [%s] with [%s] as provider with error [%v]", credName, orgID, provider, err)
		return err
	}
	// check for cloud cred status
	cloudCredStatus := func() (interface{}, bool, error) {
		status, err := IsCloudCredPresent(credName, ctx, orgID)
		if err != nil {
			return "", true, fmt.Errorf("cloud cred %s present with error %v", credName, err)
		}
		if status {
			return "", true, nil
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(cloudCredStatus, defaultTimeout, defaultRetryInterval)
	if err != nil {
		return err
	}
	return nil
}

// CreateS3BackupLocation creates backup location for S3
func CreateS3BackupLocation(name, uid, cloudCred, cloudCredUID, bucketName, orgID, encryptionKey string, validate bool, sseS3EncryptionType ...api.S3Config_Sse) error {
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return err
	}
	backupDriver := Inst().Backup
	_, _, endpoint, region, disableSSLBool := s3utils.GetAWSDetailsFromEnv()
	// Initialize a new variable to hold the SSE S3 Encryption Type
	var sseType api.S3Config_Sse
	if len(sseS3EncryptionType) == 0 {
		// If sseS3EncryptionType Type parameter is not passed , then take it from environment variable
		sseType, err = GetSseS3EncryptionType()
		if err != nil {
			return err
		}
	} else {
		sseType = sseS3EncryptionType[0]
	}
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
			Uid:   uid,
		},
		BackupLocation: &api.BackupLocationInfo{
			Path:                    bucketName,
			EncryptionKey:           encryptionKey,
			ValidateCloudCredential: validate,
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
					SseType:    sseType,
				},
			},
		},
	}

	_, err = backupDriver.CreateBackupLocation(ctx, bLocationCreateReq)
	if err != nil {
		return fmt.Errorf("failed to create backup location: %v", err)
	}
	return nil
}

// CreateS3BackupLocationWithContext creates backup location for S3 using the given context
func CreateS3BackupLocationWithContext(name, uid, cloudCred, cloudCredUID, bucketName, orgID, encryptionKey string, ctx context1.Context, validate bool, sseS3EncryptionType ...api.S3Config_Sse) error {
	backupDriver := Inst().Backup
	_, _, endpoint, region, disableSSLBool := s3utils.GetAWSDetailsFromEnv()
	// Initialize a new variable to hold the SSE S3 Encryption Type
	var sseType api.S3Config_Sse
	var err error
	if len(sseS3EncryptionType) == 0 {
		// If sseS3EncryptionType Type parameter is not passed , then take it from environment variable
		sseType, err = GetSseS3EncryptionType()
		if err != nil {
			return err
		}
	} else {
		sseType = sseS3EncryptionType[0]
	}
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
			Uid:   uid,
		},
		BackupLocation: &api.BackupLocationInfo{
			Path:                    bucketName,
			EncryptionKey:           encryptionKey,
			ValidateCloudCredential: validate,
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
					SseType:    sseType,
				},
			},
		},
	}

	_, err = backupDriver.CreateBackupLocation(ctx, bLocationCreateReq)
	if err != nil {
		return fmt.Errorf("failed to create backup location: %v", err)
	}
	return nil
}

// UpdateS3BackupLocation updates s3 backup location with the provided values
func UpdateS3BackupLocation(name string, uid string, orgID string, cloudCred string, cloudCredUID string, ctx context1.Context, sseS3EncryptionType api.S3Config_Sse, validate bool) error {

	backupDriver := Inst().Backup
	_, _, endpoint, region, disableSSLBool := s3utils.GetAWSDetailsFromEnv()
	bLocationUpdateReq := &api.BackupLocationUpdateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
			Uid:   uid,
		},
		BackupLocation: &api.BackupLocationInfo{
			ValidateCloudCredential: validate,
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
					SseType:    sseS3EncryptionType,
				},
			},
		},
	}

	_, err := backupDriver.UpdateBackupLocation(ctx, bLocationUpdateReq)
	if err != nil {
		return err
	}
	return nil
}

// CreateAzureBackupLocation creates backup location for Azure
func CreateAzureBackupLocation(name string, uid string, cloudCred string, cloudCredUID string, bucketName string, orgID string, validate bool) error {
	backupDriver := Inst().Backup
	encryptionKey := "torpedo"
	azureRegion := os.Getenv("AZURE_ENDPOINT")
	environmentType := api.S3Config_AzureEnvironmentType_AZURE_GLOBAL // Default value
	if azureRegion == "CHINA" {
		environmentType = api.S3Config_AzureEnvironmentType_AZURE_CHINA
	}
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
			Uid:   uid,
		},
		BackupLocation: &api.BackupLocationInfo{
			Path:                    bucketName,
			EncryptionKey:           encryptionKey,
			ValidateCloudCredential: validate,
			CloudCredentialRef: &api.ObjectRef{
				Name: cloudCred,
				Uid:  cloudCredUID,
			},
			Type: api.BackupLocationInfo_Azure,
			Config: &api.BackupLocationInfo_S3Config{
				S3Config: &api.S3Config{
					AzureEnvironment: &api.S3Config_AzureEnvironmentType{
						Type: environmentType,
					},
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
		return fmt.Errorf("failed to create backup location Error: %v", err)
	}
	return nil
}

// CreateAzureBackupLocationWithContext creates backup location for Azure using the given context
func CreateAzureBackupLocationWithContext(name string, uid string, cloudCred string, cloudCredUID string, bucketName string, orgID string, encryptionKey string, ctx context1.Context, validate bool) error {
	backupDriver := Inst().Backup
	azureRegion := os.Getenv("AZURE_ENDPOINT")
	environmentType := api.S3Config_AzureEnvironmentType_AZURE_GLOBAL // Default value
	if azureRegion == "CHINA" {
		environmentType = api.S3Config_AzureEnvironmentType_AZURE_CHINA
	}
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
			Uid:   uid,
		},
		BackupLocation: &api.BackupLocationInfo{
			Path:                    bucketName,
			EncryptionKey:           encryptionKey,
			ValidateCloudCredential: validate,
			CloudCredentialRef: &api.ObjectRef{
				Name: cloudCred,
				Uid:  cloudCredUID,
			},
			Type: api.BackupLocationInfo_Azure,
			Config: &api.BackupLocationInfo_S3Config{
				S3Config: &api.S3Config{
					AzureEnvironment: &api.S3Config_AzureEnvironmentType{
						Type: environmentType,
					},
				},
			},
		},
	}
	_, err := backupDriver.CreateBackupLocation(ctx, bLocationCreateReq)
	if err != nil {
		return fmt.Errorf("failed to create backup location Error: %v", err)
	}
	return nil
}

// CreateGCPBackupLocation creates backup location for Google cloud
func CreateGCPBackupLocation(name string, uid string, cloudCred string, cloudCredUID string, bucketName string, orgID string, validate bool) error {
	backupDriver := Inst().Backup
	encryptionKey := "torpedo"
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
			Uid:   uid,
		},
		BackupLocation: &api.BackupLocationInfo{
			Path:                    bucketName,
			EncryptionKey:           encryptionKey,
			ValidateCloudCredential: validate,
			CloudCredentialRef: &api.ObjectRef{
				Name: cloudCred,
				Uid:  cloudCredUID,
			},
			Type: api.BackupLocationInfo_Google,
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

// CreateGCPBackupLocationWithContext creates backup location for Google cloud
func CreateGCPBackupLocationWithContext(name string, uid string, cloudCred string, cloudCredUID string, bucketName string, orgID string, ctx context1.Context, validate bool) error {
	backupDriver := Inst().Backup
	encryptionKey := "torpedo"
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
			Uid:   uid,
		},
		BackupLocation: &api.BackupLocationInfo{
			Path:                    bucketName,
			EncryptionKey:           encryptionKey,
			ValidateCloudCredential: validate,
			CloudCredentialRef: &api.ObjectRef{
				Name: cloudCred,
				Uid:  cloudCredUID,
			},
			Type: api.BackupLocationInfo_Google,
		},
	}
	_, err := backupDriver.CreateBackupLocation(ctx, bLocationCreateReq)
	if err != nil {
		return fmt.Errorf("failed to create backup location Error: %v", err)
	}
	return nil
}

// WaitForBackupLocationAddition waits for backup location to be added successfully
// or till timeout is reached. API should poll every `timeBeforeRetry` duration
func WaitForBackupLocationAddition(
	ctx context1.Context,
	backupLocationName,
	UID,
	orgID string,
	timeout time.Duration,
	timeBeforeRetry time.Duration,
) error {
	req := &api.BackupLocationInspectRequest{
		Name:  backupLocationName,
		Uid:   UID,
		OrgId: orgID,
	}
	f := func() (interface{}, bool, error) {
		inspectBlResp, err := Inst().Backup.InspectBackupLocation(ctx, req)
		if err != nil {
			return "", true, err
		}
		actual := inspectBlResp.GetBackupLocation().GetBackupLocationInfo().GetStatus().GetStatus()
		if actual == api.BackupLocationInfo_StatusInfo_Valid {
			return "", false, nil
		}
		return "", true, fmt.Errorf("backup location status for [%s] expected was [%s] but got [%s]", backupLocationName, api.BackupLocationInfo_StatusInfo_Valid, actual)
	}
	_, err := task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
	if err != nil {
		return fmt.Errorf("failed to wait for backup location addition. Error:[%v]", err)
	}
	return nil
}

// CreateNFSBackupLocation creates backup location for nfs
func CreateNFSBackupLocation(name string, uid string, orgID string, encryptionKey string, subPath string, validate bool) error {
	serverAddr := os.Getenv("NFS_SERVER_ADDR")
	mountOption := os.Getenv("NFS_MOUNT_OPTION")
	path := os.Getenv("NFS_PATH")
	backupDriver := Inst().Backup
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
			Uid:   uid,
		},
		BackupLocation: &api.BackupLocationInfo{
			Config: &api.BackupLocationInfo_NfsConfig{
				NfsConfig: &api.NFSConfig{
					ServerAddr:  serverAddr,
					SubPath:     subPath,
					MountOption: mountOption,
				},
			},
			Path:          path,
			Type:          api.BackupLocationInfo_NFS,
			EncryptionKey: encryptionKey,
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
	if validate {
		err = WaitForBackupLocationAddition(ctx, name, uid, orgID, defaultTimeout, defaultRetryInterval)
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateNFSBackupLocationWithContext creates backup location using the given context
func CreateNFSBackupLocationWithContext(name string, uid string, subPath string, orgID string, encryptionKey string, ctx context1.Context, validate bool) error {
	serverAddr := os.Getenv("NFS_SERVER_ADDR")
	mountOption := os.Getenv("NFS_MOUNT_OPTION")
	path := os.Getenv("NFS_PATH")
	backupDriver := Inst().Backup
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
			Uid:   uid,
		},
		BackupLocation: &api.BackupLocationInfo{
			Config: &api.BackupLocationInfo_NfsConfig{
				NfsConfig: &api.NFSConfig{
					ServerAddr:  serverAddr,
					SubPath:     subPath,
					MountOption: mountOption,
				},
			},
			Path:          path,
			Type:          api.BackupLocationInfo_NFS,
			EncryptionKey: encryptionKey,
		},
	}
	_, err := backupDriver.CreateBackupLocation(ctx, bLocationCreateReq)
	if err != nil {
		return fmt.Errorf("failed to create backup location Error: %v", err)
	}
	if validate {
		err = WaitForBackupLocationAddition(ctx, name, uid, orgID, defaultTimeout, defaultRetryInterval)
		if err != nil {
			return err
		}
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
	if obj, ok := spec.(*corev1.PersistentVolumeClaim); ok {
		if obj.Labels == nil {
			obj.Labels = make(map[string]string)
		}
		log.Infof("Adding label [%s=%s] to PVC %s", key, val, obj.Name)
		obj.Labels[key] = val
		core.Instance().UpdatePersistentVolumeClaim(obj)
		return nil
	} else if obj, ok := spec.(*corev1.ConfigMap); ok {
		if obj.Labels == nil {
			obj.Labels = make(map[string]string)
		}
		log.Infof("Adding label [%s=%s] to ConfigMap %s", key, val, obj.Name)
		obj.Labels[key] = val
		core.Instance().UpdateConfigMap(obj)
		return nil
	} else if obj, ok := spec.(*corev1.Secret); ok {
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

// GetClusterConfigPath returns kubeconfig for given cluster
func GetClusterConfigPath(clusterConfig string) (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return "", fmt.Errorf("failed to get source config path. Empty KUBECONFIGS environment variable")
	}
	kubeconfigList := strings.Split(kubeconfigs, ",")
	for _, kubeconfig := range kubeconfigList {
		if kubeconfig == clusterConfig {
			log.Infof("Cluster config path: %s", fmt.Sprintf("%s/%s", KubeconfigDirectory, kubeconfig))
			return fmt.Sprintf("%s/%s", KubeconfigDirectory, kubeconfig), nil
		}
	}
	return "", nil
}

// GetSourceClusterConfigPath returns kubeconfig for source
func GetSourceClusterConfigPath() (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return "", fmt.Errorf("failed to get source config path. Empty KUBECONFIGS environment variable")
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

func GetCustomClusterConfigPath(clusterConfigIndex int) (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return "", fmt.Errorf("empty KUBECONFIGS environment variable")
	}

	kubeconfigList := strings.Split(kubeconfigs, ",")
	if len(kubeconfigList) < 2 {
		return "", fmt.Errorf(`Failed to get source config path.
				At least minimum two kubeconfigs required but has %d`, len(kubeconfigList))
	}

	log.Infof("config path: %s", fmt.Sprintf("%s/%s", KubeconfigDirectory, kubeconfigList[clusterConfigIndex]))
	return fmt.Sprintf("%s/%s", KubeconfigDirectory, kubeconfigList[clusterConfigIndex]), nil
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

// GetIBMApiKey will return the IBM API Key from GlobalCredentialConfig
func GetIBMApiKey(cluster string) (string, error) {
	return GlobalCredentialConfig.CloudProviders.GetIBMCredential(cluster).APIKey, nil
}

type NfsInfo struct {
	NfsServerAddress string
	NfsPath          string
	NfsSubPath       string
	NfsMountOptions  string
}

// GetNfsInfoFromEnv get information for nfs share.
func GetNfsInfoFromEnv() *NfsInfo {
	creds := &NfsInfo{}
	creds.NfsServerAddress = os.Getenv("NFS_SERVER_ADDR")
	if creds.NfsServerAddress == "" {
		err := fmt.Errorf("NFS_SERVER_ADDR environment variable should not be empty")
		log.FailOnError(err, "Fetching NFS server address")
	}
	creds.NfsPath = os.Getenv("NFS_PATH")
	if creds.NfsPath == "" {
		err := fmt.Errorf("NFS_PATH environment variable should not be empty")
		log.FailOnError(err, "Fetching NFS path")
	}
	creds.NfsSubPath = os.Getenv("NFS_SUB_PATH")
	creds.NfsMountOptions = os.Getenv("NFS_MOUNT_OPTION")
	return creds
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

func DeleteGcpBucket(bucketName string) {

	ctx := context1.Background()

	client, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(GlobalGkeSecretString)))
	if err != nil {
		log.FailOnError(err, "Failed to create gcp client")
	}
	defer client.Close()

	// List all objects in the bucket
	it := client.Bucket(bucketName).Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.FailOnError(err, "error iterating over gcp bucket objects")
		}

		// Delete each object in the bucket
		err = client.Bucket(bucketName).Object(objAttrs.Name).Delete(ctx)
		if err != nil {
			log.FailOnError(err, "error deleting object from gcp bucket %s", objAttrs.Name)
		}
		log.Infof("Deleted object: %s\n", objAttrs.Name)
	}

	// Delete the bucket
	bucket := client.Bucket(bucketName)
	if err := bucket.Delete(ctx); err != nil {
		log.FailOnError(err, "failed to delete bucket [%v]", bucketName)
	}
}

// DeleteAzureBucket delete bucket in azure
func DeleteAzureBucket(bucketName string) {
	// From the Azure portal, get your Storage account blob service URL endpoint.
	_, _, _, _, accountName, accountKey := GetAzureCredsFromEnv()
	azureRegion := os.Getenv("AZURE_ENDPOINT")
	urlStr := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, bucketName) // Default value
	if azureRegion == "CHINA" {
		urlStr = fmt.Sprintf("https://%s.blob.core.chinacloudapi.cn/%s", accountName, bucketName)
	}
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

// DeleteNfsSubPath delete subpath from nfs shared path.
func DeleteNfsSubPath(subPath string) {
	// Get NFS share details from ENV variables.
	creds := GetNfsInfoFromEnv()
	mountDir := fmt.Sprintf("/tmp/nfsMount" + RandomString(4))

	// Mount the NFS share to the worker node.
	workerNode := node.GetWorkerNodes()[0]
	mountCmds := []string{
		fmt.Sprintf("mkdir -p %s", mountDir),
		fmt.Sprintf("mount -t nfs %s:%s %s", creds.NfsServerAddress, creds.NfsPath, mountDir),
	}
	for _, cmd := range mountCmds {
		err := runCmd(cmd, workerNode)
		log.FailOnError(err, fmt.Sprintf("Failed to run [%s] command on node [%s], error : [%s]", cmd, workerNode, err))
	}

	defer func() {
		// Unmount the NFS share from the master node.
		umountCmds := []string{
			fmt.Sprintf("umount %s", mountDir),
			fmt.Sprintf("rm -rf %s", mountDir),
		}
		for _, cmd := range umountCmds {
			err := runCmd(cmd, workerNode)
			log.FailOnError(err, fmt.Sprintf("Failed to run [%s] command on node [%s], error : [%s]", cmd, workerNode, err))
		}
	}()

	// Remove subpath from NFS share path.
	log.Infof("Deleting NFS share subpath: [%s] from path: [%s] on server: [%s]", subPath, creds.NfsPath, creds.NfsServerAddress)
	rmCmd := fmt.Sprintf("rm -rf %s/%s", mountDir, subPath)
	err := runCmd(rmCmd, workerNode)
	log.FailOnError(err, fmt.Sprintf("Failed to run [%s] command on node [%s], error : [%s]", rmCmd, workerNode, err))
}

// DeleteFilesFromNFSLocation deletes any file/directory from the supplied path
func DeleteFilesFromNFSLocation(nfsPath string, fileName string) (err error) {
	// Getting NFS share details from ENV variables.
	creds := GetNfsInfoFromEnv()
	mountDir := fmt.Sprintf("/tmp/nfsMount" + RandomString(4))
	workerNode := node.GetWorkerNodes()[0]

	// Mounting the NFS share to the worker node.
	mountCmds := []string{
		fmt.Sprintf("mkdir -p %s", mountDir),
		fmt.Sprintf("mount -t nfs %s:%s %s", creds.NfsServerAddress, creds.NfsPath, mountDir),
	}

	for _, cmd := range mountCmds {
		err = runCmd(cmd, workerNode)
		if err != nil {
			return fmt.Errorf("failed to run [%s] command on node [%s], error : [%s]", cmd, workerNode, err)
		}
	}

	defer func() {
		// Unmounting the NFS share from the worker node.
		umountCmds := []string{
			fmt.Sprintf("umount %s", mountDir),
			fmt.Sprintf("rm -rf %s", mountDir),
		}
		for _, cmd := range umountCmds {
			if e := runCmd(cmd, workerNode); e != nil {
				err = fmt.Errorf("failed to run [%s] command on node [%s], error : [%s]", cmd, workerNode, e)
			}
		}
	}()

	rmCmd := fmt.Sprintf("cd %s/%s && rm -rf %s", mountDir, nfsPath, fileName)
	err = runCmd(rmCmd, workerNode)
	if err != nil {
		return err
	}
	return
}

// DeleteBucket deletes bucket from the cloud or shared subpath from NFS server
func DeleteBucket(provider string, bucketName string) {
	Step(fmt.Sprintf("Delete bucket [%s]", bucketName), func() {
		switch provider {
		// TODO(stgleb): PTX-2359 Add DeleteAzureBucket
		case drivers.ProviderAws:
			DeleteS3Bucket(bucketName)
		case drivers.ProviderAzure:
			DeleteAzureBucket(bucketName)
		case drivers.ProviderNfs:
			DeleteNfsSubPath(bucketName)
		case drivers.ProviderGke:
			DeleteGcpBucket(bucketName)
		}
	})
}

// HaIncreaseErrorInjectionTargetNode repl increase and reboot target node
func HaIncreaseErrorInjectionTargetNode(event *EventRecord, ctx *scheduler.Context, v *volume.Volume, storageNodeMap map[string]node.Node, errInj ErrorInjection) error {

	stepLog := fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v and reboot target node",
		Inst().V.String(), ctx.App.Key, v)
	var replicaSets []*opsapi.ReplicaSet
	var err error
	Step(stepLog,
		func() {
			log.InfoD(stepLog)
			var currRep int64
			currRep, err = Inst().V.GetReplicationFactor(v)

			if err != nil {
				err = fmt.Errorf("error getting replication factor for volume %s, Error: %v", v.Name, err)
				return
			}
			//if repl is 3 cannot increase repl for the volume
			if currRep == 3 {
				err = fmt.Errorf("cannot perform repl incease as current repl factor is %d", currRep)
				return
			}

			replicaSets, err = Inst().V.GetReplicaSets(v)

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
						var poolsUsedSize map[string]string
						var pools map[string]*opsapi.StoragePool
						poolsUsedSize, err = Inst().V.GetPoolsUsedSize(&newReplNode)
						if err != nil {
							return
						}

						pools, err = Inst().V.ListStoragePools(metav1.LabelSelector{})
						if err != nil {

							return
						}
						for p, u := range poolsUsedSize {
							listPool := pools[p]
							var usedSize uint64
							var vol *opsapi.Volume
							usedSize, err = strconv.ParseUint(u, 10, 64)
							if err != nil {

								return
							}
							freeSize := listPool.TotalSize - usedSize
							vol, err = Inst().V.InspectVolume(v.ID)
							if err != nil {
								return
							}
							if freeSize >= vol.Usage {
								break
							}
						}
					}
				}
				if newReplID != "" {
					var nodeContexts []*scheduler.Context
					nodeContexts, err = GetContextsOnNode(&[]*scheduler.Context{ctx}, &newReplNode)
					if err != nil {
						return
					}

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
							if event != nil {
								dashStats := make(map[string]string)
								dashStats["volume-name"] = v.Name
								dashStats["curr-repl-factor"] = strconv.FormatInt(currRep, 10)
								dashStats["new-repl-factor"] = strconv.FormatInt(currRep+1, 10)
								updateLongevityStats(event.Event.Type, stats.HAIncreaseEventName, dashStats)
							}

							err = Inst().V.SetReplicationFactor(v, currRep+1, []string{newReplID}, nil, false)
							if err != nil {
								err = fmt.Errorf("There is an error increasing repl [%v]", err.Error())
								return
							}
						})

					if err == nil {
						action := "reboot"
						if errInj == PX_RESTART {
							action = "restart px on"
						} else if errInj == CRASH {
							action = "crash px on"
						}
						dashStats := make(map[string]string)
						dashStats["node"] = newReplNode.Name

						stepLog = fmt.Sprintf("%s target node %s while repl increase is in-progres", action,
							newReplNode.Hostname)
						Step(stepLog,
							func() {
								log.InfoD(stepLog)
								log.Info("Waiting for 10 seconds for re-sync to initialize before target node reboot")
								time.Sleep(10 * time.Second)
								if errInj == PX_RESTART {

									if event != nil {
										updateLongevityStats(event.Event.Type, stats.PXRestartEventName, dashStats)
									}
									err = Inst().V.StopDriver([]node.Node{newReplNode}, false, nil)
									if err != nil {
										return
									}
									log.Infof("waiting for 15 mins before starting volume driver")
									time.Sleep(15 * time.Minute)
									err = Inst().V.StartDriver(newReplNode)
									if err != nil {
										return
									}
									log.InfoD("PX restarted successfully on node %v", newReplNode)
								} else if errInj == REBOOT {
									if event != nil {
										updateLongevityStats(event.Event.Type, stats.NodeRebootEventName, dashStats)
									}

									err = Inst().N.RebootNode(newReplNode, node.RebootNodeOpts{
										Force: true,
										ConnectionOpts: node.ConnectionOpts{
											Timeout:         1 * time.Minute,
											TimeBeforeRetry: 5 * time.Second,
										},
									})
									if err != nil {
										err = fmt.Errorf("error rebooting node %v, Error: %v", newReplNode.Name, err)

									}
								} else if errInj == CRASH {
									if event != nil {
										updateLongevityStats(event.Event.Type, stats.PXCrashEventName, dashStats)
									}

									errorChan := make(chan error, errorChannelSize)
									CrashVolDriverAndWait([]node.Node{newReplNode}, &errorChan)
									var errorMsgs []string
									for chanError := range errorChan {
										errorMsgs = append(errorMsgs, chanError.Error())
									}
									if len(errorMsgs) > 0 {
										err = fmt.Errorf("%s", strings.Join(errorMsgs, ","))
									}

								}
								err = ValidateReplFactorUpdate(v, currRep+1)
								if err != nil {
									err = fmt.Errorf("error in ha-increse after %s target node . Error: %v", action, err)
									return
								}
								dash.VerifySafely(err, nil, fmt.Sprintf("repl successfully increased to %d", currRep+1))
								if strings.Contains(ctx.App.Key, fastpathAppName) {
									if event != nil {
										dashStats = make(map[string]string)
										dashStats["curr-repl-factor"] = strconv.FormatInt(currRep, 10)
										dashStats["new-repl-factor"] = strconv.FormatInt(currRep-1, 10)
										dashStats["fastpath"] = "true"
										dashStats["volume-name"] = v.Name
										updateLongevityStats(event.Event.Type, stats.HADecreaseEventName, dashStats)
									}
									err = ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_INACTIVE)
									dash.VerifySafely(err, nil, "fastpath volume successfully validated")
									err = Inst().V.SetReplicationFactor(v, currRep-1, nil, nil, true)
									if err != nil {
										return
									}
								}
							})

						err = ValidateDataIntegrity(&nodeContexts)
						if err != nil {
							return
						}
					}
				} else {
					err = fmt.Errorf("no node identified to repl increase for vol: %s", v.Name)
				}
			} else {
				return
			}
		})
	return err
}

// HaIncreaseErrorInjectSourceNode repl increase and reboot source node
func HaIncreaseErrorInjectSourceNode(event *EventRecord, ctx *scheduler.Context, v *volume.Volume, storageNodeMap map[string]node.Node, errInj ErrorInjection) error {
	stepLog := fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v and reboot source node",
		Inst().V.String(), ctx.App.Key, v)
	var err error
	var currRep int64
	Step(stepLog,
		func() {
			log.InfoD(stepLog)
			currRep, err = Inst().V.GetReplicationFactor(v)
			if err != nil {
				err = fmt.Errorf("error getting replication factor for volume %s, Error: %v", v.Name, err)
				return

			}

			//if repl is 3 cannot increase repl for the volume
			if currRep == 3 {
				err = fmt.Errorf("cannot perform repl incease as current repl factor is %d", currRep)
				return
			}

			if err == nil {
				action := "reboot"
				if errInj == PX_RESTART {
					action = "restart px on"
				} else if errInj == CRASH {
					action = "crash px on"
				}
				stepLog = fmt.Sprintf("%s source node while repl increase volume driver %s on app %s's volume: %v", action,
					Inst().V.String(), ctx.App.Key, v)

				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						var replicaSets []*opsapi.ReplicaSet
						replicaSets, err = Inst().V.GetReplicaSets(v)
						if err == nil {
							replicaNodes := replicaSets[0].Nodes
							if strings.Contains(ctx.App.Key, fastpathAppName) {
								var newFastPathNode *node.Node
								newFastPathNode, err = AddFastPathLabel(ctx)
								if err == nil {
									defer Inst().S.RemoveLabelOnNode(*newFastPathNode, k8s.NodeType)
								}
							}

							if event != nil {
								dashStats := make(map[string]string)
								dashStats["curr-repl-factor"] = strconv.FormatInt(currRep, 10)
								dashStats["new-repl-factor"] = strconv.FormatInt(currRep+1, 10)
								dashStats["volume-name"] = v.Name
								updateLongevityStats(event.Event.Type, stats.HAIncreaseEventName, dashStats)
							}

							err = Inst().V.SetReplicationFactor(v, currRep+1, nil, nil, false)
							if err != nil {
								return
							} else {
								log.Infof("Waiting for 60 seconds for re-sync to initialize before source nodes reboot")
								time.Sleep(60 * time.Second)
								//rebooting source nodes one by one
								for _, nID := range replicaNodes {
									replNodeToReboot := storageNodeMap[nID]
									dashStats := make(map[string]string)
									dashStats["node"] = replNodeToReboot.Name
									log.Infof("selected repl node: %s", replNodeToReboot.Name)
									if errInj == PX_RESTART {
										if event != nil {
											updateLongevityStats(event.Event.Type, stats.PXRestartEventName, dashStats)
										}

										err = Inst().V.StopDriver([]node.Node{replNodeToReboot}, false, nil)
										if err != nil {
											return
										}
										log.Infof("waiting for 15 mins before starting volume driver")
										time.Sleep(15 * time.Minute)
										err = Inst().V.StartDriver(replNodeToReboot)
										if err != nil {
											return
										}
										log.InfoD("PX restarted successfully on node %s", replNodeToReboot.Name)

									} else if errInj == REBOOT {
										if event != nil {
											updateLongevityStats(event.Event.Type, stats.NodeRebootEventName, dashStats)
										}

										err = Inst().N.RebootNode(replNodeToReboot, node.RebootNodeOpts{
											Force: true,
											ConnectionOpts: node.ConnectionOpts{
												Timeout:         1 * time.Minute,
												TimeBeforeRetry: 5 * time.Second,
											},
										})
										if err != nil {
											err = fmt.Errorf("error rebooting node %v, Error: %v", replNodeToReboot.Name, err)
										}
									} else if errInj == CRASH {
										if event != nil {
											updateLongevityStats(event.Event.Type, stats.PXCrashEventName, dashStats)
										}

										errorChan := make(chan error, errorChannelSize)
										CrashVolDriverAndWait([]node.Node{replNodeToReboot}, &errorChan)
										var errorMsgs []string
										for chanError := range errorChan {
											errorMsgs = append(errorMsgs, chanError.Error())
										}
										if len(errorMsgs) > 0 {
											err = fmt.Errorf("%s", strings.Join(errorMsgs, ","))
										}
									}
								}
								err = ValidateReplFactorUpdate(v, currRep+1)
								if err != nil {
									err = fmt.Errorf("error in ha-increse after  source node reboot. Error: %v", err)
									return
								}
								dash.VerifySafely(err, nil, fmt.Sprintf("repl successfully increased to %d", currRep+1))
								if strings.Contains(ctx.App.Key, fastpathAppName) {
									if event != nil {
										dashStats := make(map[string]string)
										dashStats["curr-repl-factor"] = strconv.FormatInt(currRep, 10)
										dashStats["new-repl-factor"] = strconv.FormatInt(currRep-1, 10)
										dashStats["fastpath"] = "true"
										dashStats["volume-name"] = v.Name
										updateLongevityStats(event.Event.Type, stats.HADecreaseEventName, dashStats)
									}
									err = ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_INACTIVE)

									dash.VerifySafely(err, nil, "fastpath volume successfully validated")
									err = Inst().V.SetReplicationFactor(v, currRep-1, nil, nil, true)
									if err != nil {
										return
									}
								}

								for _, nID := range replicaNodes {
									var nodeContexts []*scheduler.Context
									replNodeToReboot := storageNodeMap[nID]
									nodeContexts, err = GetContextsOnNode(&[]*scheduler.Context{ctx}, &replNodeToReboot)
									if err != nil {
										return
									}
									err = ValidateDataIntegrity(&nodeContexts)
									if err != nil {
										return
									}
								}
							}

						} else {
							err = fmt.Errorf("error getting relicasets for volume %s, Error: %v", v.Name, err)
						}

					})
			} else {
				err = fmt.Errorf("error getting current replication factor for volume %s, Error: %v", v.Name, err)
			}

		})
	return err
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

// IsBackupLocationEmpty returns true if the bucket for a provider is empty
func IsBackupLocationEmpty(provider, bucketName string) (bool, error) {
	switch provider {
	case drivers.ProviderAws:
		result, err := IsS3BucketEmpty(bucketName)
		return result, err
	case drivers.ProviderNfs:
		result, err := IsNFSSubPathEmpty(bucketName)
		return result, err
	case drivers.ProviderGke:
		result, err := IsGCPBucketEmpty(bucketName)
		return result, err
	case drivers.ProviderAzure:
		result, err := IsAzureBlobEmpty(bucketName)
		return result, err
	default:
		return false, fmt.Errorf("function does not support %s provider", provider)
	}
}

func IsNFSSubPathEmpty(subPath string) (bool, error) {
	//TODO enhance the method to work with NFS server on cloud
	// Get NFS share details from ENV variables.
	creds := GetNfsInfoFromEnv()
	mountDir := fmt.Sprintf("/tmp/nfsMount" + RandomString(4))

	// Mount the NFS share to the worker node.
	masterNode := node.GetWorkerNodes()[0]
	mountCmds := []string{
		fmt.Sprintf("mkdir -p %s", mountDir),
		fmt.Sprintf("mount -t nfs %s:%s %s", creds.NfsServerAddress, creds.NfsPath, mountDir),
		fmt.Sprintf("find %s/%s -type f", mountDir, subPath),
	}
	for _, cmd := range mountCmds {
		output, err := runCmdGetOutput(cmd, masterNode)
		log.FailOnError(err, fmt.Sprintf("Failed to run [%s] command on node [%s], error : [%s]", cmd, masterNode, err))
		log.Infof("Output from command [%s] -\n%s", cmd, output)
	}

	defer func() {
		// Unmount the NFS share from the master node.
		umountCmds := []string{
			fmt.Sprintf("umount %s", mountDir),
			fmt.Sprintf("rm -rf %s", mountDir),
		}
		for _, cmd := range umountCmds {
			err := runCmd(cmd, masterNode)
			log.FailOnError(err, fmt.Sprintf("Failed to run [%s] command on node [%s], error : [%s]", cmd, masterNode, err))
		}
	}()

	// List the files in subpath from NFS share path.
	log.Infof("Checking the contents in NFS share subpath: [%s] from path: [%s] on server: [%s]", subPath, creds.NfsPath, creds.NfsServerAddress)
	fileCountCmd := fmt.Sprintf("find %s/%s -type f | wc -l", mountDir, subPath)
	log.Infof("Running command - %s", fileCountCmd)
	output, err := runCmdGetOutput(fileCountCmd, masterNode)
	log.FailOnError(err, fmt.Sprintf("Failed to run [%s] command on node [%s], error : [%s]", fileCountCmd, masterNode, err))
	log.Infof("Output of command [%s] - \n%s", fileCountCmd, output)
	result, err := strconv.Atoi(strings.TrimSpace(output))
	if result > 0 {
		return false, nil
	}
	return true, nil
}

// IsS3BucketEmpty returns true if bucket empty else false
func IsS3BucketEmpty(bucketName string) (bool, error) {
	id, secret, endpoint, s3Region, disableSSLBool := s3utils.GetAWSDetailsFromEnv()
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(id, secret, ""),
		Region:           aws.String(s3Region),
		DisableSSL:       aws.Bool(disableSSLBool),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return false, fmt.Errorf("failed to get S3 session to create bucket with %s", err)
	}

	S3Client := s3.New(sess)
	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	}

	result, err := S3Client.ListObjects(input)
	if err != nil {
		return false, fmt.Errorf("unable to fetch cotents from s3 failing with %s", err)
	}

	log.Info(fmt.Sprintf("Result content %d", len(result.Contents)))
	if len(result.Contents) > 0 {
		return false, nil
	}
	return true, nil
}

// IsGCPBucketEmpty returns true if bucket empty else false
func IsGCPBucketEmpty(bucketName string) (bool, error) {
	query := &storage.Query{Prefix: "", Delimiter: ""}
	ctx := context1.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(GlobalGkeSecretString)))
	if err != nil {
		log.Infof("Failed to create client gcp client : %v", err)
		return false, fmt.Errorf("failed to create client gcp storage %s", err)
	}
	defer client.Close()

	it := client.Bucket(bucketName).Objects(ctx, query)
	_, err = it.Next()
	if err == iterator.Done {
		// Iterator finished, bucket is empty
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("error occured while iterating over objects of gcp bucket %s", err)
	}

	// Iterator didn't finish, bucket is not empty
	return false, nil
}

// IsAzureBlobEmpty returns true if bucket empty else false
func IsAzureBlobEmpty(containerName string) (bool, error) {
	_, _, _, _, accountName, accountKey := GetAzureCredsFromEnv()
	azureEndpoint := os.Getenv("AZURE_ENDPOINT")
	urlStr := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName)
	if azureEndpoint == AzureChinaEndpoint {
		urlStr = fmt.Sprintf("https://%s.blob.core.chinacloudapi.cn/%s", accountName, containerName)
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return false, fmt.Errorf("failed to parse URL [%v]: %v", urlStr, err)
	}

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to create shared key credential [%v]", err))

	containerURL := azblob.NewContainerURL(*u, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
	ctx := context1.Background() // This example uses a never-expiring context
	listBlobsSegmentOptions := azblob.ListBlobsSegmentOptions{
		MaxResults: 1,
	}

	listBlob, err := containerURL.ListBlobsFlatSegment(ctx, azblob.Marker{}, listBlobsSegmentOptions)
	if err != nil {
		return false, fmt.Errorf("failed to list blobs in container [%v]: %v", containerName, err)
	}

	if len(listBlob.Segment.BlobItems) == 0 {
		// No blobs found, container is empty
		return true, nil
	}
	// Blobs found, container is not empty
	return false, nil
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
		// Update ObjectLockConfiguration to bucket
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
			err = fmt.Errorf("failed to update Objectlock config with Retain Count [%v] and Mode [%v]. Error: [%v]", retainCount, objectLockMode, err)
		}
	}
	return err
}

// UpdateS3BucketPolicy applies the given policy to the given bucket.
func UpdateS3BucketPolicy(bucketName string, policy string) error {

	id, secret, endpoint, s3Region, disableSslBool := s3utils.GetAWSDetailsFromEnv()
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(id, secret, ""),
		Region:           aws.String(s3Region),
		DisableSSL:       aws.Bool(disableSslBool),
		S3ForcePathStyle: aws.Bool(true),
	},
	)
	if err != nil {
		return fmt.Errorf("failed to get S3 session to update bucket policy : [%v]", err)
	}
	s3Client := s3.New(sess)
	_, err = s3Client.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(bucketName),
		Policy: aws.String(policy),
	})
	if err != nil {
		return fmt.Errorf("failed to update bucket policy with Policy [%v]. Error: [%v]", policy, err)
	}
	return nil
}

// RemoveS3BucketPolicy removes the given policy from the given bucket.
func RemoveS3BucketPolicy(bucketName string) error {
	// Create a new S3 client.
	id, secret, endpoint, s3Region, disableSslBool := s3utils.GetAWSDetailsFromEnv()
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(id, secret, ""),
		Region:           aws.String(s3Region),
		DisableSSL:       aws.Bool(disableSslBool),
		S3ForcePathStyle: aws.Bool(true),
	},
	)
	if err != nil {
		return fmt.Errorf("Failed to get S3 session to remove S3 bucket policy : [%v]", err)
	}

	s3Client := s3.New(sess)

	// Create a new DeleteBucketPolicyInput object.
	input := &s3.DeleteBucketPolicyInput{
		Bucket: aws.String(bucketName),
	}

	// Delete the bucket policy.
	_, err = s3Client.DeleteBucketPolicy(input)
	if err != nil {
		return err
	}
	return nil
}

// CreateAzureBucket creates bucket in Azure
func CreateAzureBucket(bucketName string) {
	// From the Azure portal, get your Storage account blob service URL endpoint.
	_, _, _, _, accountName, accountKey := GetAzureCredsFromEnv()
	azureRegion := os.Getenv("AZURE_ENDPOINT")
	urlStr := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, bucketName) // Default value
	if azureRegion == "CHINA" {
		urlStr = fmt.Sprintf("https://%s.blob.core.chinacloudapi.cn/%s", accountName, bucketName)
	}
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
	dash.VerifyFatal(err, nil, fmt.Sprintf("verfiy getting kubeconfigs [%v] from configmap [%s]", kubeconfigList, configMapName))
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
	Pds                                 pds.Driver
	SpecDir                             string
	AppList                             []string
	SecureAppList                       []string
	CsiAppList                          []string
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
	ScaleAppTimeout                     time.Duration
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
	PureFADAPod                         string
	RunCSISnapshotAndRestoreManyTest    bool
	VaultAddress                        string
	VaultToken                          string
	SchedUpgradeHops                    string
	MigrationHops                       string
	AutopilotUpgradeImage               string
	CsiGenericDriverConfigMap           string
	HelmValuesConfigMap                 string
	IsHyperConverged                    bool
	Dash                                *aetosutil.Dashboard
	JobName                             string
	JobType                             string
	PortworxPodRestartCheck             bool
	IsPDSApps                           bool
	AnthosAdminWorkStationNodeIP        string
	AnthosInstPath                      string
	SkipSystemChecks                    bool
	FaSecret                            string
}

// ParseFlags parses command line flags
func ParseFlags() {
	var err error

	var s, m, n, v, backupDriverName, pdsDriverName, specDir, logLoc, logLevel, appListCSV, secureAppsCSV, repl1AppsCSV, csiAppsCSV, provisionerName, configMapName string

	var schedulerDriver scheduler.Driver
	var volumeDriver volume.Driver
	var nodeDriver node.Driver
	var monitorDriver monitor.Driver
	var backupDriver backup.Driver
	var pdsDriver pds.Driver
	var appScaleFactor int
	var volUpgradeEndpointURL string
	var volUpgradeEndpointVersion string
	var upgradeStorageDriverEndpointList string
	var minRunTimeMins int
	var chaosLevel int
	var storageNodesPerAZ int
	var destroyAppTimeout time.Duration
	var driverStartTimeout time.Duration
	var scaleAppTimeout time.Duration
	var autoStorageNodeRecoveryTimeout time.Duration
	var licenseExpiryTimeoutHours time.Duration
	var meteringIntervalMins time.Duration
	var bundleLocation string
	var customConfigPath string
	var hyperConverged bool
	var enableDash bool

	var pxPodRestartCheck bool
	var deployPDSApps bool

	// TODO: We rely on the customAppConfig map to be passed into k8s.go and stored there.
	// We modify this map from the tests and expect that the next RescanSpecs will pick up the new custom configs.
	// We should make this more robust.
	var customAppConfig map[string]scheduler.AppConfig = make(map[string]scheduler.AppConfig)

	var skipSystemChecks bool
	var enableStorkUpgrade bool
	var secretType string
	var pureVolumes bool
	var pureSANType string
	var pureFADAPod string
	var runCSISnapshotAndRestoreManyTest bool
	var vaultAddress string
	var vaultToken string
	var schedUpgradeHops string
	var migrationHops string
	var autopilotUpgradeImage string
	var csiGenericDriverConfigMapName string
	//dashboard fields
	var user, testBranch, testProduct, testType, testDescription, testTags string
	var testsetID int
	var torpedoJobName string
	var torpedoJobType string
	var anthosWsNodeIp string
	var anthosInstPath string
	var faSecret string

	log.Infof("The default scheduler is %v", defaultScheduler)
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
	flag.StringVar(&csiAppsCSV, csiAppCliFlag, "", "Comma-separated list of apps to deploy with CSI provisioner")
	flag.StringVar(&provisionerName, provisionerFlag, defaultStorageProvisioner, "Name of the storage provisioner Portworx or CSI.")
	flag.IntVar(&storageNodesPerAZ, storageNodesPerAZFlag, defaultStorageNodesPerAZ, "Maximum number of storage nodes per availability zone")
	flag.DurationVar(&destroyAppTimeout, "destroy-app-timeout", defaultTimeout, "Maximum wait time for app to be deleted")
	flag.DurationVar(&scaleAppTimeout, scaleAppTimeoutFlag, 0, "Maximum wait time for app to be ready")
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
	flag.StringVar(&pureFADAPod, "pure-fada-pod", "", "If using Pure FADA volumes, what FA Pod to place the volumes in. This Pod must already exist, and be in the same Realm matching the px-pure-secret")
	flag.BoolVar(&runCSISnapshotAndRestoreManyTest, "pure-fa-snapshot-restore-to-many-test", false, "If using Pure volumes, to enable Pure clone many tests")
	flag.StringVar(&vaultAddress, "vault-addr", "", "Path to custom configuration files")
	flag.StringVar(&vaultToken, "vault-token", "", "Path to custom configuration files")
	flag.StringVar(&schedUpgradeHops, "sched-upgrade-hops", "", "Comma separated list of versions scheduler upgrade to take hops")
	flag.StringVar(&migrationHops, "migration-hops", "", "Comma separated list of versions for migration pool")
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
	flag.StringVar(&dataIntegrityValidationTests, dataIntegrityValidationTestsFlag, "", "list data integrity validation tests to run data integrity validation")
	flag.StringVar(&user, userFlag, "nouser", "user name running the tests")
	flag.StringVar(&testDescription, testDescriptionFlag, "Torpedo Workflows", "test suite description")
	flag.StringVar(&testType, testTypeFlag, "system-test", "test types like system-test,functional,integration")
	flag.StringVar(&testTags, testTagsFlag, "", "tags running the tests. Eg: key1:val1,key2:val2")
	flag.IntVar(&testsetID, testSetIDFlag, 0, "testset id to post the results")
	flag.StringVar(&testBranch, testBranchFlag, "master", "branch of the product")
	flag.StringVar(&testProduct, testProductFlag, "PxEnp", "Portworx product under test")
	flag.StringVar(&pxRuntimeOpts, "px-runtime-opts", "", "comma separated list of run time options for cluster update")
	flag.StringVar(&pxClusterOpts, "px-cluster-opts", "", "comma separated list of cluster options for cluster update")
	flag.BoolVar(&pxPodRestartCheck, failOnPxPodRestartCount, false, "Set it true for px pods restart check during test")
	flag.BoolVar(&deployPDSApps, deployPDSAppsFlag, false, "To deploy pds apps and return scheduler context for pds apps")
	flag.StringVar(&pdsDriverName, pdsDriveCliFlag, defaultPdsDriver, "Name of the pdsdriver to use")
	flag.StringVar(&anthosWsNodeIp, anthosWsNodeIpCliFlag, "", "Anthos admin work station node IP")
	flag.StringVar(&anthosInstPath, anthosInstPathCliFlag, "", "Anthos config path where all conf files present")
	flag.StringVar(&faSecret, faSecretCliFlag, "", "comma seperated list of famanagementip=tokenValue pairs")

	// System checks https://github.com/portworx/torpedo/blob/86232cb195400d05a9f83d57856f8f29bdc9789d/tests/common.go#L2173
	// should be skipped from AfterSuite() if this flag is set to true. This is to avoid distracting test failures due to
	// unstable testing environments.
	flag.BoolVar(&skipSystemChecks, skipSystemCheckCliFlag, false, "Skip system checks during after suite")
	flag.Parse()

	log.SetLoglevel(logLevel)
	tpLogPath = fmt.Sprintf("%s/%s", logLoc, "torpedo.log")
	suiteLogger = CreateLogger(tpLogPath)
	log.SetTorpedoFileOutput(suiteLogger)

	appList, err := splitCsv(appListCSV)
	if err != nil {
		log.Fatalf("failed to parse app list: %v. err: %v", appListCSV, err)
	}

	csiAppsList := make([]string, 0)
	if len(csiAppsCSV) > 0 {
		apl, err := splitCsv(csiAppsCSV)
		log.FailOnError(err, fmt.Sprintf("failed to parse csiAppsCSV app list: %v", csiAppsCSV))
		csiAppsList = append(csiAppsList, apl...)
		log.Infof("provisioner CSI apps : %+v", csiAppsList)
		appList = append(appList, csiAppsList...)
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
		log.FailOnError(err, fmt.Sprintf("failed to parse repl-1 app list: %v", repl1AppsCSV))
		repl1AppList = append(repl1AppList, apl...)
		log.Infof("volume repl 1 apps : %+v", repl1AppList)
		//Adding repl-1 apps as part of app list for deployment
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

		log.Infof("Pds driver name %s", pdsDriverName)
		if pdsDriverName != "" {
			if pdsDriver, err = pds.Get(pdsDriverName); err != nil {
				log.Fatalf("cannot find pds driver for %s. Err: %v\n", pdsDriverName, err)
			} else {
				log.Infof("Pds driver found %v", pdsDriver)
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
				Pds:                                 pdsDriver,
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
				CsiAppList:                          csiAppsList,
				Provisioner:                         provisionerName,
				MaxStorageNodesPerAZ:                storageNodesPerAZ,
				DestroyAppTimeout:                   destroyAppTimeout,
				ScaleAppTimeout:                     scaleAppTimeout,
				DriverStartTimeout:                  driverStartTimeout,
				AutoStorageNodeRecoveryTimeout:      autoStorageNodeRecoveryTimeout,
				ConfigMap:                           configMapName,
				BundleLocation:                      bundleLocation,
				CustomAppConfig:                     customAppConfig,
				Backup:                              backupDriver,
				SecretType:                          secretType,
				PureVolumes:                         pureVolumes,
				PureSANType:                         pureSANType,
				PureFADAPod:                         pureFADAPod,
				RunCSISnapshotAndRestoreManyTest:    runCSISnapshotAndRestoreManyTest,
				VaultAddress:                        vaultAddress,
				VaultToken:                          vaultToken,
				SchedUpgradeHops:                    schedUpgradeHops,
				MigrationHops:                       migrationHops,
				AutopilotUpgradeImage:               autopilotUpgradeImage,
				CsiGenericDriverConfigMap:           csiGenericDriverConfigMapName,
				LicenseExpiryTimeoutHours:           licenseExpiryTimeoutHours,
				MeteringIntervalMins:                meteringIntervalMins,
				IsHyperConverged:                    hyperConverged,
				Dash:                                dash,
				JobName:                             torpedoJobName,
				JobType:                             torpedoJobType,
				PortworxPodRestartCheck:             pxPodRestartCheck,
				AnthosAdminWorkStationNodeIP:        anthosWsNodeIp,
				AnthosInstPath:                      anthosInstPath,
				IsPDSApps:                           deployPDSApps,
				SkipSystemChecks:                    skipSystemChecks,
				FaSecret:                            faSecret,
			}
			if instance.S.String() == "openshift" {
				instance.LogLoc = "/mnt"
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
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
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

// CollectLogsFromPods collects logs from specified pods and stores them in a directory named after the test case
func CollectLogsFromPods(testCaseName string, podLabel map[string]string, namespace string, logLabel string) {

	// Check to handle cloud based deployment with 0 master nodes
	if len(node.GetMasterNodes()) == 0 {
		log.Warnf("Skipping pod log collection for pods with [%s] label in test case [%s] as it's cloud cluster", logLabel, testCaseName)
		return
	}

	// In case of ocp skip log collection
	if Inst().S.String() == openshift.SchedName {
		log.Warnf("Skipping pod log collection for pods with [%s] label in test case [%s] as it's ocp cluster", logLabel, testCaseName)
		return
	}

	testCaseName = strings.ReplaceAll(testCaseName, " ", "")
	podList, err := core.Instance().GetPods(namespace, podLabel)
	if err != nil {
		log.Errorf("Error in getting pods for the [%s] logs of test case [%s], Err: %v", logLabel, testCaseName, err.Error())
		return
	}
	masterNode := node.GetMasterNodes()[0]
	err = runCmd("pwd", masterNode)
	if err != nil {
		log.Errorf("Error in running [pwd] command in node [%s] for the [%s] logs of test case [%s]", masterNode.Name, logLabel, testCaseName)
		return
	}
	testCaseLogDirPath := fmt.Sprintf("%s/%s-logs", pxbLogDirPath, testCaseName)
	log.Infof("Creating a directory [%s] in node [%s] to store [%s] logs for the test case [%s]", testCaseLogDirPath, masterNode.Name, logLabel, testCaseName)
	err = runCmd(fmt.Sprintf("mkdir -p %v", testCaseLogDirPath), masterNode)
	if err != nil {
		log.Errorf("Error in creating a directory [%s] in node [%s] to store [%s] logs for the test case [%s]. Err: %v", testCaseLogDirPath, masterNode.Name, logLabel, testCaseName, err.Error())
		return
	}
	for _, pod := range podList.Items {
		log.Infof("Writing [%s] pod into a %v/%v.log file", pod.Name, testCaseLogDirPath, pod.Name)
		err = runCmd(fmt.Sprintf("kubectl logs %s -n %s > %s/%s.log", pod.Name, namespace, testCaseLogDirPath, pod.Name), masterNode)
		if err != nil {
			log.Errorf("Error in writing [%s] pod into a %v/%v.log file. Err: %v", pod.Name, testCaseLogDirPath, pod.Name, err.Error())
		}
	}
}

// collectStorkLogs collects Stork logs and stores them using the CollectLogsFromPods function
func collectStorkLogs(testCaseName string) {
	storkLabel := make(map[string]string)
	storkLabel["name"] = "stork"
	pxNamespace, err := Inst().V.GetVolumeDriverNamespace()
	if err != nil {
		log.Errorf("Error in getting portworx namespace. Err: %v", err.Error())
		return
	}
	CollectLogsFromPods(testCaseName, storkLabel, pxNamespace, "stork")
}

// CollectMongoDBLogs collects MongoDB logs and stores them using the CollectLogsFromPods function
func CollectMongoDBLogs(testCaseName string) {
	pxbLabel := make(map[string]string)
	pxbLabel["app.kubernetes.io/component"] = mongodbStatefulset
	pxbNamespace, err := backup.GetPxBackupNamespace()
	if err != nil {
		log.Errorf("Error in getting px-backup namespace. Err: %v", err.Error())
		return
	}
	CollectLogsFromPods(testCaseName, pxbLabel, pxbNamespace, "mongodb")
}

// collectPxBackupLogs collects Px-Backup logs and stores them using the CollectLogsFromPods function
func collectPxBackupLogs(testCaseName string) {
	pxbLabel := make(map[string]string)
	pxbLabel["app"] = "px-backup"
	pxbNamespace, err := backup.GetPxBackupNamespace()
	if err != nil {
		log.Errorf("Error in getting px-backup namespace. Err: %v", err.Error())
		return
	}
	CollectLogsFromPods(testCaseName, pxbLabel, pxbNamespace, "px-backup")
}

// compressSubDirectories compresses all subdirectories within the specified directory on the master node
func compressSubDirectories(dirPath string) {
	masterNode := node.GetMasterNodes()[0]
	log.Infof("Compressing sub-directories in the directory [%s] in node [%s]", dirPath, masterNode.Name)
	err := runCmdWithNoSudo(fmt.Sprintf("find %s -mindepth 1 -depth -type d -exec sh -c 'tar czf \"${1%%/}.tar.gz\" -C \"$(dirname \"$1\")\" \"$(basename \"$1\")\" && rm -rf \"$1\"' sh {} \\;", dirPath), masterNode)
	if err != nil {
		log.Errorf("Error in compressing sub-directories in the directory [%s] in node [%s]", dirPath, masterNode.Name)
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

			if expandedPool.LastOperation.Status == opsapi.SdkStoragePool_OPERATION_IN_PROGRESS ||
				expandedPool.LastOperation.Status == opsapi.SdkStoragePool_OPERATION_PENDING {
				// storage pool resize has been triggered
				log.InfoD("Pool %s expansion started", poolID)
				return nil, false, nil
			}

		}
		return nil, true, fmt.Errorf("pool %s resize not triggered ", poolID)
	}

	_, err := task.DoRetryWithTimeout(f, 2*time.Minute, 5*time.Second)
	return err
}

// RebootNodeAndWaitForPxUp reboots node and waits for  volume driver to be up
func RebootNodeAndWaitForPxUp(n node.Node) error {

	err := RebootNodeAndWaitForPxDown(n)
	err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
	if err != nil {
		return err
	}

	return nil

}

// RebootNodeAndWaitForPxDown reboots node and waits for volume driver to be down
func RebootNodeAndWaitForPxDown(n node.Node) error {

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

// ValidateDriveRebalance checks rebalance state of new drives added
func ValidateDriveRebalance(stNode node.Node) error {

	disks := stNode.Disks
	initPoolCount := len(stNode.Pools)
	var err error
	var drivePath string
	drivePathsToValidate := make([]string, 0)
	//2 min wait for new disk to associate with the node
	time.Sleep(2 * time.Minute)

	t := func() (interface{}, bool, error) {
		err = Inst().V.RefreshDriverEndpoints()
		if err != nil {
			return nil, true, err
		}

		stNode, err = node.GetNodeByName(stNode.Name)
		if err != nil {
			return nil, true, err
		}

		for k := range stNode.Disks {
			if _, ok := disks[k]; !ok {
				drivePath = k
				return nil, false, nil
			}
		}

		return nil, true, fmt.Errorf("drive path not found")
	}
	_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 1*time.Minute)
	if err != nil {
		//this is a special case occurs where drive is added with same path as deleted pool
		if initPoolCount >= len(stNode.Pools) {
			for p := range stNode.Disks {
				drivePathsToValidate = append(drivePathsToValidate, p)
			}
		} else {
			return err
		}

	} else {
		drivePathsToValidate = append(drivePathsToValidate, drivePath)
	}

	for _, p := range drivePathsToValidate {
		drivePath = p
		log.Infof("Validating rebalance for path %s", drivePath)
		cmd := fmt.Sprintf("sv drive add -d %s -o status", drivePath)
		var prevStatus string

		t = func() (interface{}, bool, error) {

			// Execute the command and check get rebalance status
			currStatus, err := Inst().V.GetPxctlCmdOutputConnectionOpts(stNode, cmd, node.ConnectionOpts{
				IgnoreError:     false,
				TimeBeforeRetry: defaultRetryInterval,
				Timeout:         defaultTimeout,
			}, false)

			if err != nil {
				if strings.Contains(err.Error(), "Device already exists") || strings.Contains(err.Error(), "Drive already in use") {
					return "", false, nil
				}
				return "", true, err
			}
			log.Infof(fmt.Sprintf("Rebalance Status for drive [%s] in node [%s] : %s", drivePath, stNode.Name, strings.TrimSpace(currStatus)))
			if strings.Contains(currStatus, "Rebalance done") {
				return "", false, nil
			}
			if prevStatus == currStatus {
				return "", false, fmt.Errorf("rebalance Status for drive [%s] in node [%s] is not progressing", drivePath, stNode.Name)
			}
			prevStatus = currStatus
			return "", true, fmt.Errorf("wait for pool rebalance to complete for drive [%s]", drivePath)
		}

		_, err = task.DoRetryWithTimeout(t, 180*time.Minute, 3*time.Minute)
		if err != nil {
			return err
		}
	}

	// checking all pools are online after drive rebalance
	expectedStatus := "Online"
	err = WaitForPoolStatusToUpdate(stNode, expectedStatus)
	return err
}

// ValidateRebalanceJobs checks rebalance state of pools if running
func ValidateRebalanceJobs(stNode node.Node) error {

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

func updatePxClusterOpts() error {
	if pxClusterOpts != "" {
		log.InfoD("Setting cluster options: %s", pxClusterOpts)
		optionsMap := make(map[string]string)
		runtimeOpts, err := splitCsv(pxClusterOpts)
		log.FailOnError(err, "Error parsing run time options")

		for _, opt := range runtimeOpts {
			if !strings.Contains(opt, "=") {
				log.Fatalf("Given cluster option is not in expected format key=val, Actual : %v", opt)
			}
			optArr := strings.Split(opt, "=")
			ketString := "--" + optArr[0]
			optionsMap[ketString] = optArr[1]
		}
		currNode := node.GetWorkerNodes()[0]
		return Inst().V.SetClusterOptsWithConfirmation(currNode, optionsMap)
	} else {
		log.Info("No cluster options provided to update")
	}
	return nil

}

// GetCloudDriveDeviceSpecs returns Cloud drive specs on the storage cluster
func GetCloudDriveDeviceSpecs() ([]string, error) {
	log.InfoD("Getting cloud drive specs")
	deviceSpecs := make([]string, 0)
	IsOperatorBasedInstall, err := Inst().V.IsOperatorBasedInstall()
	if err != nil && !k8serrors.IsNotFound(err) {
		return deviceSpecs, err
	}

	if !IsOperatorBasedInstall {
		ns, err := Inst().V.GetVolumeDriverNamespace()
		if err != nil {
			return deviceSpecs, err
		}
		daemonSets, err := apps.Instance().ListDaemonSets(ns, metav1.ListOptions{
			LabelSelector: "name=portworx",
		})
		if err != nil {
			return deviceSpecs, err
		}

		if len(daemonSets) == 0 {
			return deviceSpecs, fmt.Errorf("no portworx daemonset found")
		}
		for _, container := range daemonSets[0].Spec.Template.Spec.Containers {
			if container.Name == "portworx" {
				for _, arg := range container.Args {
					if strings.Contains(arg, "size") {
						deviceSpecs = append(deviceSpecs, arg)
					}
				}
			}
		}
		return deviceSpecs, nil
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
	tags["pureFADAPod"] = Inst().PureFADAPod
	dash.TestCaseBegin(testName, testDescription, strconv.Itoa(testRepoID), tags)
	if TestRailSetupSuccessful && testRepoID != 0 {
		RunIdForSuite = testrailuttils.AddRunsToMilestone(testRepoID)
		CurrentTestRailTestCaseId = testRepoID
	}
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

// StartPxBackupTorpedoTest starts the logging for Px Backup torpedo test
func StartPxBackupTorpedoTest(testName string, testDescription string, tags map[string]string, testRepoID int, _ TestcaseAuthor, _ TestcaseQuarter) {
	instanceIDString := strconv.Itoa(testRepoID)
	timestamp := time.Now().Format("01-02-15h04m05s")
	Inst().InstanceID = fmt.Sprintf("%s-%s", instanceIDString, timestamp)
	StartTorpedoTest(testName, testDescription, tags, testRepoID)
}

// EndPxBackupTorpedoTest ends the logging for Px Backup torpedo test and updates results in testrail
func EndPxBackupTorpedoTest(contexts []*scheduler.Context) {
	defer func() {
		err := SetSourceKubeConfig()
		log.FailOnError(err, "failed to switch context to source cluster")
	}()
	CloseLogger(TestLogger)
	dash.TestCaseEnd()
	if TestRailSetupSuccessful && CurrentTestRailTestCaseId != 0 && RunIdForSuite != 0 {
		AfterEachTest(contexts, CurrentTestRailTestCaseId, RunIdForSuite)
	}

	currentSpecReport := ginkgo.CurrentSpecReport()
	if currentSpecReport.Failed() {
		log.Infof(">>>> FAILED TEST: %s", currentSpecReport.FullText())
	}
	// Cleanup all the namespaces created by the testcase
	err := DeleteAllNamespacesCreatedByTestCase()
	if err != nil {
		log.Errorf("Error in deleting namespaces created by the testcase. Err: %v", err.Error())
	}

	err = SetDestinationKubeConfig()
	if err != nil {
		log.Errorf("Error in setting destination kubeconfig. Err: %v", err.Error())
		return
	}

	err = DeleteAllNamespacesCreatedByTestCase()
	if err != nil {
		log.Errorf("Error in deleting namespaces created by the testcase. Err: %v", err.Error())
	}

	err = SetSourceKubeConfig()
	log.FailOnError(err, "failed to switch context to source cluster")

	masterNodes := node.GetMasterNodes()
	// TODO: enable the log collection for charmed k8s once we get the way to ssh into worker node from juju
	if len(masterNodes) > 0 && GetClusterProvider() != "charmed" {
		log.Infof(">>>> Collecting logs for testcase : %s", currentSpecReport.FullText())
		testCaseName := currentSpecReport.FullText()
		matches := regexp.MustCompile(`\{([^}]+)\}`).FindStringSubmatch(currentSpecReport.FullText())
		if len(matches) > 1 {
			testCaseName = matches[1]
		}
		masterNode := masterNodes[0]
		log.Infof("Creating a directory [%s] to store logs", pxbLogDirPath)
		err := runCmd(fmt.Sprintf("mkdir -p %v", pxbLogDirPath), masterNode)
		if err != nil {
			log.Errorf("Error in creating a directory [%s] to store logs. Err: %v", pxbLogDirPath, err.Error())
			return
		}
		collectStorkLogs(testCaseName)
		collectPxBackupLogs(testCaseName)
		compressSubDirectories(pxbLogDirPath)
	}
}

func CreateMultiVolumesAndAttach(wg *sync.WaitGroup, count int, nodeName string) (map[string]string, error) {
	createdVolIDs := make(map[string]string)
	defer wg.Done()
	timeString := time.Now().Format(time.RFC1123)
	timeString = regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(timeString, "_")
	for count > 0 {
		volName := fmt.Sprintf("%s-%d-%s", VolumeCreatePxRestart, count, timeString)
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
		log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s  after creating the volume", volName, volId)
		PrintInspectVolume(volId)
	}
	return createdVolIDs, nil
}

// GetPoolsInUse lists all persistent volumes and returns the pool IDs
func GetPoolsInUse() ([]string, error) {
	var poolsInUse []string
	pvlist, err := k8sCore.GetPersistentVolumes()
	if err != nil || pvlist == nil || len(pvlist.Items) == 0 {
		return nil, fmt.Errorf("no persistent volume found. Error: %v", err)
	}

	for _, pv := range pvlist.Items {
		volumeID := pv.GetName()
		poolUuids, err := GetPoolIDsFromVolName(volumeID)
		//Needed this logic as a workaround for PWX-35637
		if err != nil && strings.Contains(err.Error(), "not found") {
			continue
		}
		poolsInUse = append(poolsInUse, poolUuids...)
	}

	return poolsInUse, err
}

// GetPoolIDWithIOs returns the pools with IOs happening
func GetPoolIDWithIOs(contexts []*scheduler.Context) ([]string, error) {
	// pick a  pool doing some IOs from a pools list
	var err error
	var isIOsInProgress bool
	err = Inst().V.RefreshDriverEndpoints()
	if err != nil {
		return nil, err
	}

	poolIdsWithIOs := make([]string, 0)
	for _, ctx := range contexts {
		vols, err := Inst().S.GetVolumes(ctx)
		if err != nil {
			return nil, err
		}

		node := node.GetStorageDriverNodes()[0]
		for _, vol := range vols {
			appVol, err := Inst().V.InspectVolume(vol.ID)
			if err != nil {
				return nil, err
			}

			t := func() (interface{}, bool, error) {
				isIOsInProgress, err = Inst().V.IsIOsInProgressForTheVolume(&node, appVol.Id)
				if err != nil || !isIOsInProgress {
					return false, true, err
				}
				return true, false, nil
			}

			_, err = task.DoRetryWithTimeout(t, 2*time.Minute, 10*time.Second)
			if err != nil {
				return nil, err
			}

			if isIOsInProgress {
				log.Infof("IOs are in progress for [%v]", vol.Name)
				poolUuids := appVol.ReplicaSets[0].PoolUuids
				for _, p := range poolUuids {
					if !slices.Contains(poolIdsWithIOs, p) {
						poolIdsWithIOs = append(poolIdsWithIOs, p)
					}

				}
			}
		}

	}
	if len(poolIdsWithIOs) > 0 {
		return poolIdsWithIOs, nil
	}

	return nil, fmt.Errorf("no pools have IOs running,Err: %v", err)
}

// GetPoolWithIOsInGivenNode returns the poolID in the given node with IOs happening
func GetPoolWithIOsInGivenNode(stNode node.Node, contexts []*scheduler.Context, expandType opsapi.SdkStoragePool_ResizeOperationType, targetSizeGiB uint64) (*opsapi.StoragePool, error) {

	eligibilityMap, err := GetPoolExpansionEligibility(&stNode, expandType, targetSizeGiB)
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

	if selectedNodePoolID == "" {
		log.Warnf("No pool with IO found, picking a random pool in node %s to resize", stNode.Name)
		for _, p := range nodePools {
			if eligibilityMap[p] {
				selectedNodePoolID = p
			}
		}
	}

	selectedPool, err = GetStoragePoolByUUID(selectedNodePoolID)

	if err != nil {
		return nil, err
	}
	return selectedPool, nil
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
		for _, uuid := range each.PoolUuids {
			if !Contains(poolUuids, uuid) {
				poolUuids = append(poolUuids, uuid)
			}
		}
	}
	return poolUuids, nil
}

func GetNodeFromPoolUUID(poolUUID string) (*node.Node, error) {
	for _, n := range node.GetStorageNodes() {
		for _, p := range n.StoragePools {
			if p.Uuid == poolUUID {
				return &n, nil
			}
		}
	}
	return nil, fmt.Errorf("failed to find node for pool UUID %v", poolUUID)
}

// GetPoolExpansionEligibility identifying the nodes and pools in it if they are eligible for expansion
func GetPoolExpansionEligibility(stNode *node.Node, expandType opsapi.SdkStoragePool_ResizeOperationType, targetIncrementInGiB uint64) (map[string]bool, error) {
	var err error

	namespace, err := Inst().V.GetVolumeDriverNamespace()
	if err != nil {
		return nil, err
	}

	var maxCloudDrives int

	if _, err = core.Instance().GetSecret(PX_VSPHERE_SCERET_NAME, namespace); err == nil {
		maxCloudDrives = VSPHERE_MAX_CLOUD_DRIVES
	} else if _, err = core.Instance().GetSecret(PX_PURE_SECRET_NAME, namespace); err == nil {
		maxCloudDrives = FA_MAX_CLOUD_DRIVES
	} else {
		maxCloudDrives = CLOUD_PROVIDER_MAX_CLOUD_DRIVES
	}

	var currentNodeDrives int

	drvM, err := Inst().V.GetPoolDrives(stNode)
	if err != nil {
		return nil, fmt.Errorf("error getting block drives from node %s, Err :%v", stNode.Name, err)
	}

	for _, devices := range drvM {
		currentNodeDrives += len(devices)
	}
	eligibilityMap := make(map[string]bool)

	log.Infof("Node %s has total drives %d", stNode.Name, currentNodeDrives)
	eligibilityMap[stNode.Id] = true
	if currentNodeDrives == maxCloudDrives {
		eligibilityMap[stNode.Id] = false
	}
	nodePoolStatus, err := Inst().V.GetNodePoolsStatus(*stNode)
	if err != nil {
		return nil, err
	}

	log.Infof("Node [%s] has [%d] pools", stNode.Name, len(stNode.StoragePools))
	for _, pool := range stNode.StoragePools {
		eligibilityMap[pool.Uuid] = true

		d := drvM[fmt.Sprintf("%d", pool.ID)]
		log.Infof("pool %s has %d drives", pool.Uuid, len(d))
		if nodePoolStatus[pool.Uuid] == "Offline" {
			eligibilityMap[pool.Uuid] = false
		} else {
			if expandType == opsapi.SdkStoragePool_RESIZE_TYPE_ADD_DISK {
				if len(d) == POOL_MAX_CLOUD_DRIVES {
					log.Infof("pool %s has reached max drives", pool.Uuid)
					eligibilityMap[pool.Uuid] = false
				} else {
					baseDiskSizeInGib := d[0].SizeInGib
					poolSize := uint64(0)
					for _, drive := range d {
						poolSize += drive.SizeInGib
					}
					if targetIncrementInGiB == 0 {
						targetIncrementInGiB = baseDiskSizeInGib
					}
					targetSizeGiB := poolSize + targetIncrementInGiB
					expectedPoolDrivesAfterExpansion := int(math.Ceil(float64(targetSizeGiB) / float64(baseDiskSizeInGib)))
					if expectedPoolDrivesAfterExpansion > POOL_MAX_CLOUD_DRIVES {
						log.Infof("pool %s will reach max drives if expanded to size [%v] using add-drive", pool.Uuid, targetSizeGiB)
						eligibilityMap[pool.Uuid] = false
					} else {
						currentPoolDrives := len(d)
						expectedNodeDrivesAfterExpansion := currentNodeDrives + (expectedPoolDrivesAfterExpansion - currentPoolDrives)
						stc, err := Inst().V.GetDriver()
						if err != nil {
							return nil, err
						}
						if stc.Spec.CloudStorage.JournalDeviceSpec != nil {
							expectedNodeDrivesAfterExpansion++
						}
						if stc.Spec.CloudStorage.KvdbDeviceSpec != nil || stc.Spec.CloudStorage.SystemMdDeviceSpec != nil {
							expectedNodeDrivesAfterExpansion++
						}
						log.Infof("Expected node drives after pool [%s ] expansion for node [%s] is [%d], max cloud drives allowed [%d]", pool.Uuid, stNode.Name, expectedNodeDrivesAfterExpansion, maxCloudDrives)
						if expectedNodeDrivesAfterExpansion > maxCloudDrives {
							log.Infof("node %s  will reach max drives if pool %s expanded to size [%v] using add-drive", stNode.Name, pool.Uuid, targetSizeGiB)
							eligibilityMap[pool.Uuid] = false
							eligibilityMap[stNode.Id] = false
						}

					}

				}
			}
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
func GetPoolsDetailsOnNode(n *node.Node) ([]*opsapi.StoragePool, error) {
	var poolDetails []*opsapi.StoragePool

	// Refreshing Node Driver to make sure all changes done to the nodes are refreshed
	err := Inst().V.RefreshDriverEndpoints()
	if err != nil {
		return nil, err
	}
	//updating the node info after refresh
	stDriverNodes := node.GetStorageDriverNodes()
	for _, stDriverNode := range stDriverNodes {
		if stDriverNode.VolDriverNodeID == n.VolDriverNodeID {
			n = &stDriverNode
			break
		}
	}

	if node.IsStorageNode(*n) == false {
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

// IsEksCluster returns true if current operator installation is on an EKS cluster
func IsEksCluster() bool {
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

// MakeStoragetoStoragelessNode returns true on converting Storage Node to Storageless Node
func MakeStoragetoStoragelessNode(n node.Node) error {
	storageLessNodeBeforePoolDelete := node.GetStorageLessNodes()
	// Get total list of pools present on the node
	poolList, err := GetPoolsDetailsOnNode(&n)
	if err != nil {
		return err
	}

	lenPools := len(poolList)
	log.InfoD("total number of Pools present on the Node [%v] is [%d]", n.Name, lenPools)

	// Enter pool maintenance mode before deleting the pools from the cluster
	err = Inst().V.EnterPoolMaintenance(n)
	if err != nil {
		return fmt.Errorf("failed to set pool maintenance mode on node %s. Err: [%v]", n.Name, err)
	}

	time.Sleep(1 * time.Minute)
	expectedStatus := "In Maintenance"
	err = WaitForPoolStatusToUpdate(n, expectedStatus)
	if err != nil {
		return fmt.Errorf("node [%s] pools are not in status [%s]. Err: [%v]", n.Name, expectedStatus, err)
	}

	// Delete all the pools present on the Node
	for i := 0; i < lenPools; i++ {
		err := Inst().V.DeletePool(n, strconv.Itoa(i), true)
		if err != nil {
			return err
		}
	}

	err = Inst().V.ExitPoolMaintenance(n)
	if err != nil {
		return fmt.Errorf("failed to exit pool maintenance mode on node [%s] Error: [%v]", n.Name, err)
	}

	err = Inst().V.WaitDriverUpOnNode(n, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("volume driver down on node %s with Error: [%v]", n.Name, err)
	}
	expectedStatus = "Online"
	err = WaitForPoolStatusToUpdate(n, expectedStatus)
	if err != nil {
		return fmt.Errorf("node %s pools are not in status %s. Err:[%v]", n.Name, expectedStatus, err)
	}

	storageLessNodeAfterPoolDelete := node.GetStorageLessNodes()
	if len(storageLessNodeBeforePoolDelete) <= len(storageLessNodeAfterPoolDelete) {
		return fmt.Errorf("making storage node to storagelessnode failed")
	}
	return nil
}

// IsPksCluster returns true if current operator installation is on an EKS cluster
func IsPksCluster() bool {
	if stc, err := Inst().V.GetDriver(); err == nil {
		if oputil.IsPKS(stc) {
			log.InfoD("PKS installation with PX operator detected.")
			return true
		}
	}
	return false
}

// IsOkeCluster returns true if current operator installation is on an EKS cluster
func IsOkeCluster() bool {
	if stc, err := Inst().V.GetDriver(); err == nil {
		if oputil.IsOKE(stc) {
			log.InfoD("OKE installation with PX operator detected.")
			return true
		}
	}
	return false
}

// IsAksCluster returns true if current operator installation is on an EKS cluster
func IsAksCluster() bool {
	if stc, err := Inst().V.GetDriver(); err == nil {
		if oputil.IsAKS(stc) {
			log.InfoD("AKS installation with PX operator detected.")
			return true
		}
	}
	return false
}

// IsIksCluster returns true if current operator installation is on an EKS cluster
func IsIksCluster() bool {
	if stc, err := Inst().V.GetDriver(); err == nil {
		if oputil.IsIKS(stc) {
			log.InfoD("IKS installation with PX operator detected.")
			return true
		}
	}
	return false
}

// IsOpenShift returns true if current operator installation is on an EKS cluster
func IsOpenShift() bool {
	if stc, err := Inst().V.GetDriver(); err == nil {
		if oputil.IsOpenshift(stc) {
			log.InfoD("OpenShift installation with PX operator detected.")
			return true
		}
	}
	return false
}

// IsLocalCluster returns true if the cluster used is local cluster from vsphere
func IsLocalCluster(n node.Node) bool {
	response, err := IsCloudDriveInitialised(n)
	if err != nil || response == false {
		return false
	}
	return true
}

// IsPoolInMaintenance returns true if pool in maintenance
func IsPoolInMaintenance(n node.Node) bool {
	expectedStatus := "In Maintenance"
	poolsStatus, err := Inst().V.GetNodePoolsStatus(n)
	if err != nil || poolsStatus == nil {
		return false
	}

	for _, v := range poolsStatus {
		if v == expectedStatus {
			return true
		}
	}
	return false
}

// WaitForPoolOffline waits  till pool went to offline status
func WaitForPoolOffline(n node.Node) error {

	t := func() (interface{}, bool, error) {
		poolsStatus, err := Inst().V.GetNodePoolsStatus(n)
		if err != nil {
			return nil, true, err
		}

		for _, v := range poolsStatus {
			if v == "Offline" {
				return nil, false, nil
			}
		}
		return nil, true, fmt.Errorf("no pool is offline is node %s", n.Name)
	}
	_, err := task.DoRetryWithTimeout(t, time.Minute*360, time.Minute*2)
	return err
}

// GetPoolIDFromPoolUUID Returns Pool ID from Pool UUID
func GetPoolIDFromPoolUUID(poolUuid string) (int32, error) {
	nodesPresent := node.GetStorageNodes()
	for _, each := range nodesPresent {
		poolsPresent, err := GetPoolsDetailsOnNode(&each)
		if err != nil {
			return -1, err
		}
		for _, eachPool := range poolsPresent {
			if eachPool.Uuid == poolUuid {
				return eachPool.ID, nil
			}
		}
	}
	return -1, nil
}

// GetPoolObjFromPoolIdOnNode Returns pool object from pool ID on a specific Node
func GetPoolObjFromPoolIdOnNode(n *node.Node, poolID int) (*opsapi.StoragePool, error) {
	poolDetails, err := GetPoolsDetailsOnNode(n)
	if err != nil {
		return nil, err
	}
	for _, eachPool := range poolDetails {
		if eachPool.ID == int32(poolID) {
			return eachPool, nil
		}
	}
	return nil, fmt.Errorf("Failed to get details of Storage Pool On specific Node ")
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

// WaitForPoolStatusToUpdate returns true when pool status updated to expected status
func WaitForPoolStatusToUpdate(nodeSelected node.Node, expectedStatus string) error {
	t := func() (interface{}, bool, error) {
		poolsStatus, err := Inst().V.GetNodePoolsStatus(nodeSelected)
		if err != nil {
			return nil, true,
				fmt.Errorf("error getting pool status on node %s,err: %v", nodeSelected.Name, err)
		}
		if poolsStatus == nil {
			return nil,
				false, fmt.Errorf("pools status is nil")
		}
		for k, v := range poolsStatus {
			if v != expectedStatus {
				return nil, true,
					fmt.Errorf("pool %s is not %s, current status: %s", k, expectedStatus, v)
			}
		}
		return nil, false, nil
	}
	_, err := task.DoRetryWithTimeout(t, 30*time.Minute, 2*time.Minute)
	return err
}

// RandomString generates a random lowercase string of length characters.
func RandomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	const letters = "abcdefghijklmnopqrstuvwxyz"
	randomBytes := make([]byte, length)
	for i := range randomBytes {
		randomBytes[i] = letters[rand.Intn(len(letters))]
	}
	randomString := string(randomBytes)
	return randomString
}

func EnterPoolMaintenance(stNode node.Node) error {
	log.InfoD("Setting pools in maintenance on node %s", stNode.Name)
	if err := Inst().V.EnterPoolMaintenance(stNode); err != nil {
		return err
	}

	status, err := Inst().V.GetNodeStatus(stNode)
	if err != nil {
		return err
	}
	log.InfoD("Node [%s] has status: [%v] after entering pool maintenance", stNode.Name, status)

	expectedStatus := "In Maintenance"
	if err = WaitForPoolStatusToUpdate(stNode, expectedStatus); err != nil {
		return fmt.Errorf("node %s pools are not in status %s. Err:%v", stNode.Name, expectedStatus, err)
	}

	return nil
}

func ExitPoolMaintenance(stNode node.Node) error {
	var exitErr error
	if exitErr = Inst().V.ExitPoolMaintenance(stNode); exitErr != nil {
		return fmt.Errorf("error exiting pool maintenance in the node [%v]. Err: %v", stNode.Name, exitErr)
	}

	if exitErr = Inst().V.WaitDriverUpOnNode(stNode, 5*time.Minute); exitErr != nil {
		return fmt.Errorf("error waiting for driver up after exiting pool maintenance in the node [%v]. Err: %v", stNode.Name, exitErr)
	}

	// wait as even PX is up it is taking some time for pool status to update when all pools are deleted
	time.Sleep(1 * time.Minute)
	cmd := "pxctl sv pool show"
	var out string

	// Execute the command and check if any pools exist
	out, exitErr = Inst().N.RunCommandWithNoRetry(stNode, cmd, node.ConnectionOpts{
		Timeout:         2 * time.Minute,
		TimeBeforeRetry: 10 * time.Second,
	})
	if exitErr != nil {
		return fmt.Errorf("error checking pools in the node [%v]. Err: %v", stNode.Name, exitErr)
	}
	log.Infof("pool show: [%s]", out)

	// skipping waitForPoolStatusToUpdate if there are no pools in the node
	if strings.Contains(out, "No drives configured for this node") {
		return nil
	}

	expectedStatus := "Online"
	if exitErr = WaitForPoolStatusToUpdate(stNode, expectedStatus); exitErr != nil {
		return fmt.Errorf("pools are not online after exiting pool maintenance in the node [%v],Err: %v", stNode.Name, exitErr)
	}

	return nil
}

// DeleteGivenPoolInNode deletes pool with given ID in the given node
func DeleteGivenPoolInNode(stNode node.Node, poolIDToDelete string, retry bool) (err error) {

	// Moving repls on the node before deletion
	nodeVols, err := GetVolumesOnNode(stNode.VolDriverNodeID)
	if err != nil {
		return fmt.Errorf("error getting volumes node [%s],Err: %v ", stNode.Name, err)
	}

	for _, vol := range nodeVols {
		newReplicaNode, err := GetNodeIdToMoveReplica(vol)
		log.Infof("New Replica node is [%s]", newReplicaNode)
		if err != nil {
			return fmt.Errorf("error getting replica node for volume [%s],Err: %v ", vol, err)
		}

		err = MoveReplica(vol, stNode.VolDriverNodeID, newReplicaNode)
		if err != nil {
			return fmt.Errorf("error moving replica from node [%s] to volume [%s],Err: %v ", stNode.VolDriverNodeID, newReplicaNode, err)
		}
	}

	if err := EnterPoolMaintenance(stNode); err != nil {
		return err
	}
	if err := Inst().V.DeletePool(stNode, poolIDToDelete, retry); err != nil {
		return err
	}

	err = ExitPoolMaintenance(stNode)

	if err != nil && !strings.Contains(err.Error(), "not in pool maintenance mode") {
		return err
	}
	return nil
}

func GetPoolUUIDWithMetadataDisk(stNode node.Node) (string, error) {

	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         2 * time.Minute,
			TimeBeforeRetry: defaultRetryInterval,
		},
		Action: "start",
	}
	drivesMap, err := Inst().N.GetBlockDrives(stNode, systemOpts)
	if err != nil {
		return "", fmt.Errorf("error getting block drives from node %s, Err :%v", stNode.Name, err)
	}

	var metadataPoolID string
outer:
	for _, drv := range drivesMap {
		for k, v := range drv.Labels {
			if k == "mdpoolid" {
				metadataPoolID = v
				break outer
			}
		}
	}

	if metadataPoolID != "" {
		mpID, err := strconv.Atoi(metadataPoolID)
		if err != nil {
			return "", fmt.Errorf("error converting metadataPoolID [%v] to int. Error: %v", metadataPoolID, err)
		}

		for _, p := range stNode.Pools {
			if p.ID == int32(mpID) {
				log.Infof("Identified metadata pool UUID: %s", p.Uuid)
				return p.Uuid, nil
			}
		}
	}

	return "", fmt.Errorf("no pool with metadata in node [%s]", stNode.Name)
}

// SetupTestRail checks if the required parameters for testrail are passed, verifies connectivity and creates milestone if it does not exist
func SetupTestRail() {
	if testRailHostname != "" && testRailUsername != "" && testRailPassword != "" {
		err := testrailuttils.Init(testRailHostname, testRailUsername, testRailPassword)
		if err == nil {
			if testrailuttils.MilestoneName == "" || testrailuttils.RunName == "" || testrailuttils.JobRunID == "" {
				err = fmt.Errorf("not all details provided to update testrail")
				log.FailOnError(err, "Error occurred while testrail initialization")
			}
			testrailuttils.CreateMilestone()
			TestRailSetupSuccessful = true
		}
	} else {
		log.Debugf("Not all information to connect to testrail is provided, skipping updates to testrail")
	}
}

// AsgKillNode terminates the given node
func AsgKillNode(nodeToKill node.Node) error {
	initNodes := node.GetStorageDriverNodes()
	initNodeNames := make([]string, len(initNodes))
	var err error
	for _, iNode := range initNodes {
		initNodeNames = append(initNodeNames, iNode.Name)
	}
	stepLog := fmt.Sprintf("Deleting node [%v]", nodeToKill.Name)

	Step(stepLog, func() {
		log.InfoD(stepLog)
		// workaround for eks until EKS sdk is implemented
		if IsEksCluster() {
			_, err = Inst().N.RunCommandWithNoRetry(nodeToKill, "ifconfig eth0 down  < /dev/null &", node.ConnectionOpts{
				Timeout:         2 * time.Minute,
				TimeBeforeRetry: 10 * time.Second,
			})

		} else {
			err = Inst().S.DeleteNode(nodeToKill)
		}

	})
	if err != nil {
		return err
	}

	stepLog = "Wait for  node get replaced by autoscaling group"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		t := func() (interface{}, bool, error) {

			err = Inst().S.RefreshNodeRegistry()
			if err != nil {
				return "", true, err
			}

			err = Inst().V.RefreshDriverEndpoints()
			if err != nil {
				return "", true, err
			}

			newNodesList := node.GetStorageDriverNodes()

			for _, nNode := range newNodesList {
				if !Contains(initNodeNames, nNode.Name) {
					return "", false, nil
				}
			}

			return "", true, fmt.Errorf("no new node scaled up")
		}

		_, err = task.DoRetryWithTimeout(t, defaultAutoStorageNodeRecoveryTimeout, waitResourceCleanup)

	})
	return err

}

// RefreshDriverEndPoints returns nil if refreshing driver endpoint is successful
func RefreshDriverEndPoints() error {
	var err error
	err = Inst().V.RefreshDriverEndpoints()
	if err != nil {
		return err
	}
	return nil
}

// GetVolumesFromPoolID returns list of volumes from pool uuid
func GetVolumesFromPoolID(contexts []*scheduler.Context, poolUuid string) ([]*volume.Volume, error) {
	var volumes []*volume.Volume

	// Refresh driver end points before processing
	err := RefreshDriverEndPoints()
	if err != nil {
		return nil, err
	}
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
			poolUuids := appVol.ReplicaSets[0].PoolUuids
			for _, p := range poolUuids {
				if p == poolUuid {
					volumes = append(volumes, vol)
				}
			}
		}
	}
	return volumes, nil
}

// DoRetryWithTimeoutWithGinkgoRecover calls `task.DoRetryWithTimeout` along with `ginkgo.GinkgoRecover()`, to be used in callbacks with panics or ginkgo assertions
func DoRetryWithTimeoutWithGinkgoRecover(taskFunc func() (interface{}, bool, error), timeout, timeBeforeRetry time.Duration) (interface{}, error) {
	taskFuncWithGinkgoRecover := func() (interface{}, bool, error) {
		defer ginkgo.GinkgoRecover()
		return taskFunc()
	}
	return task.DoRetryWithTimeout(taskFuncWithGinkgoRecover, timeout, timeBeforeRetry)
}

// GetVolumesInDegradedState Get the list of volumes in degraded state
func GetVolumesInDegradedState(contexts []*scheduler.Context) ([]*volume.Volume, error) {
	var volumes []*volume.Volume
	err := RefreshDriverEndPoints()
	if err != nil {
		return nil, err
	}
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
			log.InfoD(fmt.Sprintf("Current Status of the volume [%v] is [%v]", vol.Name, appVol.Status))
			if fmt.Sprintf("[%v]", appVol.Status.String()) != "VOLUME_STATUS_DEGRADED" {
				volumes = append(volumes, vol)
			}
		}
	}
	return volumes, nil
}

// IsVolumeStatusUP returns true is volume status is up
func IsVolumeStatusUP(vol *volume.Volume) (bool, error) {
	appVol, err := Inst().V.InspectVolume(vol.ID)
	if err != nil {
		return false, err
	}
	if fmt.Sprintf("%v", appVol.Status.String()) != "VOLUME_STATUS_UP" {
		return false, nil
	}
	log.InfoD("volume [%v] status is [%v]", vol.Name, appVol.Status.String())
	return true, nil
}

// GetVolumeReplicationStatus returns PX volume replication status
func GetVolumeReplicationStatus(vol *volume.Volume) (string, error) {
	apiVol, err := Inst().V.InspectVolume(vol.ID)
	if err != nil {
		return "", err
	}
	cmd := fmt.Sprintf("pxctl volume inspect %s | grep \"Replication Status\"", apiVol.Id)

	output, err := Inst().N.RunCommand(node.GetStorageDriverNodes()[0], cmd, node.ConnectionOpts{
		Timeout:         1 * time.Minute,
		TimeBeforeRetry: 5 * time.Second,
		Sudo:            true,
	})
	if err != nil {
		return "", err
	}
	//sample output : "Replication Status       :  Up"
	output = strings.Split(strings.TrimSpace(output), ":")[1]
	return strings.TrimSpace(output), nil
}

type KvdbNode struct {
	PeerUrls   []string `json:"PeerUrls"`
	ClientUrls []string `json:"ClientUrls"`
	Leader     bool     `json:"Leader"`
	DbSize     int      `json:"DbSize"`
	IsHealthy  bool     `json:"IsHealthy"`
	ID         string   `json:"ID"`
}

// GetAllKvdbNodes returns list of all kvdb nodes present in the cluster
func GetAllKvdbNodes() ([]KvdbNode, error) {
	type kvdbNodes []map[string]KvdbNode
	storageNodes := node.GetStorageNodes()
	randomIndex := rand.Intn(len(storageNodes))
	randomNode := storageNodes[randomIndex]

	jsonConvert := func(jsonString string) ([]KvdbNode, error) {
		var nodes kvdbNodes
		var kvdb KvdbNode
		var kvdbNodes []KvdbNode

		err := json.Unmarshal([]byte(fmt.Sprintf("[%s]", jsonString)), &nodes)
		if err != nil {
			return nil, err
		}
		for _, nodeMap := range nodes {
			for id, value := range nodeMap {
				kvdb.ID = id
				kvdb.Leader = value.Leader
				kvdb.IsHealthy = value.IsHealthy
				kvdb.ClientUrls = value.ClientUrls
				kvdb.DbSize = value.DbSize
				kvdb.PeerUrls = value.PeerUrls
				kvdbNodes = append(kvdbNodes, kvdb)
			}
		}
		return kvdbNodes, nil
	}

	var allKvdbNodes []KvdbNode
	// Execute the command and check the alerts of type POOL
	command := "pxctl service kvdb members list -j"
	out, err := Inst().N.RunCommand(randomNode, command, node.ConnectionOpts{
		Timeout:         2 * time.Minute,
		TimeBeforeRetry: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	log.InfoD("List of KVDBMembers in the cluster [%v]", out)

	// Convert KVDB members to map
	allKvdbNodes, err = jsonConvert(out)
	if err != nil {
		return nil, err
	}
	return allKvdbNodes, nil
}

func GetKvdbMasterNode() (*node.Node, error) {
	var getKvdbLeaderNode node.Node
	allkvdbNodes, err := GetAllKvdbNodes()
	if err != nil {
		return nil, err
	}

	for _, each := range allkvdbNodes {
		if each.Leader {
			getKvdbLeaderNode, err = node.GetNodeDetailsByNodeID(each.ID)
			if err != nil {
				return nil, err
			}
			break
		}
	}
	return &getKvdbLeaderNode, nil
}

// KillKvdbMasterNodeAndFailover kills kvdb master node and returns after failover is complete
func KillKvdbMasterNodeAndFailover() error {
	kvdbMaster, err := GetKvdbMasterNode()
	if err != nil {
		return err
	}
	err = KillKvdbMemberUsingPid(*kvdbMaster)
	if err != nil {
		return err
	}
	err = WaitForKVDBMembers()
	if err != nil {
		return err
	}
	return nil
}

// GetKvdbMasterPID returns the PID of KVDB master node
func GetKvdbMasterPID(kvdbNode node.Node) (string, error) {
	var processPid string
	command := "ps -ef | grep -i px-etcd"
	out, err := Inst().N.RunCommand(kvdbNode, command, node.ConnectionOpts{
		Timeout:         20 * time.Second,
		TimeBeforeRetry: 5 * time.Second,
		Sudo:            true,
	})
	if err != nil {
		return "", err
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		if strings.Contains(line, "px-etcd start") && !strings.Contains(line, "grep") {
			fields := strings.Fields(line)
			processPid = fields[1]
			break
		}
	}
	return processPid, err
}

// WaitForKVDBMembers waits till all kvdb members comes up online and healthy
func WaitForKVDBMembers() error {
	t := func() (interface{}, bool, error) {
		allKvdbNodes, err := GetAllKvdbNodes()
		if err != nil {
			return "", true, err
		}
		if len(allKvdbNodes) != 3 {
			return "", true, fmt.Errorf("not all kvdb nodes are online")
		}

		for _, each := range allKvdbNodes {
			if each.IsHealthy == false {
				return "", true, fmt.Errorf("all kvdb nodes are not healthy")
			}
		}
		log.Info("all kvdb nodes are healthy")
		return "", false, nil
	}
	_, err := task.DoRetryWithTimeout(t, defaultKvdbRetryInterval, 20*time.Second)
	if err != nil {
		return err
	}
	return nil
}

// KillKvdbMemberUsingPid return error in case of command failure
func KillKvdbMemberUsingPid(kvdbNode node.Node) error {
	pid, err := GetKvdbMasterPID(kvdbNode)
	if err != nil {
		return err
	}
	command := fmt.Sprintf("kill -9 %s", pid)
	log.InfoD("killing PID using command [%s]", command)
	err = runCmd(command, kvdbNode)
	if err != nil {
		return err
	}
	return nil
}

// IsKVDBNode returns true if a node is kvdb node else it returns false
func IsKVDBNode(n node.Node) (bool, error) {
	KvdbNodes, err := GetAllKvdbNodes()
	if err != nil {
		return false, err
	}
	for _, each := range KvdbNodes {
		if each.ID == n.Id {
			return true, nil
		}
	}
	return false, nil
}

// getReplicaNodes returns the list of nodes which has replicas
func getReplicaNodes(vol *volume.Volume) ([]string, error) {
	getReplicaSets, err := Inst().V.GetReplicaSets(vol)
	if err != nil {
		return nil, err
	}
	return getReplicaSets[0].Nodes, nil
}

// IsDMthin returns true if setup is dmthin enabled
func IsDMthin() (bool, error) {
	opBasedInstall, _ := Inst().V.IsOperatorBasedInstall()
	dmthinEnabled := false
	if !opBasedInstall {
		log.Warn("This is not PX Operator based install, DMTHIN is not supported on legacy Daemonset installs, skipping this check..")
		return dmthinEnabled, nil
	}

	cluster, err := Inst().V.GetDriver()
	if err != nil {
		return dmthinEnabled, err
	}
	argsList, err := util.MiscArgs(cluster)
	for _, args := range argsList {
		if strings.Contains(strings.ToLower(args), "px-storev2") {
			dmthinEnabled = true
		}
	}
	return dmthinEnabled, nil
}

// AddMetadataDisk add metadisk to the node if not already exists
func AddMetadataDisk(n node.Node) error {
	drivesMap, err := Inst().N.GetBlockDrives(n, node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         2 * time.Minute,
			TimeBeforeRetry: defaultRetryInterval,
		},
		Action: "start",
	})
	if err != nil {
		return err
	}

	isDedicatedMetadataDiskExist := false

	for _, v := range drivesMap {
		for lk := range v.Labels {
			if lk == "mdvol" {
				isDedicatedMetadataDiskExist = true
			}
		}
	}

	if !isDedicatedMetadataDiskExist {
		cluster, err := Inst().V.GetDriver()
		log.FailOnError(err, "error getting storage cluster")
		metadataSpec := cluster.Spec.CloudStorage.SystemMdDeviceSpec
		deviceSpec := fmt.Sprintf("%s --metadata", *metadataSpec)

		log.InfoD("Initiate add cloud drive and validate")
		err = Inst().V.AddCloudDrive(&n, deviceSpec, -1)

		if err != nil {
			return fmt.Errorf("error adding metadata device [%s] to node [%s]. Err: %v", *metadataSpec, n.Name, err)
		}

	}

	return nil

}

// CreateNamespaces Create N number of namespaces and return namespace list
func CreateNamespaces(nsName string, numberOfNamespaces int) ([]string, error) {

	// Create multiple namespaces in string
	var (
		namespaces []string
	)
	namespace := fmt.Sprintf("large-resource-%v", time.Now().Unix())
	if nsName != "" {
		namespace = fmt.Sprintf("%s", nsName)
	}

	// Create a good number of namespaces
	for nsCount := 0; nsCount < numberOfNamespaces; nsCount++ {
		namespace := fmt.Sprintf("%v-%v", namespace, nsCount)
		nsName := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			},
		}
		log.InfoD("Creating namespace %v", namespace)
		_, err := k8sCore.CreateNamespace(nsName)
		if err != nil {
			return nil, err
		} else {
			namespaces = append(namespaces, namespace)
		}
	}
	return namespaces, nil
}

// createConfigMaps with the given number of entries on specified namespaces
func createConfigMaps(namespaces []string, numberOfConfigmap int, numberOfEntries int) error {

	// Create random data to add in ConfigMap
	var randomData = make(map[string]string)
	for i := 0; i < numberOfEntries; i++ {
		randomString := RandomString(80)
		randomStringWithTimeStamp := fmt.Sprintf("%v.%v", randomString, time.Now().Unix())
		randomData[randomStringWithTimeStamp] = randomString
	}

	// create the number of configmaps needed
	k8sCore = core.Instance()
	for _, namespace := range namespaces {
		for i := 0; i < numberOfConfigmap; i++ {
			name := fmt.Sprintf("configmap-%s-%d-%v", namespace, i, time.Now().Unix())
			log.InfoD("Creating Configmap: %v", name)
			metaObj := metaV1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			}
			obj := &corev1.ConfigMap{
				ObjectMeta: metaObj,
				Data:       randomData,
			}

			_, err := k8sCore.CreateConfigMap(obj)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// createSecrets with the given number of entries on specified namespaces
func createSecrets(namespaces []string, numberOfSecrets int, numberOfEntries int) error {

	// Create random data to add in ConfigMap
	var randomData = make(map[string]string)
	for i := 0; i < numberOfEntries; i++ {
		randomString := RandomString(80)
		randomStringWithTimeStamp := fmt.Sprintf("%v.%v", randomString, time.Now().Unix())
		randomData[randomStringWithTimeStamp] = randomString
	}

	// create the number of configmaps needed
	k8sCore = core.Instance()
	for _, namespace := range namespaces {
		for i := 0; i < numberOfSecrets; i++ {
			name := fmt.Sprintf("secret-%s-%d-%v", namespace, i, time.Now().Unix())
			log.InfoD("Creating secret: %v", name)
			metaObj := metaV1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			}
			obj := &corev1.Secret{
				ObjectMeta: metaObj,
				StringData: randomData,
			}

			_, err := k8sCore.CreateSecret(obj)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteNamespaces Deletes all the namespaces given in a list of namespaces and return error if any
func DeleteNamespaces(namespaces []string) error {
	// Delete a list of namespaces given
	k8sCore = core.Instance()
	for _, namespace := range namespaces {
		err := k8sCore.DeleteNamespace(namespace)
		if err != nil {
			return err
		}
	}
	return nil
}

// VerifyNilPointerDereferenceError returns true if nil pointer dereference, output of the log messages
func VerifyNilPointerDereferenceError(n *node.Node) (bool, string, error) {

	cmdGrepOutput := "journalctl | grep -i -A 50 \"nil pointer dereference\""
	output, err := runCmdOnce(cmdGrepOutput, *n)
	if err != nil {
		return false, "", fmt.Errorf("command failed while running [%v] on Node [%v]", cmdGrepOutput, n.Name)
	}
	re, err := regexp.Compile("panic: runtime error.*invalid memory address or nil pointer dereference")
	if err != nil {
		return false, "", fmt.Errorf("command failed running [%v] on Node [%v]", cmdGrepOutput, n.Name)
	}
	if re.MatchString(fmt.Sprintf("%v", output)) {
		return true, output, nil
	}

	return false, "", nil
}

// GetClusterProviders returns the list of cluster providers
func GetClusterProviders() []string {
	providersStr := os.Getenv("CLUSTER_PROVIDER")
	if providersStr != "" {
		return strings.Split(providersStr, ",")
	}
	return clusterProviders
}

// GetPoolUuidsWithStorageFull returns list of pool uuids if storage full
func GetPoolUuidsWithStorageFull() ([]string, error) {
	var poolUuids []string

	isPoolOffline := func(n node.Node) (bool, error) {
		poolsStatus, err := Inst().V.GetNodePoolsStatus(n)
		if err != nil {
			return false, err
		}

		for _, v := range poolsStatus {
			if v == "Offline" {
				return true, nil
			}
		}
		return false, nil
	}

	pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
	if err != nil {
		return nil, err
	}
	calculatePercentage := func(usedValue float64, totalValue float64) float64 {
		percentage := (usedValue / totalValue) * 100
		return float64(percentage)
	}

	for _, eachPool := range pools {
		nodeDetail, err := GetNodeWithGivenPoolID(eachPool.Uuid)
		if err != nil {
			return nil, err
		}

		poolOfflineStatus, err := isPoolOffline(*nodeDetail)
		if err != nil {
			return nil, err
		}

		if calculatePercentage(float64(eachPool.Used), float64(eachPool.TotalSize)) > 90.0 || poolOfflineStatus {
			poolUuids = append(poolUuids, eachPool.GetUuid())
		}
	}

	return poolUuids, nil
}

// GetVolumeConsumedSize returns size of the volume
func GetVolumeConsumedSize(vol volume.Volume) (uint64, error) {
	apiVol, err := Inst().V.InspectVolume(vol.ID)
	if err != nil {
		return 0, err
	}
	return apiVol.Usage, nil
}

// GetAllVolumesWithIO Returns list of volumes with IO
func GetAllVolumesWithIO(contexts []*scheduler.Context) ([]*volume.Volume, error) {

	allVolsWithIO := []*volume.Volume{}
	for _, eachContext := range contexts {
		vols, err := Inst().S.GetVolumes(eachContext)
		if err != nil {
			log.Errorf("Failed to get app %s's volumes", eachContext.App.Key)
		}
		log.Infof("list of all volumes present in the cluster [%v]", vols)
		for _, eachVol := range vols {
			isIOsInProgress, err := Inst().V.IsIOsInProgressForTheVolume(&node.GetStorageNodes()[0], eachVol.ID)
			if err != nil {
				return nil, err
			}

			if isIOsInProgress {
				allVolsWithIO = append(allVolsWithIO, eachVol)
			}
		}
	}
	return allVolsWithIO, nil
}

func IsVolumeFull(vol volume.Volume) (bool, error) {
	volPercentage, err := GetVolumeFullPercentage(vol)
	if err != nil {
		return false, err
	}
	if volPercentage > 80.0 {
		return true, nil
	}

	return false, nil
}

// GetVolumeFullPercentage returns percentage of volume size consumed
func GetVolumeFullPercentage(vol volume.Volume) (float64, error) {
	calculatePercentage := func(usedSize float64, totalSize float64) float64 {
		percentage := (usedSize / totalSize) * 100
		return float64(percentage)
	}

	appVol, err := Inst().V.InspectVolume(vol.ID)
	log.FailOnError(err, fmt.Sprintf("err inspecting vol : %s", vol.ID))

	totalSize := appVol.Spec.Size
	usedSize, err := GetVolumeConsumedSize(vol)
	if err != nil {
		return 0, err
	}

	log.Infof("Total size of Volume is [%v] and used size is [%v]", totalSize, usedSize)
	percentageFull := calculatePercentage(float64(usedSize), float64(totalSize))
	log.Infof("Volume [%v] : Percentage of size consumed [%v]", vol.Name, percentageFull)

	return percentageFull, nil
}

// GetPoolCapacityUsed Get Pool capacity percentage used
func GetPoolCapacityUsed(poolUUID string) (float64, error) {

	calculatePercentage := func(usedSize float64, totalSize float64) float64 {
		percentage := (usedSize / totalSize) * 100
		return float64(percentage)
	}

	pool, err := GetStoragePoolByUUID(poolUUID)
	log.FailOnError(err, "Failed to get pool Details from PoolUUID [%v]", poolUUID)

	usedSize, totalSize := pool.Used, pool.TotalSize
	log.Infof("Used vs Total volume stats [%v]/[%v]", usedSize, totalSize)

	poolSizeUsed := calculatePercentage(float64(usedSize), float64(totalSize))

	return poolSizeUsed, nil
}

// GetRandomNode Gets Random node
func GetRandomNode(pxNodes []node.Node) node.Node {
	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(pxNodes))
	randomNode := pxNodes[randomIndex]
	return randomNode
}

func RemoveLabelsAllNodes(label string, forStorage, forStorageLess bool) error {
	if !forStorage && !forStorageLess {
		return errors.New("at least one of forStorage or forStorageLess must be true")
	}

	var pxNodes []node.Node

	if forStorage && forStorageLess {
		pxNodes = node.GetStorageDriverNodes()
	} else if forStorage {
		pxNodes = node.GetStorageNodes()
	} else if forStorageLess {
		pxNodes = node.GetStorageLessNodes()
	}

	for _, node := range pxNodes {
		log.Infof("Node Name: %s\n", node.Name)
		if err := Inst().S.RemoveLabelOnNode(node, label); err != nil {
			return fmt.Errorf("error removing label on node [%s]: %w", node.Name, err)
		}
	}

	return nil
}

func AddCloudDrive(stNode node.Node, poolID int32) error {
	driveSpecs, err := GetCloudDriveDeviceSpecs()
	if err != nil {
		return fmt.Errorf("error getting cloud drive specs, err: %v", err)
	}
	deviceSpec := driveSpecs[0]
	deviceSpecParams := strings.Split(deviceSpec, ",")
	var specSize uint64
	var driveSize string

	if poolID != -1 {
		systemOpts := node.SystemctlOpts{
			ConnectionOpts: node.ConnectionOpts{
				Timeout:         2 * time.Minute,
				TimeBeforeRetry: defaultRetryInterval,
			},
			Action: "start",
		}
		drivesMap, err := Inst().N.GetBlockDrives(stNode, systemOpts)
		if err != nil {
			return fmt.Errorf("error getting block drives from node %s, Err :%v", stNode.Name, err)
		}

	outer:
		for _, v := range drivesMap {
			labels := v.Labels
			for _, pID := range labels {
				if pID == fmt.Sprintf("%d", poolID) {
					driveSize = v.Size
					i := strings.Index(driveSize, "G")
					driveSize = driveSize[:i]
					break outer
				}
			}
		}
	}

	if driveSize != "" {
		paramsArr := make([]string, 0)
		for _, param := range deviceSpecParams {
			if strings.Contains(param, "size") {
				paramsArr = append(paramsArr, fmt.Sprintf("size=%s,", driveSize))
			} else {
				paramsArr = append(paramsArr, param)
			}
		}
		deviceSpec = strings.Join(paramsArr, ",")
		specSize, err = strconv.ParseUint(driveSize, 10, 64)
		if err != nil {
			return fmt.Errorf("error converting size to uint64, err: %v", err)
		}
	}

	pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
	if err != nil {
		return fmt.Errorf("error getting pools list, err: %v", err)
	}
	dash.VerifyFatal(len(pools) > 0, true, "Verify pools exist")

	var currentTotalPoolSize uint64

	for _, pool := range pools {
		currentTotalPoolSize += pool.GetTotalSize() / units.GiB
	}

	log.Info(fmt.Sprintf("current pool size: %d GiB", currentTotalPoolSize))

	expectedTotalPoolSize := currentTotalPoolSize + specSize

	log.InfoD("Initiate add cloud drive and validate")
	err = Inst().V.AddCloudDrive(&stNode, deviceSpec, poolID)
	if err != nil {
		return fmt.Errorf("add cloud drive failed on node %s, err: %v", stNode.Name, err)
	}

	log.InfoD("Validate pool rebalance after drive add")
	err = ValidateDriveRebalance(stNode)
	if err != nil {
		return fmt.Errorf("pool re-balance failed, err: %v", err)
	}
	n1, err := node.GetNodeByName(stNode.Name)
	if err != nil {
		return fmt.Errorf("error getting node by name %s, err: %v", stNode.Name, err)
	}
	err = Inst().V.WaitDriverUpOnNode(n1, addDriveUpTimeOut)
	if err != nil {
		return fmt.Errorf("volume driver is down on node %s, err: %v", stNode.Name, err)
	}
	dash.VerifyFatal(err == nil, true, "PX is up after add drive")

	var newTotalPoolSize uint64

	pools, err = Inst().V.ListStoragePools(metav1.LabelSelector{})
	if err != nil {
		return fmt.Errorf("error getting pools list, err: %v", err)
	}
	dash.VerifyFatal(len(pools) > 0, true, "Verify pools exist")
	for _, pool := range pools {
		newTotalPoolSize += pool.GetTotalSize() / units.GiB
	}
	isPoolSizeUpdated := false

	if newTotalPoolSize >= expectedTotalPoolSize {
		isPoolSizeUpdated = true
	}
	log.Info(fmt.Sprintf("updated pool size: %d GiB", newTotalPoolSize))
	dash.VerifyFatal(isPoolSizeUpdated, true, fmt.Sprintf("Validate total pool size after add cloud drive on node %s", stNode.Name))
	err = Inst().V.RefreshDriverEndpoints()
	return err
}

func IsJournalEnabled() (bool, error) {
	storageSpec, err := Inst().V.GetStorageSpec()
	if err != nil {
		return false, err
	}
	jDev := storageSpec.GetJournalDev()
	if jDev != "" {
		log.Infof("JournalDev: %s", jDev)
		return true, nil
	}
	return false, nil
}

func runDataIntegrityValidation(testName string) bool {

	if Inst().N.String() == ssh.DriverName || Inst().N.String() == vsphere.DriverName {
		if strings.Contains(testName, "{") {
			i := strings.Index(testName, "{")
			j := strings.Index(testName, "}")

			testName = testName[i+1 : j]
		}
		log.Infof("validating test-name [%s] for running data integrity validation", testName)
		if strings.Contains(dataIntegrityValidationTests, testName) {
			return true
		}

		if strings.Contains(testName, "Longevity") {
			pc, _, _, _ := runtime.Caller(1)
			if strings.Contains(dataIntegrityValidationTests, runtime.FuncForPC(pc).Name()) {
				return true
			}
			return false
		}
	}

	return false
}

func ValidateDataIntegrity(contexts *[]*scheduler.Context) (mError error) {
	testName := ginkgo.CurrentSpecReport().FullText()
	if strings.Contains(testName, "Longevity") || strings.Contains(testName, "Trigger") {
		pc, _, _, _ := runtime.Caller(1)
		testName = runtime.FuncForPC(pc).Name()
		sInd := strings.LastIndex(testName, ".")
		if sInd != -1 {
			fnNames := strings.Split(testName, ".")
			for _, fn := range fnNames {
				if strings.Contains(fn, "Trigger") {
					testName = fn
					break
				}
			}
		}

	}

	if !runDataIntegrityValidation(testName) {
		log.Infof(fmt.Sprintf("skipping data integrity validation for %s is not the list %s", testName, dataIntegrityValidationTests))
		return nil
	}

outer:
	for _, ctx := range *contexts {
		appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
		var timeout time.Duration
		if ctx.ReadinessTimeout == time.Duration(0) {
			timeout = appScaleFactor * defaultTimeout
		} else {
			timeout = appScaleFactor * ctx.ReadinessTimeout
		}
		//Waiting for all the apps in ctx are running
		mError = Inst().S.WaitForRunning(ctx, timeout, defaultRetryInterval)
		if mError != nil {
			return mError
		}
		var appVolumes []*volume.Volume
		appVolumes, mError = Inst().S.GetVolumes(ctx)
		if mError != nil {
			return mError
		}

		//waiting for volumes replication status should be up before calculating md5sum
		for _, v := range appVolumes {
			var replicaSets []*opsapi.ReplicaSet
			replicaSets, mError = Inst().V.GetReplicaSets(v)
			if mError != nil {
				return mError
			}

			//skipping the validation if volume is repl 2
			if len(replicaSets) == 1 && len(replicaSets[0].PoolUuids) == 1 {
				continue outer
			}
			t := func() (interface{}, bool, error) {
				replicationStatus, err := GetVolumeReplicationStatus(v)
				if err != nil {
					return nil, false, err
				}

				if replicationStatus == "Up" {
					return "", false, nil
				}
				if replicationStatus == "Resync" {
					return "", true, fmt.Errorf("volume %s is still in Resync state", v.ID)
				}
				return "", false, fmt.Errorf("volume %s is in %s state cannot proceed further", v.ID, replicationStatus)
			}
			_, mError = task.DoRetryWithTimeout(t, 120*time.Minute, 2*time.Minute)
			if mError != nil {
				return mError
			}
		}

		log.InfoD(fmt.Sprintf("scale down app %s to 0", ctx.App.Key))
		defer func() {
			if tempErr := revertAppScale(ctx); tempErr != nil {
				mError = multierr.Append(mError, tempErr)
			}
		}()
		applicationScaleMap, err := Inst().S.GetScaleFactorMap(ctx)
		if err != nil {
			mError = multierr.Append(mError, err)
		}

		applicationScaleDownMap := make(map[string]int32, len(ctx.App.SpecList))

		for name := range applicationScaleMap {
			applicationScaleDownMap[name] = 0
		}

		err = Inst().S.ScaleApplication(ctx, applicationScaleDownMap)
		if err != nil {
			mError = multierr.Append(mError, err)
			return mError
		}
		//waiting for volumes to be detached after scale down
		for _, v := range appVolumes {
			t := func() (interface{}, bool, error) {
				apiVol, err := Inst().V.InspectVolume(v.ID)
				if err != nil {
					return "", false, err
				}
				if len(apiVol.AttachedOn) == 0 {
					return "", false, nil
				}
				return "", true, fmt.Errorf("volume %s is still attached to %s", v.ID, apiVol.AttachedOn)
			}
			_, err = task.DoRetryWithTimeout(t, 15*time.Minute, 10*time.Second)
			if err != nil {
				mError = multierr.Append(mError, err)
				return mError

			}
		}

		poolChecksumMap := make(map[string]string)
		dmthinPoolChecksumMap := make(map[string]map[string]string)

		isDmthinSetup, err := IsDMthin()
		if err != nil {
			mError = multierr.Append(mError, err)
			return mError

		}

		//function to calulate md5sum of the given volume in the give pool
		calChecksum := func(wg *sync.WaitGroup, v *volume.Volume, nodeDetail *node.Node, poolUuid string, errCh chan<- error) {
			defer ginkgo.GinkgoRecover()
			defer wg.Done()

			pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			if err != nil {
				errCh <- err
				close(errCh)
				return
			}
			var poolID int32
			for _, pool := range pools {
				if pool.Uuid == poolUuid {
					poolID = pool.ID
					break
				}
			}

			inspectVolume, err := Inst().V.InspectVolume(v.ID)
			if err != nil {
				errCh <- err
				close(errCh)
				return
			}
			log.InfoD("Getting md5sum for volume %s on pool %s in node %s", inspectVolume.Id, poolUuid, nodeDetail.Name)
			//To-Do update the command if set up is dmthin
			cmd := fmt.Sprintf("/opt/pwx/bin/runc exec -t portworx md5sum /var/.px/%d/%s/pxdev", poolID, inspectVolume.Id)

			if isDmthinSetup {

				err = validateDmthinVolumeDataIntegrity(inspectVolume, nodeDetail, poolID, &dmthinPoolChecksumMap)
				if err != nil {
					errCh <- err
					close(errCh)
					return
				}
			} else {
				output, err := Inst().N.RunCommand(*nodeDetail, cmd, node.ConnectionOpts{
					Timeout:         defaultTimeout,
					TimeBeforeRetry: defaultRetryInterval,
					Sudo:            true,
				})
				if err != nil {
					errCh <- err
					close(errCh)
					return
				}
				vChecksum := strings.Split(strings.TrimSpace(output), " ")[0]
				poolChecksumMap[poolUuid] = vChecksum
			}

		}

		for _, v := range appVolumes {
			replicaSets, err := Inst().V.GetReplicaSets(v)
			if err != nil {
				mError = multierr.Append(mError, err)
				return mError

			}

			wGroup := new(sync.WaitGroup)
			errCh := make(chan error)

			for _, rs := range replicaSets {
				poolUuids := rs.PoolUuids
				for _, poolUuid := range poolUuids {
					nodeDetail, err := GetNodeWithGivenPoolID(poolUuid)
					if err != nil {
						mError = multierr.Append(mError, err)
						return mError

					}
					wGroup.Add(1)
					go calChecksum(wGroup, v, nodeDetail, poolUuid, errCh)
				}
			}
			wGroup.Wait()

			for mErr := range errCh {
				mError = multierr.Append(mError, mErr)
			}

			if mError != nil {
				// Combine all errors into one and return
				return mError
			}

			if isDmthinSetup {

				var primaryMap map[string]string
				var primaryNode string

				for n, m := range dmthinPoolChecksumMap {
					if primaryMap == nil {
						primaryNode = n
						primaryMap = m
					} else {
						eq := reflect.DeepEqual(primaryMap, m)

						if !eq {
							err = fmt.Errorf("md5sum of volume [%s] having [%v] on node [%s] is not matching with checksum [%v] on node [%s]", v.ID, primaryMap, primaryNode, m, n)
							mError = multierr.Append(mError, err)
							return mError
						}
						log.InfoD("md5sum of volume [%s] having [%v] on node [%s] is matching with checksum [%v] on node [%s]", v.ID, primaryMap, primaryNode, m, n)

					}
				}
				//clearing the pool after the volume validation
				for k := range dmthinPoolChecksumMap {
					delete(dmthinPoolChecksumMap, k)
				}

			} else {
				var checksum string
				var primaryPool string
				for p, c := range poolChecksumMap {
					if checksum == "" {
						primaryPool = p
						checksum = c
					} else {
						if c != checksum {
							err = fmt.Errorf("md5sum of volume [%s] having [%s] on pool [%s] is not matching with checksum [%s] on pool [%s]", v.ID, checksum, primaryPool, c, p)
							mError = multierr.Append(mError, err)
							return mError
						}
						log.InfoD("md5sum of volume [%s] having [%s] on pool [%s] is matching with checksum [%s] on pool [%s]", v.ID, checksum, primaryPool, c, p)
					}

				}
				//clearing the pool after the volume validation
				for k := range poolChecksumMap {
					delete(poolChecksumMap, k)
				}
			}

		}

	}
	return nil
}

func revertAppScale(ctx *scheduler.Context) error {
	//reverting application scale
	applicationScaleUpMap := make(map[string]int32, len(ctx.App.SpecList))

	applicationScaleMap, err := Inst().S.GetScaleFactorMap(ctx)
	if err != nil {
		return err
	}
	for name, scale := range applicationScaleMap {
		log.InfoD(fmt.Sprintf("scale up app %s to %d", ctx.App.Key, scale))
		applicationScaleUpMap[name] = scale
	}
	err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
	return err
}

func validateDmthinVolumeDataIntegrity(inspectVolume *opsapi.Volume, nodeDetail *node.Node, poolID int32, dmthinPoolChecksumMap *map[string]map[string]string) error {

	volPath := fmt.Sprintf("/dev/pwx%d/%s", poolID, inspectVolume.Id)
	mountPath := fmt.Sprintf("/tmp/%s", inspectVolume.Id)
	mountCmd := fmt.Sprintf("mount %s %s", volPath, mountPath)
	creatDir := fmt.Sprintf("mkdir %s", mountPath)
	umountCmd := fmt.Sprintf("umount %s", mountPath)

	cmdConnectionOpts := node.ConnectionOpts{
		Timeout:         15 * time.Second,
		TimeBeforeRetry: 5 * time.Second,
		Sudo:            true,
	}

	log.Infof("Running command %s on %s", creatDir, nodeDetail.Name)
	_, err := Inst().N.RunCommandWithNoRetry(*nodeDetail, creatDir, cmdConnectionOpts)

	if err != nil {
		return err
	}
	log.Infof("Running command %s on %s", mountCmd, nodeDetail.Name)
	_, err = Inst().N.RunCommandWithNoRetry(*nodeDetail, mountCmd, cmdConnectionOpts)

	if err != nil {
		return err
	}

	defer func(N node.Driver, node node.Node, command string, options node.ConnectionOpts) {
		log.Infof("Running command %s on %s", command, nodeDetail.Name)
		_, err := N.RunCommandWithNoRetry(node, command, options)
		if err != nil {
			log.Errorf(err.Error())
		}
	}(Inst().N, *nodeDetail, umountCmd, cmdConnectionOpts)

	eg := errgroup.Group{}

	eg.Go(func() error {
		md5Cmd := fmt.Sprintf("md5sum %s/*", mountPath)
		log.Infof("Running command %s  on %s", md5Cmd, nodeDetail.Name)
		output, err := Inst().N.RunCommand(*nodeDetail, md5Cmd, node.ConnectionOpts{
			Timeout:         defaultTimeout,
			TimeBeforeRetry: defaultRetryInterval,
			Sudo:            true,
		})

		if err != nil {
			return err
		}
		log.Infof("md5sum of vol %s on node %s : %s", inspectVolume.Id, nodeDetail.Name, output)
		nodeChecksumMap := make(map[string]string)
		for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
			checksumOut := strings.Split(line, "  ")
			if len(checksumOut) == 2 {
				nodeChecksumMap[checksumOut[1]] = checksumOut[0]
			} else {
				return fmt.Errorf("cannot obtain checksum value from node %s, output: %v", nodeDetail.Name, checksumOut)
			}
		}
		(*dmthinPoolChecksumMap)[nodeDetail.Name] = nodeChecksumMap
		return nil

	})

	if err = eg.Wait(); err != nil {
		return err
	}

	return nil

}

// GetContextsOnNode returns the contexts which have volumes attached on the given node.
func GetContextsOnNode(contexts *[]*scheduler.Context, n *node.Node) ([]*scheduler.Context, error) {
	contextsOnNode := make([]*scheduler.Context, 0)
	testName := ginkgo.CurrentSpecReport().FullText()

	if strings.Contains(testName, "Longevity") || strings.Contains(testName, "Trigger") {
		pc, _, _, _ := runtime.Caller(1)
		testName = runtime.FuncForPC(pc).Name()
		sInd := strings.LastIndex(testName, ".")
		if sInd != -1 {
			fnNames := strings.Split(testName, ".")
			for _, fn := range fnNames {
				if strings.Contains(fn, "Trigger") {
					testName = fn
					break
				}
			}
		}

	}

	if !runDataIntegrityValidation(testName) {
		return contextsOnNode, nil
	}

	log.Infof("Getting contexts on node %s", n.Name)
	for _, ctx := range *contexts {
		appVolumes, err := Inst().S.GetVolumes(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range appVolumes {
			// case where volume is attached to the given node
			attachedNode, err := Inst().V.GetNodeForVolume(v, 1*time.Minute, 5*time.Second)
			if err != nil {
				return nil, err
			}
			if attachedNode != nil && (n.VolDriverNodeID == attachedNode.VolDriverNodeID) {
				contextsOnNode = append(contextsOnNode, ctx)
				break
			}
			//case where volume is attached to different node but one of the replicas is the give node
			replicaSets, err := Inst().V.GetReplicaSets(v)
			if err != nil {
				return nil, err
			}

			for _, rn := range replicaSets[0].Nodes {
				if rn == n.VolDriverNodeID {
					contextsOnNode = append(contextsOnNode, ctx)
					break
				}
			}
		}
	}

	return contextsOnNode, nil

}

type CloudDrive struct {
	Type              string            `json:"Type"`
	Size              int               `json:"Size"`
	ID                string            `json:"ID"`
	Path              string            `json:"Path"`
	Iops              int               `json:"Iops"`
	Vpus              int               `json:"Vpus"`
	PXType            string            `json:"PXType"`
	State             string            `json:"State"`
	Labels            map[string]string `json:"labels"`
	AttachOptions     interface{}       `json:"AttachOptions"`
	Provisioner       string            `json:"Provisioner"`
	EncryptionKeyInfo string            `json:"EncryptionKeyInfo"`
}

// GetAllCloudDriveDetailsOnNode returns list of cloud drives present on the node
func GetAllCloudDriveDetailsOnNode(n *node.Node) ([]CloudDrive, error) {

	var drives CloudDrive
	var allCloudDrives []CloudDrive

	// Execute the command and check the alerts of type POOL
	command := fmt.Sprintf("pxctl cd inspect --node %v -j", n.Id)
	out, err := Inst().N.RunCommandWithNoRetry(node.GetStorageNodes()[0], command, node.ConnectionOpts{
		Timeout:         2 * time.Minute,
		TimeBeforeRetry: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	var configData struct {
		Configs map[string]CloudDrive `json:"Configs"`
	}

	err = json.Unmarshal([]byte(out), &configData)
	if err != nil {
		return nil, err
	}

	for _, config := range configData.Configs {
		drives.ID = config.ID
		drives.Size = config.Size
		drives.Type = config.Type
		drives.PXType = config.PXType
		drives.Iops = config.Iops
		drives.Vpus = config.Vpus
		drives.Path = config.Path
		drives.State = config.State
		drives.AttachOptions = config.AttachOptions
		drives.Provisioner = config.Provisioner
		drives.EncryptionKeyInfo = config.EncryptionKeyInfo
		drives.Labels = config.Labels
		allCloudDrives = append(allCloudDrives, drives)
	}
	return allCloudDrives, nil
}

type DriveDetails struct {
	PoolUUID string
	Disks    []string
}

// GetDrivePathFromNode returns drive paths from all the pools on Node
func GetDrivePathFromNode(n *node.Node) ([]DriveDetails, error) {

	allPools := n.StoragePools
	var allDrives []DriveDetails

	for i := 0; i < len(allPools); i++ {
		var drive DriveDetails
		drive.Disks = []string{}
		var drives []string
		cmd := fmt.Sprintf("pxctl sv pool show -j | jq '.datapools[%v].uuid'", i)
		out, err := runCmdOnce(cmd, *n)
		if err != nil {
			return nil, err
		}
		// Split the string into lines based on the newline character ("\n")
		PoolUUID := strings.TrimSpace(out)

		cmd = fmt.Sprintf("pxctl sv pool show -j | jq '.datapools[%v].Info | {Resources, ResourceJournal, ResourceSystemMetadata} | .. | .path? // empty'", i)
		out, err = runCmdOnce(cmd, *n)
		if err != nil {
			return nil, err
		}
		// Split the string into lines based on the newline character ("\n")
		lines := strings.Split(out, "\n")

		// Print each line
		for _, line := range lines {
			// Remove leading and trailing spaces or double quotes if present
			line = strings.TrimSpace(strings.Trim(line, `"`))
			drives = append(drives, line)
		}
		drive.PoolUUID = PoolUUID
		drive.Disks = drives
		allDrives = append(allDrives, drive)
	}

	return allDrives, nil
}

// GetDriveProperties Returns type of the Disk from Path Specified
func GetDriveProperties(path string) (CloudDrive, error) {
	for _, each := range node.GetStorageNodes() {
		cloudDrives, err := GetAllCloudDriveDetailsOnNode(&each)
		if err != nil {
			return CloudDrive{}, err
		}
		for _, eachDrive := range cloudDrives {
			if strings.Contains(path, eachDrive.Path) {
				return eachDrive, nil
			}
		}
	}
	return CloudDrive{}, nil
}

// returns ID and Name of the volume present
type VolMap struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// ListVolumeNamesUsingPxctl , Returns list of volumes present in the cluster
// option will get the output of pxctl volume list, records ID and VolName and returns the struct
func ListVolumeNamesUsingPxctl(n *node.Node) ([]VolMap, error) {
	volList := []VolMap{}
	var vols VolMap

	cmd := "pxctl volume list -j | jq "
	output, err := runCmdGetOutput(cmd, *n)
	if err != nil {
		return nil, err
	}

	// Define a slice of Volume structs
	var vol []struct {
		ID      string `json:"id"`
		Locator struct {
			Name string `json:"name"`
		} `json:"locator"`
	}
	err = json.Unmarshal([]byte(output), &vol)
	if err != nil {
		return nil, err
	}

	for _, volName := range vol {
		vols.ID = volName.ID
		vols.Name = volName.Locator.Name
		volList = append(volList, vols)
	}

	return volList, nil
}

// IsVolumeExits Returns true if volume with ID or Name exists on the cluster
func IsVolumeExits(volName string) bool {
	isVolExist := false
	n := node.GetStorageNodes()
	allVols, err := ListVolumeNamesUsingPxctl(&n[0])
	if err != nil {
		return false
	}

	for _, eachVol := range allVols {
		if eachVol.ID == volName || eachVol.Name == volName {
			isVolExist = true
		}
	}
	return isVolExist
}

func ValidateCRMigration(pods *corev1.PodList, appData *asyncdr.AppData) error {
	pods_created_len := len(pods.Items)
	log.InfoD("Num of Pods on source: %v", pods_created_len)
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	if err != nil {
		return err
	}
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, sourceClusterConfigPath)
	if err != nil {
		return err
	}
	options := scheduler.ScheduleOptions{Namespace: appData.Ns}
	var emptyCtx = &scheduler.Context{
		UID:             "",
		ScheduleOptions: options,
		App: &spec.AppSpec{
			Key:      "",
			SpecList: []interface{}{},
		}}
	log.InfoD("Create cluster pair between source and destination clusters")
	ScheduleValidateClusterPair(emptyCtx, false, true, defaultClusterPairDir, false)
	migName := migrationKey + time.Now().Format("15h03m05s")
	mig, err := asyncdr.CreateMigration(migName, appData.Ns, asyncdr.DefaultClusterPairName, appData.Ns, &includeVolumesFlag, &includeResourcesFlag, &startApplicationsFlag, nil)
	if err != nil {
		return err
	}
	migrationList = append(migrationList, mig)
	err = asyncdr.WaitForMigration(migrationList)
	if err != nil {
		return fmt.Errorf("Migration failed")
	}
	// Sleeping here, as apps deploys one by one, which takes time to collect all pods
	time.Sleep(5 * time.Minute)
	SetDestinationKubeConfig()
	pods_migrated, err := core.Instance().GetPods(appData.Ns, nil)
	if err != nil {
		return err
	}
	pods_migrated_len := len(pods_migrated.Items)
	log.InfoD("Num of Pods on dest: %v", pods_migrated_len)
	if pods_created_len != pods_migrated_len {
		return fmt.Errorf("Pods migration failed as %v pods found on source and %v on destination", pods_created_len, pods_migrated_len)
	}
	destClusterConfigPath, err := GetDestinationClusterConfigPath()
	if err != nil {
		return err
	}
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, destClusterConfigPath)
	if err != nil {
		return fmt.Errorf("CRDs not migrated properly, err: %v", err)
	}
	SetSourceKubeConfig()
	err = asyncdr.DeleteAndWaitForMigrationDeletion(mig.Name, mig.Namespace)
	if err != nil {
		return err
	}
	return nil
}

func DeleteCrAndRepo(appData *asyncdr.AppData, appPath string) error {
	SetDestinationKubeConfig()
	log.InfoD("Starting crd deletion")
	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	SetSourceKubeConfig()
	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	return nil
}

// IsPXRunningOnNode returns true if px is running on the node
func IsPxRunningOnNode(pxNode *node.Node) (bool, error) {
	isPxInstalled, err := Inst().V.IsDriverInstalled(*pxNode)
	if err != nil {
		log.Debugf("Could not get PX status on %s", pxNode.Name)
		return false, err
	}
	if !isPxInstalled {
		return false, nil
	}
	status := Inst().V.IsPxReadyOnNode(*pxNode)
	return status, nil
}

type ProvisionStatus struct {
	NodeUUID      string
	IpAddress     string
	HostName      string
	NodeStatus    string
	PoolID        string
	PoolUUID      string
	PoolStatus    string
	IoPriority    string
	TotalSize     float64
	AvailableSize float64
	UsedSize      float64
}

func tibToGib(tib float64) float64 {
	return tib * 1024
}

func convertToGiB(size string) float64 {
	output := strings.Split(size, " ")
	number, err := strconv.ParseFloat(output[0], 64)
	if err != nil {
		return -1
	}
	if strings.Contains(size, "GiB") {
		return number
	} else if strings.Contains(size, "TiB") {
		return tibToGib(number)
	}
	return -1
}

// GetClusterProvisionStatusOnSpecificNode Returns provision status from the specific node
func GetClusterProvisionStatusOnSpecificNode(n node.Node) ([]ProvisionStatus, error) {
	clusterProvision := []ProvisionStatus{}
	cmd := "pxctl cluster provision-status list"
	output, err := runCmdGetOutput(cmd, n)
	if err != nil {
		log.Infof("running command [%v] failed on Node [%v]", cmd, n.Name)
		return nil, err
	}
	log.InfoD("Output of CMD Output [%v]", output)

	lines := strings.Split(output, "\n")
	pattern := `(\S+)\s+(\S+)\s+\S+\s+(\S+)\s+(\S+)\s+\(\s+(\S+)\s+\)\s+(\S+)\s+(\S+)\s+(\S+\s\S+)+\s+(\S+\s\S+)+\s+(\S+\s\S+)+\s+(\S+\s\S+)+\s+`
	if !strings.Contains(lines[0], "HOSTNAME") {
		// This is needed as in 2.x.y output don't print HOSTNAME
		pattern = `(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+\(\s(\S+)\s\)\s+(\S+)\s+(\S+)\s+(\S+\s\S+)+\s+(\S+\s\S+)+\s+(\S+\s\S+)+\s+(\S+\s\S+)+\s+`
	}
	// Compile the regex pattern
	r := regexp.MustCompile(pattern)

	for _, eachLine := range lines {
		var provisionStatus ProvisionStatus
		if !strings.Contains(eachLine, "NODE") {
			matches := r.FindStringSubmatch(eachLine)
			if len(matches) > 0 {
				provisionStatus.NodeUUID = matches[1]
				provisionStatus.IpAddress = matches[2]
				provisionStatus.NodeStatus = matches[3]
				provisionStatus.PoolID = matches[4]
				provisionStatus.PoolUUID = matches[5]
				provisionStatus.PoolStatus = matches[6]
				provisionStatus.IoPriority = matches[7]
				provisionStatus.TotalSize = convertToGiB(matches[8])
				provisionStatus.AvailableSize = convertToGiB(matches[9])
				provisionStatus.UsedSize = convertToGiB(matches[10])
				clusterProvision = append(clusterProvision, provisionStatus)
			}
		}
	}
	log.Infof("Cluster provision status [%v]", clusterProvision)
	return clusterProvision, nil
}

// GetClusterProvisionStatus  returns details of cluster provision status
func GetClusterProvisionStatus() ([]ProvisionStatus, error) {
	// Using Node which is up and running
	var selectedNode []node.Node
	for _, eachNode := range node.GetStorageDriverNodes() {
		status, err := IsPxRunningOnNode(&eachNode)
		if err != nil {
			log.InfoD("Px is not running on the Node.. searching for other node")
			continue
		}
		if status {
			selectedNode = append(selectedNode, eachNode)
			break
		}
	}
	if len(selectedNode) == 0 {
		return nil, fmt.Errorf("No Valid node exists")
	}

	// Select Random Volumes for pool Expand
	randomIndex := rand.Intn(len(selectedNode))
	randomNode := selectedNode[randomIndex]

	clusterProvision, err := GetClusterProvisionStatusOnSpecificNode(randomNode)
	if err != nil {
		return nil, err
	}

	log.Infof("Cluster provision status [%v]", clusterProvision)
	return clusterProvision, nil
}

// GetPoolTotalSize Return total Pool size
func GetPoolTotalSize(poolUUID string) (float64, error) {
	provision, err := GetClusterProvisionStatus()
	if err != nil {
		return -1, err
	}
	for _, eachProvision := range provision {
		if eachProvision.PoolUUID == poolUUID {
			return eachProvision.TotalSize, nil
		}
	}
	return -1, err
}

// GetPoolAvailableSize Returns available pool Size
func GetPoolAvailableSize(poolUUID string) (float64, error) {
	provision, err := GetClusterProvisionStatus()
	if err != nil {
		return -1, err
	}
	for _, eachProvision := range provision {
		if eachProvision.PoolUUID == poolUUID {
			return eachProvision.AvailableSize, nil
		}
	}
	return -1, err
}

// GetAllPoolsOnNode Returns list of all pool uuids present on specific Node
func GetAllPoolsOnNode(nodeUuid string) ([]string, error) {
	var poolDetails []string
	provision, err := GetClusterProvisionStatus()
	if err != nil {
		return nil, err
	}
	for _, eachProvision := range provision {
		if eachProvision.NodeUUID == nodeUuid {
			poolDetails = append(poolDetails, eachProvision.PoolUUID)
		}
	}
	return poolDetails, nil
}

// Set default provider as aws
func GetClusterProvider() string {
	clusterProvider = os.Getenv("CLUSTER_PROVIDER")
	return clusterProvider
}

// Get Gke Secret
func GetGkeSecret() (string, error) {
	cm, err := core.Instance().GetConfigMap("cloud-config", "default")
	if err != nil {
		return "", err
	}
	return cm.Data["cloud-json"], nil
}

// WaitForSnapShotToReady returns snapshot status after waiting till snapshot gets to Ready state
func WaitForSnapShotToReady(snapshotScheduleName, snapshotName, appNamespace string) (*storkapi.ScheduledVolumeSnapshotStatus, error) {

	var schedVolumeSnapstatus *storkapi.ScheduledVolumeSnapshotStatus
	delVol := func() (interface{}, bool, error) {
		resp, err := storkops.Instance().GetSnapshotSchedule(snapshotScheduleName, appNamespace)
		if err != nil {
			return "", false, err
		}
	outer:
		for _, snapshotStatuses := range resp.Status.Items {
			for _, snapStatus := range snapshotStatuses {
				if snapStatus.Name == snapshotName {
					schedVolumeSnapstatus = snapStatus
					break outer
				}
			}
		}
		if schedVolumeSnapstatus == nil {
			return nil, false, fmt.Errorf("no scheduled volume snapshot status found with name [%s] with snapshot schedule [%s] in namespace [%s]", snapshotName, snapshotScheduleName, appNamespace)
		}

		if schedVolumeSnapstatus.Status == snapv1.VolumeSnapshotConditionError {
			return nil, false, fmt.Errorf("snapshot: %s failed. status: %v", snapshotName, schedVolumeSnapstatus.Status)
		}

		if schedVolumeSnapstatus.Status == snapv1.VolumeSnapshotConditionPending {
			return nil, true, fmt.Errorf("scheduled volume snapshot status found with name [%s] has status [%v]", snapshotName, schedVolumeSnapstatus.Status)
		}

		return nil, false, nil
	}
	_, err := task.DoRetryWithTimeout(delVol, time.Duration(3)*appReadinessTimeout, 30*time.Second)

	return schedVolumeSnapstatus, err
}

// IsCloudCredPresent checks whether the Cloud Cred is present or not
func IsCloudCredPresent(cloudCredName string, ctx context1.Context, orgID string) (bool, error) {
	cloudCredEnumerateRequest := &api.CloudCredentialEnumerateRequest{
		OrgId:          orgID,
		IncludeSecrets: false,
	}
	cloudCredObjs, err := Inst().Backup.EnumerateCloudCredentialByUser(ctx, cloudCredEnumerateRequest)
	if err != nil {
		return false, err
	}
	for _, cloudCredObj := range cloudCredObjs.GetCloudCredentials() {
		if cloudCredObj.GetName() == cloudCredName {
			log.Infof("Cloud Credential [%s] is present", cloudCredName)
			return true, nil
		}
	}
	return false, nil
}

// GetAllPoolsPresent returns list of all pools present in the cluster
func GetAllPoolsPresent() ([]string, error) {
	var poolDetails []string
	provision, err := GetClusterProvisionStatus()
	if err != nil {
		return nil, err
	}
	for _, eachProvision := range provision {
		poolDetails = append(poolDetails, eachProvision.PoolUUID)
	}
	return poolDetails, nil
}

// AddCloudCredentialOwnership adds new ownership to the existing CloudCredential object.
func AddCloudCredentialOwnership(cloudCredentialName string, cloudCredentialUid string, userNames []string, groups []string, accessType OwnershipAccessType, publicAccess OwnershipAccessType, ctx context1.Context, orgID string) error {
	backupDriver := Inst().Backup
	userIDs := make([]string, 0)
	groupIDs := make([]string, 0)
	for _, userName := range userNames {
		userID, err := backup.FetchIDOfUser(userName)
		if err != nil {
			return err
		}
		userIDs = append(userIDs, userID)
	}

	for _, group := range groups {
		groupID, err := backup.FetchIDOfGroup(group)
		if err != nil {
			return err
		}
		groupIDs = append(groupIDs, groupID)
	}

	userCloudCredentialOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)

	for _, userID := range userIDs {
		userCloudCredentialOwnershipAccessConfig := &api.Ownership_AccessConfig{
			Id:     userID,
			Access: api.Ownership_AccessType(accessType),
		}
		userCloudCredentialOwnershipAccessConfigs = append(userCloudCredentialOwnershipAccessConfigs, userCloudCredentialOwnershipAccessConfig)
	}

	groupCloudCredentialOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)

	for _, groupID := range groupIDs {
		groupCloudCredentialOwnershipAccessConfig := &api.Ownership_AccessConfig{
			Id:     groupID,
			Access: api.Ownership_AccessType(accessType),
		}
		groupCloudCredentialOwnershipAccessConfigs = append(groupCloudCredentialOwnershipAccessConfigs, groupCloudCredentialOwnershipAccessConfig)
	}

	cloudCredentialInspectRequest := &api.CloudCredentialInspectRequest{
		OrgId: orgID,
		Name:  cloudCredentialName,
		Uid:   cloudCredentialUid,
	}
	cloudCredentialInspectResp, err := Inst().Backup.InspectCloudCredential(ctx, cloudCredentialInspectRequest)
	if err != nil {
		return err
	}
	currentGroupsConfigs := cloudCredentialInspectResp.CloudCredential.GetOwnership().GetGroups()
	groupCloudCredentialOwnershipAccessConfigs = append(groupCloudCredentialOwnershipAccessConfigs, currentGroupsConfigs...)
	currentUsersConfigs := cloudCredentialInspectResp.CloudCredential.GetOwnership().GetCollaborators()
	userCloudCredentialOwnershipAccessConfigs = append(userCloudCredentialOwnershipAccessConfigs, currentUsersConfigs...)

	cloudCredentialOwnershipUpdateReq := &api.CloudCredentialOwnershipUpdateRequest{
		OrgId: orgID,
		Name:  cloudCredentialName,
		Ownership: &api.Ownership{
			Groups:        groupCloudCredentialOwnershipAccessConfigs,
			Collaborators: userCloudCredentialOwnershipAccessConfigs,
			Public: &api.Ownership_PublicAccessControl{
				Type: api.Ownership_AccessType(publicAccess),
			},
		},
		Uid: cloudCredentialUid,
	}

	_, err = backupDriver.UpdateOwnershipCloudCredential(ctx, cloudCredentialOwnershipUpdateReq)
	if err != nil {
		return fmt.Errorf("failed to update CloudCredential ownership : %v", err)
	}
	return nil
}

// GenerateS3BucketPolicy Generates an S3 bucket policy based on encryption policy provided
func GenerateS3BucketPolicy(sid string, encryptionPolicy string, bucketName string, enforceServerSideEncryption ...bool) (string, error) {

	encryptionPolicyValues := strings.Split(encryptionPolicy, "=")
	if len(encryptionPolicyValues) < 2 {
		return "", fmt.Errorf("failed to generate policy for s3,check for proper length of encryptionPolicy : %v", encryptionPolicy)
	}
	var enforceSse string
	if len(enforceServerSideEncryption) == 0 {
		// If enableServerSideEncryption is not passed , default it to true
		enforceSse = "true"
	}
	policy := `{
	   "Version": "2012-10-17",
	   "Statement": [
		  {
			 "Sid": "%s",
			 "Effect": "Deny",
			 "Principal": "*",
			 "Action": "s3:PutObject",
			 "Resource": "arn:aws:s3:::%s/*",
			 "Condition": {
				"StringNotEquals": {
				   "%s":"%s"
				}
			 }
		  },
		  {
			"Sid": "DenyUnencryptedObjectUploads",
			"Effect": "Deny",
			"Principal": "*",
			"Action": "s3:PutObject",
			"Resource": "arn:aws:s3:::%s/*",
			"Condition": {
				"Null": {
					"%s":"%s"
				}
			}
		  }	
	   ]
	}`

	// Replace the placeholders in the policy with the values passed to the function.
	policy = fmt.Sprintf(policy, sid, bucketName, encryptionPolicyValues[0], encryptionPolicyValues[1], bucketName, encryptionPolicyValues[0], enforceSse)

	return policy, nil
}

// GetSseS3EncryptionType fetches SSE type for S3 bucket from the environment variable
func GetSseS3EncryptionType() (api.S3Config_Sse, error) {
	var sseType api.S3Config_Sse
	s3SseTypeEnv := os.Getenv("S3_SSE_TYPE")
	if s3SseTypeEnv != "" {
		sseDetails, err := s3utils.GetS3SSEDetailsFromEnv()
		if err != nil {
			return sseType, err
		}
		switch sseDetails.SseType {
		case s3utils.SseS3:
			sseType = api.S3Config_SSE_S3
		default:
			return sseType, fmt.Errorf("failed to sse s3 encryption type not valid: [%v]", sseDetails.SseType)
		}
	} else {
		sseType = api.S3Config_Invalid
	}
	return sseType, nil
}

func UpdateDriverVariables(envVar, runTimeOpts map[string]string) error {
	// Get PX StorageCluster
	clusterSpec, err := Inst().V.GetDriver()
	if err != nil {
		return err
	}

	var newEnvVarList []corev1.EnvVar

	//Update environment variables in the spec
	if envVar != nil && len(envVar) > 0 {
		for _, env := range clusterSpec.Spec.Env {
			newEnvVarList = append(newEnvVarList, env)
		}

		for k, v := range envVar {
			newEnvVarList = append(newEnvVarList, corev1.EnvVar{Name: k, Value: v})
		}
		clusterSpec.Spec.Env = newEnvVarList
	}

	//Update RunTimeOpts in the spec
	if runTimeOpts != nil && len(runTimeOpts) > 0 {
		if clusterSpec.Spec.RuntimeOpts == nil {
			clusterSpec.Spec.RuntimeOpts = make(map[string]string)
		}
		for k, v := range runTimeOpts {
			clusterSpec.Spec.RuntimeOpts[k] = v
		}
	}

	pxOperator := operator.Instance()
	_, err = pxOperator.UpdateStorageCluster(clusterSpec)
	if err != nil {
		return err
	}

	log.InfoD("Deleting PX pods for reloading the runtime Opts")
	err = DeletePXPods(clusterSpec.Namespace)
	if err != nil {
		return err
	}
	_, err = optest.ValidateStorageClusterIsOnline(clusterSpec, 10*time.Minute, 3*time.Minute)
	if err != nil {
		return err
	}

	log.InfoD("Waiting for PX Nodes to be up")
	for _, n := range node.GetStorageDriverNodes() {
		if err := Inst().V.WaitDriverUpOnNode(n, 15*time.Minute); err != nil {
			return err
		}
	}

	return nil
}

// DeletePXPods delete px pods
func DeletePXPods(nameSpace string) error {
	pxLabel := make(map[string]string)
	pxLabel["name"] = "portworx"
	if err := core.Instance().DeletePodsByLabels(nameSpace, pxLabel, podDestroyTimeout); err != nil {
		return err
	}
	return nil
}

func GetKubevirtVersionToUpgrade() string {
	kubevirtVersion, present := os.LookupEnv("KUBEVIRT_UPGRADE_VERSION")
	if present && kubevirtVersion != "" {
		return kubevirtVersion
	}
	return LatestKubevirtVersion
}

// GetRandomSubset generates a random subset of elements from a given list.
func GetRandomSubset(elements []string, subsetSize int) ([]string, error) {
	if subsetSize > len(elements) {
		return nil, fmt.Errorf("subset size exceeds the length of the input list")
	}

	shuffledElements := make([]string, len(elements))
	copy(shuffledElements, elements)
	rand.Shuffle(len(shuffledElements), func(i, j int) {
		shuffledElements[i], shuffledElements[j] = shuffledElements[j], shuffledElements[i]
	})

	return shuffledElements[:subsetSize], nil
}

// DeleteAllNamespacesCreatedByTestCase deletes all the namespaces created for the test case
func DeleteAllNamespacesCreatedByTestCase() error {

	// Get all the namespaces on the cluster
	k8sCore := core.Instance()
	allNamespaces, err := k8sCore.ListNamespaces(make(map[string]string))
	if err != nil {
		return fmt.Errorf("error in listing namespaces. Err: %v", err.Error())
	}

	// Iterate and remove all namespaces
	for _, ns := range allNamespaces.Items {
		if strings.Contains(ns.Name, Inst().InstanceID) {
			log.Infof("Deleting namespace [%s]", ns.Name)
			err = k8sCore.DeleteNamespace(ns.Name)
			if err != nil {
				// Not returning anything as it's the best case effort
				log.InfoD("Error in deleting namespace [%s]. Err: %v", ns.Name, err.Error())
			}
		}
	}
	return nil
}

func GetNodeForGivenVolumeName(volName string) (*node.Node, error) {

	t := func() (interface{}, bool, error) {
		pxVol, err := Inst().V.InspectVolume(volName)
		if err != nil {
			log.Warnf("Failed to inspect volume [%s], Err: %v", volName, err)
			return nil, false, err
		}

		for _, n := range node.GetStorageDriverNodes() {
			ok, err := Inst().V.IsVolumeAttachedOnNode(pxVol, n)
			if err != nil {
				return nil, false, err
			}
			if ok {
				return &n, false, err
			}
		}

		// Snapshots may not be attached to a node
		if pxVol.Source.Parent != "" {
			return nil, false, nil
		}

		return nil, true, fmt.Errorf("volume [%s] is not attached on any node", volName)
	}

	n, err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second)
	if err != nil {
		return nil, err
	}

	if n != nil {
		attachedNode := n.(*node.Node)
		return attachedNode, nil
	}

	return nil, fmt.Errorf("no attached node found for vol [%s]", volName)
}

// GetProcessPID returns the PID of KVDB master node
func GetProcessPID(memberNode node.Node, processName string) (string, error) {
	var processPid string
	command := fmt.Sprintf("ps -ef | grep -i %s", processName)
	out, err := Inst().N.RunCommand(memberNode, command, node.ConnectionOpts{
		Timeout:         20 * time.Second,
		TimeBeforeRetry: 5 * time.Second,
		Sudo:            true,
	})
	if err != nil {
		return "", err
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		if strings.Contains(line, fmt.Sprintf("/usr/local/bin/%s", processName)) && !strings.Contains(line, "grep") {
			fields := strings.Fields(line)
			processPid = fields[1]
			break
		}
	}
	return processPid, err
}

// KillPxExecUsingPid return error in case of command failure
func KillPxExecUsingPid(memberNode node.Node) error {
	pid, err := GetProcessPID(memberNode, "pxexec")
	if err != nil {
		return err
	}
	if pid == "" {
		log.InfoD("Procrss with PID doesnot exists !! ")
		return nil
	}
	command := fmt.Sprintf("kill -9 %s", pid)
	log.InfoD("killing PID using command [%s]", command)
	err = runCmd(command, memberNode)
	if err != nil {
		return err
	}
	return nil
}

// KillPxStorageUsingPid return error in case of command failure
func KillPxStorageUsingPid(memberNode node.Node) error {
	nodes := []node.Node{}
	nodes = append(nodes, memberNode)
	err := Inst().V.StopDriver(nodes, true, nil)
	if err != nil {
		return err
	}
	return nil
}

// GetAllPodsInNameSpace Returns list of pods running in the namespace
func GetAllPodsInNameSpace(nameSpace string) ([]corev1.Pod, error) {
	pods, err := k8sCore.GetPods(nameSpace, nil)
	if err != nil {
		return nil, err
	}
	return pods.Items, nil
}

func installGrafana(namespace string) {
	enableGrafana := false

	// If true, enable grafana on the set up
	if enableGrafanaVar := os.Getenv("ENABLE_GRAFANA"); enableGrafanaVar != "" {
		enableGrafana, _ = strconv.ParseBool(enableGrafanaVar)
	}
	if enableGrafana {
		grafanaScript := "/torpedo/deployments/setup_grafana.sh"
		if _, err := os.Stat(grafanaScript); errors.Is(err, os.ErrNotExist) {
			log.Warnf("Cannot find grafana set up script in path %s", grafanaScript)
			return
		}

		// Change permission on file to be able to execute
		if err := osutils.Chmod("+x", grafanaScript); err != nil {
			log.Warnf("error changing permission for script [%s], err: %v", grafanaScript, err)
			return
		}

		output, stErr, err := osutils.ExecTorpedoShell(fmt.Sprintf("%s %s", grafanaScript, namespace))
		if err != nil {
			log.Warnf("error running script [%s], err: %v", grafanaScript, err)
			return
		}
		if stErr != "" {
			log.Warnf("got standard error while running script [%s], err: %s", grafanaScript, stErr)
			return
		}
		log.Infof(output)
	}

}

func SetupProxyServer(n node.Node) error {
	createDirCommand := "mkdir -p /exports/testnfsexportdir"
	output, err := Inst().N.RunCommandWithNoRetry(n, createDirCommand, node.ConnectionOpts{
		Sudo:            true,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	})
	if err != nil {
		return err
	}
	log.Infof(output)

	addVersionCmd := "echo -e \"MOUNTD_NFS_V4=\"yes\"\nRPCNFSDARGS=\"-N 2 -N 4\"\" >> /etc/sysconfig/nfs"
	output, err = Inst().N.RunCommandWithNoRetry(n, addVersionCmd, node.ConnectionOpts{
		Sudo:            true,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	})
	if err != nil {
		return err
	}
	log.Infof(output)

	updateExportsCmd := "echo \"/exports/testnfsexportdir *(rw,sync,no_root_squash)\" > /etc/exports"
	output, err = Inst().N.RunCommandWithNoRetry(n, updateExportsCmd, node.ConnectionOpts{
		Sudo:            true,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	})
	if err != nil {
		return err
	}
	log.Infof(output)

	checkExportfsCmd := "which exportfs"
	output, err = Inst().N.RunCommandWithNoRetry(n, checkExportfsCmd, node.ConnectionOpts{
		Sudo:            true,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	})
	if err != nil || output == "" {
		log.Warnf("The command exportfs not found")

		var installNfsUtilsCmd string
		checkDistroCmd := "source /etc/os-release && echo $ID"
		output, err = Inst().N.RunCommandWithNoRetry(n, checkDistroCmd, node.ConnectionOpts{
			Sudo:            true,
			TimeBeforeRetry: defaultRetryInterval,
			Timeout:         defaultTimeout,
		})
		if err != nil {
			return err
		}
		log.Infof("The Linux distribution is %s", output)

		switch strings.TrimSpace(output) {
		case "ubuntu", "debian":
			log.Infof("Installing nfs-common")
			installNfsUtilsCmd = "apt-get update && apt-get install -y nfs-common"
		case "centos", "rhel", "fedora":
			log.Infof("Installing nfs-utils")
			installNfsUtilsCmd = "yum install -y nfs-utils"
		default:
			return fmt.Errorf("unsupported Linux distribution")
		}

		output, err = Inst().N.RunCommandWithNoRetry(n, installNfsUtilsCmd, node.ConnectionOpts{
			Sudo:            true,
			TimeBeforeRetry: defaultRetryInterval,
			Timeout:         defaultTimeout,
		})
		if err != nil {
			return err
		}
		log.Infof(output)
	} else {
		log.Infof(output)
	}

	exportCmd := "exportfs -a"
	output, err = Inst().N.RunCommandWithNoRetry(n, exportCmd, node.ConnectionOpts{
		Sudo:            true,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	})
	if err != nil {
		return err
	}
	log.Infof(output)

	enableNfsServerCmd := "systemctl enable nfs-server"
	output, err = Inst().N.RunCommandWithNoRetry(n, enableNfsServerCmd, node.ConnectionOpts{
		Sudo:            true,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	})
	if err != nil {
		return err
	}
	log.Infof(output)

	startNfsServerCmd := "systemctl restart nfs-server"
	output, err = Inst().N.RunCommandWithNoRetry(n, startNfsServerCmd, node.ConnectionOpts{
		Sudo:            true,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	})
	if err != nil {
		return err
	}
	log.Infof(output)

	return nil
}

func CreateNFSProxyStorageClass(scName, nfsServer, mountPath string) error {
	params := make(map[string]string)
	params["repl"] = "1"
	params["io_profile"] = "none"
	params["proxy_endpoint"] = fmt.Sprintf("nfs://%s", nfsServer)
	params["proxy_nfs_exportpath"] = fmt.Sprintf("%s", mountPath)
	params["mount_options"] = "vers=4.0"
	v1obj := metav1.ObjectMeta{
		Name: scName,
	}
	reclaimPolicyDelete := corev1.PersistentVolumeReclaimDelete
	bindMode := storageapi.VolumeBindingImmediate
	allowWxpansion := true
	scObj := storageapi.StorageClass{
		ObjectMeta:           v1obj,
		Provisioner:          "kubernetes.io/portworx-volume",
		Parameters:           params,
		ReclaimPolicy:        &reclaimPolicyDelete,
		VolumeBindingMode:    &bindMode,
		AllowVolumeExpansion: &allowWxpansion,
	}

	k8sStorage := k8sStorage.Instance()
	_, err := k8sStorage.CreateStorageClass(&scObj)
	return err
}

// PrintK8sClusterInfo prints info about K8s cluster nodes
func PrintK8sClusterInfo() {
	log.Info("Get cluster info..")
	t := func() (interface{}, bool, error) {
		nodeList, err := core.Instance().GetNodes()
		if err != nil {
			return "", true, fmt.Errorf("failed to get nodes, Err %v", err)
		}
		if len(nodeList.Items) > 0 {
			for _, n := range nodeList.Items {
				nodeType := "Worker"
				if core.Instance().IsNodeMaster(n) {
					nodeType = "Master"
				}
				log.Infof(
					"Node Name: %s, Node Type: %s, Kernel Version: %s, Kubernetes Version: %s, OS: %s, Container Runtime: %s",
					n.Name, nodeType,
					n.Status.NodeInfo.KernelVersion, n.Status.NodeInfo.KubeletVersion, n.Status.NodeInfo.OSImage,
					n.Status.NodeInfo.ContainerRuntimeVersion)
			}
			return "", false, nil
		}
		return "", false, fmt.Errorf("no nodes were found in the cluster")
	}
	if _, err := task.DoRetryWithTimeout(t, 1*time.Minute, 5*time.Second); err != nil {
		log.Warnf("failed to get k8s cluster info, Err: %v", err)
	}
}

func CreatePXCloudCredential() error {
	/*
		Creating a cloud credential for cloudsnap wit the given params
		Deleting the existing cred if exists so that we can use same creds to delete the s3 bucket once test is completed.
	*/
	id, secret, endpoint, s3Region, disableSSl, err := getCreateCredParams()

	if err != nil {
		return err
	}

	n := node.GetStorageDriverNodes()[0]
	uuidCmd := "cred list -j | grep uuid"

	output, err := Inst().V.GetPxctlCmdOutputConnectionOpts(n, uuidCmd, node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	}, false)
	if err != nil {
		log.Warnf("No creds found, creating new cred, Err: %v", err)
	}

	if output != "" {
		log.Infof("Cloud Cred exists [%s]", output)
		log.Warnf("Deleting existing cred and creating new cred with given params")
		credUUID := strings.Split(output, ":")[1]
		credUUID = strings.ReplaceAll(strings.TrimSpace(credUUID), "\"", "")
		credDeleteCmd := fmt.Sprintf("cred delete %s", credUUID)
		output, err = Inst().V.GetPxctlCmdOutputConnectionOpts(n, credDeleteCmd, node.ConnectionOpts{
			IgnoreError:     false,
			TimeBeforeRetry: defaultRetryInterval,
			Timeout:         defaultTimeout,
		}, false)

		if err != nil {
			err = fmt.Errorf("error deleting existing cred [%s], cause: %v", credUUID, err)
			return err
		}

		log.Infof("Deleted exising cred [%s]", output)
	}

	cloudCredName := "px-cloud-cred"

	credCreateCmd := fmt.Sprintf("cred create %s --provider s3 --s3-access-key %s --s3-secret-key %s --s3-endpoint %s --s3-region %s", cloudCredName, id, secret, endpoint, s3Region)

	if disableSSl {
		credCreateCmd = fmt.Sprintf("%s --s3-disable-ssl", credCreateCmd)
	}

	log.Infof("Running command [%s]", credCreateCmd)

	// Execute the command and check get rebalance status
	output, err = Inst().V.GetPxctlCmdOutputConnectionOpts(node.GetStorageNodes()[0], credCreateCmd, node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	}, false)

	if err != nil {
		err = fmt.Errorf("error creating cloud cred, cause: %v", err)
		return err
	}

	log.Infof(output)

	return nil

}

func GetCloudsnapBucketName(contexts []*scheduler.Context) (string, error) {

	var bucketName string
	//Stopping cloudnsnaps before bucket deletion
	for _, ctx := range contexts {
		if strings.Contains(ctx.App.Key, "cloudsnap") {
			if bucketName == "" {
				vols, err := Inst().S.GetVolumeParameters(ctx)
				if err != nil {
					err = fmt.Errorf("error getting volume params for %s, cause: %v", ctx.App.Key, err)
					return "", err
				}
				for vol, params := range vols {
					csBksps, err := Inst().V.GetCloudsnaps(vol, params)
					if err != nil {
						err = fmt.Errorf("error getting cloud snaps for %s, cause: %v", vol, err)
						return "", err
					}
					for _, csBksp := range csBksps {
						bkid := csBksp.GetId()
						bucketName = strings.Split(bkid, "/")[0]
						break
					}
				}
				log.Infof("Got Bucket Name [%s]", bucketName)
			}
			vols, err := Inst().S.GetVolumes(ctx)
			if err != nil {
				return "", err
			}
			for _, vol := range vols {
				appVol, err := Inst().V.InspectVolume(vol.ID)
				if err != nil {
					return "", err
				}
				err = suspendCloudsnapBackup(appVol.Id)
				if err != nil {
					return "", err
				}
			}
		}
	}
	return bucketName, nil
}

func DeleteCloudSnapBucket(bucketName string) error {

	if bucketName != "" {
		id, secret, endpoint, s3Region, _, err := getCreateCredParams()
		if err != nil {
			return err
		}
		var sess *session.Session
		if strings.Contains(endpoint, "minio") {

			sess, err = session.NewSessionWithOptions(session.Options{
				Config: aws.Config{
					Endpoint:         aws.String(endpoint),
					Region:           aws.String(s3Region),
					Credentials:      credentials.NewStaticCredentials(id, secret, ""),
					S3ForcePathStyle: aws.Bool(true),
				},
			})
			if err != nil {
				return fmt.Errorf("failed to initialize new session: %v", err)
			}
		}

		if strings.Contains(endpoint, "amazonaws") {
			sess, err = session.NewSessionWithOptions(session.Options{
				Config: aws.Config{
					Region:      aws.String(s3Region),
					Credentials: credentials.NewStaticCredentials(id, secret, ""),
				},
			})
			if err != nil {
				return fmt.Errorf("failed to initialize new session: %v", err)
			}
		}

		if sess == nil {
			return fmt.Errorf("failed to initialize new session using endpoint [%s], Cause: %v", endpoint, err)
		}

		client := s3.New(sess)
		err = deleteAndValidateBucketDeletion(client, bucketName)
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteAndValidateBucketDeletion(client *s3.S3, bucketName string) error {
	// Delete all objects and versions in the bucket
	log.Debugf("Deleting bucket [%s]", bucketName)
	time.Sleep(5 * time.Minute)
	err := client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		// Iterate through the objects in the bucket and delete them
		var objects []*s3.ObjectIdentifier

		for _, obj := range page.Contents {
			objects = append(objects, &s3.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		_, err := client.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &s3.Delete{
				Objects: objects,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			log.Warnf("failed to delete objects in bucket: %v", err)
			return false
		}

		return true
	})
	if err != nil {
		return fmt.Errorf("failed to delete objects in bucket: %v", err)
	}

	// List the objects in the bucket
	listObjectsInput := s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	listObjectsOutput, err := client.ListObjectsV2(&listObjectsInput)
	if err != nil {
		return err
	}
	if len(listObjectsOutput.Contents) == 0 {
		log.Debugf("Bucket [%s] is empty", bucketName)
	} else {
		// Delete the objects
		deleteObjectsInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &s3.Delete{
				Objects: make([]*s3.ObjectIdentifier, len(listObjectsOutput.Contents)),
				Quiet:   aws.Bool(true),
			},
		}

		for i, object := range listObjectsOutput.Contents {
			deleteObjectsInput.Delete.Objects[i] = &s3.ObjectIdentifier{
				Key: aws.String(*object.Key),
			}
		}

		_, err = client.DeleteObjects(deleteObjectsInput)
		if err != nil {
			return err
		}
	}

	// Delete the bucket
	_, err = client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		var aerr awserr.Error
		if errors.As(err, &aerr) {
			if aerr.Code() == s3.ErrCodeNoSuchBucket {
				log.Infof("Bucket: %v doesn't exist.!!", bucketName)
				return nil
			}
			return fmt.Errorf("couldn't delete bucket: %v", err)
		}
	}

	log.Infof("Successfully deleted the bucket: %v", bucketName)
	return nil
}

func suspendCloudsnapBackup(volId string) error {

	isBackupActive, err := IsCloudsnapBackupActiveOnVolume(volId)
	if err != nil {
		return fmt.Errorf("error checking backup status  for volume  [%s],Err: %v ", volId, err)
	}
	log.Infof("Backup status for vol [%s]: %v", volId, isBackupActive)

	if isBackupActive {
		scheduleOfVol, err := GetVolumeSnapShotScheduleOfVol(volId)
		if err != nil {
			return fmt.Errorf("error getting volumes snapshot schedule for volume [%s],Err: %v ", volId, err)
		}

		if !*scheduleOfVol.Spec.Suspend {
			log.Infof("Snapshot schedule is not suspended. Suspending it")
			makeSuspend := true
			scheduleOfVol.Spec.Suspend = &makeSuspend
			_, err := storkops.Instance().UpdateSnapshotSchedule(scheduleOfVol)
			if err != nil {
				return fmt.Errorf("error suspending volumes snapshot schedule for volume  [%s],Err: %v ", volId, err)
			}

		}
	}
	return nil
}

func getCreateCredParams() (id, secret, endpoint, s3Region string, disableSSLBool bool, err error) {

	id = os.Getenv("S3_AWS_ACCESS_KEY_ID")
	if id == "" {
		id = os.Getenv("AWS_ACCESS_KEY_ID")
	}

	if id == "" {
		err = fmt.Errorf("S3_AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID Environment variable is not provided")
		return

	}

	secret = os.Getenv("S3_AWS_SECRET_ACCESS_KEY")
	if secret == "" {
		secret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	if secret == "" {
		err = fmt.Errorf("S3_AWS_SECRET_ACCESS_KEY or AWS_SECRET_ACCESS_KEY Environment variable is not provided")
		return
	}

	endpoint = os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		err = fmt.Errorf("S3_ENDPOINT Environment variable is not provided")
		return
	}

	s3Region = os.Getenv("S3_REGION")
	if s3Region == "" {
		err = fmt.Errorf("S3_REGION Environment variable is not provided")
		return
	}

	disableSSL := os.Getenv("S3_DISABLE_SSL")
	disableSSLBool = false

	if len(disableSSL) > 0 {
		disableSSLBool, err = strconv.ParseBool(disableSSL)
		if err != nil {
			err = fmt.Errorf("S3_DISABLE_SSL=%s is not a valid boolean value,err: %v", disableSSL, err)
			return
		}
	}

	return
}

// ExportSourceKubeConfig changes the KUBECONFIG environment variable to the source cluster config path
func ExportSourceKubeConfig() error {
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	if err != nil {
		return err
	}
	err = os.Unsetenv("KUBECONFIG")
	if err != nil {
		return err
	}
	return os.Setenv("KUBECONFIG", sourceClusterConfigPath)
}

// ExportDestinationKubeConfig changes the KUBECONFIG environment variable to the destination cluster config path
func ExportDestinationKubeConfig() error {
	DestinationClusterConfigPath, err := GetDestinationClusterConfigPath()
	if err != nil {
		return err
	}

	err = os.Unsetenv("KUBECONFIG")
	if err != nil {
		return err
	}
	return os.Setenv("KUBECONFIG", DestinationClusterConfigPath)
}

// SwitchBothKubeConfigANDContext switches both KUBECONFIG and context to the given cluster
func SwitchBothKubeConfigANDContext(cluster string) error {
	if cluster == "source" {
		err := ExportSourceKubeConfig()
		if err != nil {
			return err
		}
		err = SetSourceKubeConfig()
		if err != nil {
			return err
		}
	} else if cluster == "destination" {
		err := ExportDestinationKubeConfig()
		if err != nil {
			return err
		}
		err = SetDestinationKubeConfig()
		if err != nil {
			return err
		}
	}
	return nil
}

// DoPDBValidation continuously validates the Pod Disruption Budget against
// cluster upgrades, appending errors to mError, until a stop signal is received.
func DoPDBValidation(stopSignal <-chan struct{}, mError *error) {
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
				err := fmt.Errorf("cluster is not parallely upgraded")
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

// GetVolumesOnNode returns the volume IDs which have repl on a give node
func GetVolumesOnNode(nodeId string) ([]string, error) {
	var volumes []string

	pvs, err := k8sCore.GetPersistentVolumes()
	if err != nil {
		return nil, err
	}

	for _, vol := range pvs.Items {
		volDetails, err := Inst().V.InspectVolume(vol.GetName())
		if err != nil {
			return volumes, err
		}
		replSets := volDetails.GetReplicaSets()
		var replNodes []string
		for _, replSet := range replSets {
			replNodes = append(replNodes, replSet.GetNodes()...)
		}
		if Contains(replNodes, nodeId) {
			volumes = append(volumes, vol.GetName())
		}
	}

	return volumes, nil
}

// IsKubevirtInstalled returns true if Kubevirt is installed else returns false
func IsKubevirtInstalled() bool {
	k8sApiExtensions := apiextensions.Instance()
	crdList, err := k8sApiExtensions.ListCRDs()
	if err != nil {
		return false
	}
	for _, crd := range crdList.Items {
		if crd.Name == "kubevirts.kubevirt.io" {
			k8sKubevirt := kubevirt.Instance()
			version, err := k8sKubevirt.GetVersion()
			if err == nil && version != "" {
				log.InfoD("Version %s", version)
				return true
			}
		}
	}
	return false
}

// IsCloudsnapBackupActiveOnVolume returns true is cloudsnap backup is active on the volume
func IsCloudsnapBackupActiveOnVolume(volName string) (bool, error) {
	params := make(map[string]string)
	csBksps, err := Inst().V.GetCloudsnaps(volName, params)
	if err != nil {
		return false, err
	}

	for _, csBksp := range csBksps {
		if csBksp.GetSrcVolumeName() == volName && (csBksp.GetStatus() == opsapi.SdkCloudBackupStatusType_SdkCloudBackupStatusTypeActive || csBksp.GetStatus() == opsapi.SdkCloudBackupStatusType_SdkCloudBackupStatusTypeNotStarted) {

			return true, nil
		}
	}

	return false, nil
}

// GetVolumeSnapShotScheduleOfVol returns VolumeSnapshotSchedule for the given volume
func GetVolumeSnapShotScheduleOfVol(volName string) (*storkapi.VolumeSnapshotSchedule, error) {

	params := make(map[string]string)
	csBksps, err := Inst().V.GetCloudsnaps(volName, params)
	if err != nil {
		return nil, err
	}
	volDetails, err := Inst().V.InspectVolume(volName)
	if err != nil {
		return nil, err
	}

	pvcName, ok := volDetails.Spec.VolumeLabels["pvc"]
	if ok {

		for _, csBksp := range csBksps {
			if csBksp.GetSrcVolumeName() == volName {
				ns := csBksp.GetNamespace()
				snapshotSchedules, err := storkops.Instance().ListSnapshotSchedules(ns)
				if err != nil {
					return nil, err
				}

				for _, snapshotSched := range snapshotSchedules.Items {
					if strings.Contains(snapshotSched.GetName(), pvcName) {
						return &snapshotSched, nil
					}
				}
			}

		}

	}

	return nil, fmt.Errorf("snapshot schedule for volume [%s] not found", volName)
}

// MoveReplica moves the replica of a volume from one node to another
func MoveReplica(volName, fromNode, toNode string) error {
	appVol, err := Inst().V.InspectVolume(volName)
	log.Infof("Updating replicas for volume [%s/%s]", appVol.Id, volName)
	if err != nil {
		return err
	}
	replNodes := appVol.GetReplicaSets()[0].GetNodes()
	tpVol := &volume.Volume{ID: volName, Name: volName}
	if len(replNodes) == 1 {

		err = Inst().V.SetReplicationFactor(tpVol, 2, []string{toNode}, nil, true)
		if err != nil {
			return err
		}
		err = Inst().V.SetReplicationFactor(tpVol, 1, []string{fromNode}, nil, true)
		if err != nil {
			return err
		}
	} else {
		err = Inst().V.SetReplicationFactor(tpVol, int64(len(replNodes)-1), []string{fromNode}, nil, true)
		if err != nil {
			return err
		}
		err = Inst().V.SetReplicationFactor(tpVol, int64(len(replNodes)), []string{toNode}, nil, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func GetNodeIdToMoveReplica(volName string) (string, error) {

	appVol, err := Inst().V.InspectVolume(volName)
	if err != nil {
		return "", err
	}
	replNodes := appVol.GetReplicaSets()[0].GetNodes()
	stNodes := node.GetStorageNodes()
	log.Infof("replNodes %v", replNodes)

	ShuffleSlice(stNodes)

	for _, stNode := range stNodes {
		log.Infof("checking for node [%s]", stNode.VolDriverNodeID)
		if !Contains(replNodes, stNode.VolDriverNodeID) {
			return stNode.VolDriverNodeID, nil
		}
	}

	return "", err
}

// ShuffleSlice shuffles the elements of a slice in place.
func ShuffleSlice[T any](slice []T) {
	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source) // Initialize the random number generator

	for i := range slice {
		j := rng.Intn(i + 1)                    // Generate a random index from 0 to i.
		slice[i], slice[j] = slice[j], slice[i] // Swap the elements at indices i and j.
	}
}

func PrereqForNodeDecomm(nodeToDecommission node.Node, suspendedScheds []*storkapi.VolumeSnapshotSchedule) error {
	nodeVols, err := GetVolumesOnNode(nodeToDecommission.VolDriverNodeID)
	if err != nil {
		return fmt.Errorf("error getting volumes node [%s],Err: %v ", nodeToDecommission.Name, err)
	}

	credCmd := "cred list -j | grep uuid"

	output, err := Inst().V.GetPxctlCmdOutputConnectionOpts(nodeToDecommission, credCmd, node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: 5 * time.Second,
		Timeout:         30 * time.Second,
	}, false)

	if err == nil && len(output) > 0 {
		log.InfoD("Cloudsnap is enabled. Checking if backup is active on volumes")
		for _, vol := range nodeVols {
			isBackupActive, err := IsCloudsnapBackupActiveOnVolume(vol)
			if err != nil {
				return fmt.Errorf("error checking backup status  for volume  [%s],Err: %v ", vol, err)
			}
			log.Infof("Backup status for vol [%s]: %v", vol, isBackupActive)

			if isBackupActive {
				scheduleOfVol, err := GetVolumeSnapShotScheduleOfVol(vol)
				if err != nil {
					return fmt.Errorf("error getting volumes snapshot schedule for volume [%s],Err: %v ", vol, err)
				}

				if !*scheduleOfVol.Spec.Suspend {
					log.Infof("Snapshot schedule is not suspended. Suspending it")
					makeSuspend := true
					scheduleOfVol.Spec.Suspend = &makeSuspend
					_, err := storkops.Instance().UpdateSnapshotSchedule(scheduleOfVol)
					if err != nil {
						return fmt.Errorf("error suspending volumes snapshot schedule for volume  [%s],Err: %v ", vol, err)
					}
					suspendedScheds = append(suspendedScheds, scheduleOfVol)
				}
			}

		}
	}

	for _, vol := range nodeVols {
		newReplicaNode, err := GetNodeIdToMoveReplica(vol)
		log.Infof("newReplicaNode %s", newReplicaNode)
		if err != nil {
			return fmt.Errorf("error getting replica node for volume [%s],Err: %v ", vol, err)
		}

		err = MoveReplica(vol, nodeToDecommission.VolDriverNodeID, newReplicaNode)
		if err != nil {
			return fmt.Errorf("error moving replica from node [%s] to volume [%s],Err: %v ", nodeToDecommission.VolDriverNodeID, newReplicaNode, err)
		}
	}
	return nil
}

// ValidatePXStatus validates if PX is running on all nodes
func ValidatePXStatus() error {
	for _, n := range node.GetStorageDriverNodes() {
		if err := Inst().V.WaitDriverUpOnNode(n, 30*time.Second); err != nil {
			return err
		}
	}

	return nil
}

// GetNodeFromIPAddress returns node details from the provided IP Address
func GetNodeFromIPAddress(ipaddress string) (*node.Node, error) {
	for _, eachNode := range node.GetNodes() {
		log.Infof(fmt.Sprintf("Comparing [%v] with [%v]", eachNode.GetMgmtIp(), ipaddress))
		if eachNode.GetMgmtIp() == ipaddress {
			log.Infof("Matched IP Address [%v]", eachNode.MgmtIp)
			return &eachNode, nil
		}
	}
	return nil, fmt.Errorf("Unable to fetch Node details from ipaddress [%v]", ipaddress)
}

// IsVolumeTypePureBlock Returns true if the volume type if pureBlock
func IsVolumeTypePureBlock(ctx *scheduler.Context, volName string) (bool, error) {
	vols, err := Inst().S.GetVolumeParameters(ctx)
	if err != nil {
		return false, err
	}
	for vol, params := range vols {
		log.Infof(fmt.Sprintf("Checking for Volume [%v]", vol))
		if vol == volName && params["backend"] == k8s.PureBlock {
			return true, nil
		}
	}
	return false, nil
}

// SetUnSetDiscardMountRTOptions Set discard mount run time options on the Node
func SetUnSetDiscardMountRTOptions(n *node.Node, unset bool) error {
	rtOptions := 1
	if !unset {
		rtOptions = 0
	}
	optionsMap := make(map[string]string)
	optionsMap["discard_mount_force"] = fmt.Sprintf("%v", rtOptions)
	err := Inst().V.SetClusterRunTimeOpts(*n, optionsMap)
	if err != nil {
		return err
	}
	return nil
}

// ValidatePxLicenseSummary validates the license summary by comparing SKU and feature quantities
func ValidatePxLicenseSummary() error {
	summary, err := Inst().V.GetLicenseSummary()
	if err != nil {
		return fmt.Errorf("failed to get license summary. Err: [%v]", err)
	}
	log.Infof("License summary: %v", summary)
	switch {
	case IsIksCluster():
		log.Infof("Get SKU and compare with IBM cloud license activated using catalog")
		// Get SKU and compare with IBM cloud license
		isValidLicense := summary.SKU == IBMTestLicenseSKU || summary.SKU == IBMTestLicenseDRSKU || summary.SKU == IBMProdLicenseSKU || summary.SKU == IBMProdLicenseDRSKU
		if !isValidLicense {
			return fmt.Errorf("license type is not valid: %v", summary.SKU)
		}
		log.InfoD("Compare with PX IBM cloud licensed features")
		isTestOrProdSKU := summary.SKU == IBMTestLicenseSKU || summary.SKU == IBMProdLicenseSKU
		for _, feature := range summary.Features {
			if limit, ok := IBMLicense[LabLabel(feature.Name)]; ok {
				// Special handling for DisasterRecovery feature and certain SKUs
				if !isTestOrProdSKU && LabLabel(feature.Name) == LabDisasterRecovery {
					limit = &pxapi.LicensedFeature_Enabled{Enabled: true}
				}
				if !reflect.DeepEqual(feature.Quantity, limit) {
					return fmt.Errorf("verifying quantity for [%v]: actual [%v], expected [%v]", feature.Name, feature.Quantity, limit)
				}
			}
		}
		log.Infof("Validated IBM cloud license successfully")
	case IsAksCluster():
		return fmt.Errorf("license validation is not supported on AKS cluster")
	case IsEksCluster():
		return fmt.Errorf("license validation is not supported on EKS cluster")
	case IsOkeCluster():
		return fmt.Errorf("license validation is not supported on OKE cluster")
	case IsPksCluster():
		return fmt.Errorf("license validation is not supported on PKS cluster")
	default:
		return fmt.Errorf("license validation is not supported on Unknown cluster")
	}
	return nil
}

// SplitStorageDriverUpgradeURL splits the given storage driver upgrade URL into endpoint and version
// For the upgradeURL https://install.portworx.com/3.1.1, this returns Endpoint: https://install.portworx.com and Version: 3.1.1
func SplitStorageDriverUpgradeURL(upgradeURL string) (string, string, error) {
	parsedURL, err := url.Parse(upgradeURL)
	if err != nil {
		return "", "", err
	}
	pathSegments := strings.Split(strings.TrimSuffix(parsedURL.Path, "/"), "/")
	endpoint := *parsedURL
	endpoint.Path = strings.Join(pathSegments[:len(pathSegments)-1], "/")
	pxVersion := pathSegments[len(pathSegments)-1]
	return endpoint.String(), pxVersion, nil
}

// GetIQNOfNode returns the IQN of the given node in a FA setup
func GetIQNOfNode(n node.Node) (string, error) {
	cmd := "cat /etc/iscsi/initiatorname.iscsi"
	output, err := runCmdGetOutput(cmd, n)
	if err != nil {
		return "", err
	}

	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "InitiatorName") {
			return strings.Split(line, "=")[1], nil
		}
	}
	return "", fmt.Errorf("iqn not found")
}

// GetIQNOfFA gets the IQN of the FA
func GetIQNOfFA(n node.Node, FAclient flasharray.Client) (string, error) {
	//Run iscsiadm commands to login to the controllers
	networkInterfaces, err := pureutils.GetSpecificInterfaceBasedOnServiceType(&FAclient, "iscsi")
	log.FailOnError(err, "Failed to get network interfaces based on service type")

	for _, networkInterface := range networkInterfaces {
		ip := networkInterface.Address
		log.InfoD("IP address of the iscsi service: %v", ip)
		cmd := fmt.Sprintf("iscsiadm -m discovery -t st -p %s", ip)
		output, err := runCmdGetOutput(cmd, n)
		if err != nil {
			return "", err
		}
		log.InfoD("Output of iscsiadm discovery command: %v", output)
		// Split the input text by newline character to get each line
		controllers := strings.Split(output, "\n")
		// Loop through each line
		for _, controller := range controllers {
			// Split the line by space
			parts := strings.Split(controller, " ")
			if len(parts) == 2 {
				iqn := parts[1]
				return iqn, nil
			}
		}

	}
	return "", fmt.Errorf("IQN not found")

}

func RefreshIscsiSession(n node.Node) error {
	cmd := "iscsiadm -m session --rescan"
	_, err := runCmdGetOutput(cmd, n)
	if err != nil {
		return err
	}
	return nil
}

// GetPVCObjFromVol Returns pvc object from Volume
func GetPVCObjFromVol(vol *volume.Volume) (*corev1.PersistentVolumeClaim, error) {
	return k8sCore.GetPersistentVolumeClaim(vol.Name, vol.Namespace)
}

// Enables and Sets trashcan on the cluster
func EnableTrashcanOnCluster(size string) error {
	currNode := node.GetStorageDriverNodes()[0]
	log.Infof("setting value of trashcan (volume-expiration-minutes) to [%v] ", size)
	err := Inst().V.SetClusterOptsWithConfirmation(currNode,
		map[string]string{"--volume-expiration-minutes": fmt.Sprintf("%v", size)})
	return err
}

// WaitForVolumeClean Returns True if Volume in clean state
func WaitForVolumeClean(vol *volume.Volume) error {
	t := func() (interface{}, bool, error) {
		volDetails, err := Inst().V.InspectVolume(vol.ID)
		if err != nil {
			return nil, true, fmt.Errorf("error getting volume by using id %s", vol.ID)
		}

		for _, v := range volDetails.RuntimeState {
			log.InfoD("RuntimeState is in state %s", v.GetRuntimeState()["RuntimeState"])
			if v.GetRuntimeState()["RuntimeState"] == "clean" {
				return nil, false, nil
			}
		}
		return nil, true, fmt.Errorf("volume resync hasn't started")
	}
	_, err := task.DoRetryWithTimeout(t, 30*time.Minute, 60*time.Second)
	return err
}

// GetFADetailsUsed Returns list of FlashArrays used in the cluster
func GetFADetailsUsed() ([]pureutils.FlashArrayEntry, error) {
	//get the flash array details
	volDriverNamespace, err := Inst().V.GetVolumeDriverNamespace()
	if err != nil {
		return nil, fmt.Errorf("Failed to get details on FlashArray used in the cluster")
	}

	pxPureSecret, err := pureutils.GetPXPureSecret(volDriverNamespace)
	if err != nil {
		return nil, fmt.Errorf("Unable to get Px Pure Secret")
	}

	if len(pxPureSecret.Arrays) > 0 {
		return pxPureSecret.Arrays, nil
	}
	return nil, fmt.Errorf("Failed to list FA Arrays ")
}

func FlashArrayGetIscsiPorts() (map[string][]string, error) {
	flashArrays, err := GetFADetailsUsed()
	log.FailOnError(err, "Failed to get flasharray details")

	faWithIscsi := make(map[string][]string)
	for _, eachFaInt := range flashArrays {
		// Connect to Flash Array using Mgmt IP and API Token
		faClient, err := pureutils.PureCreateClientAndConnect(eachFaInt.MgmtEndPoint, eachFaInt.APIToken)
		if err != nil {
			return nil, err
		}

		// Get All Data Interfaces
		faData, err := pureutils.GetSpecificInterfaceBasedOnServiceType(faClient, "iscsi")
		if err != nil || len(faData) == 0 {
			log.Infof("Failed to get data interface on to FA using Mgmt IP [%v]", eachFaInt.MgmtEndPoint)
			return nil, err
		}
		log.InfoD("All FA Details [%v]", faData)

		for _, eachFA := range faData {
			log.Infof("Each FA Details [%v]", eachFA)
			if eachFA.Enabled && eachFA.Address != "" {
				log.Infof("Fa Interface with iscsi IP [%v]", eachFA.Name)
				faWithIscsi[eachFaInt.MgmtEndPoint] = append(faWithIscsi[eachFaInt.MgmtEndPoint], eachFA.Name)
			}
		}
	}
	return faWithIscsi, nil
}

// DisableFlashArrayNetworkInterface Disables network interface provided fa Management IP and IFace to Disable on FA
func DisableFlashArrayNetworkInterface(faMgmtIP string, iface string) error {
	flashArrays, err := GetFADetailsUsed()
	if err != nil {
		return err
	}

	for _, eachFaInt := range flashArrays {
		// Connect to Flash Array using Mgmt IP and API Token
		faClient, err := pureutils.PureCreateClientAndConnect(eachFaInt.MgmtEndPoint, eachFaInt.APIToken)
		if err != nil {
			return err
		}

		if eachFaInt.MgmtEndPoint == faMgmtIP {
			isEnabled, err := pureutils.IsNetworkInterfaceEnabled(faClient, iface)
			log.FailOnError(err, fmt.Sprintf("Interface [%v] is not enabled on FA [%v]", iface, eachFaInt.MgmtEndPoint))
			if err != nil {
				log.Errorf("Interface [%v] is not enabled on FA [%v]", iface, eachFaInt.MgmtEndPoint)
				return err
			}
			if !isEnabled {
				log.Infof("Network interface is not enabled Ignoring...")
				return nil
			}

			// Ignore the check here as there is an issue with API's that we are using
			_, _ = pureutils.DisableNetworkInterface(faClient, iface)
			time.Sleep(10 * time.Second)

			isDisabled, errDis := pureutils.IsNetworkInterfaceEnabled(faClient, iface)
			log.Infof("[%v] is Enabled [%v]", iface, isDisabled)
			if !isDisabled && errDis == nil {
				return nil
			}
		}
	}
	return fmt.Errorf("Disabling Interface failed for interface [%v] on Mgmt Ip [%v]", iface, faMgmtIP)
}

// EnableFlashArrayNetworkInterface Enables network interface on FA Management IP
func EnableFlashArrayNetworkInterface(faMgmtIP string, iface string) error {
	flashArrays, err := GetFADetailsUsed()
	if err != nil {
		return err
	}
	for _, eachFaInt := range flashArrays {
		// Connect to Flash Array using Mgmt IP and API Token
		faClient, err := pureutils.PureCreateClientAndConnect(eachFaInt.MgmtEndPoint, eachFaInt.APIToken)
		if err != nil {
			log.Errorf("Failed to connect to FA using Mgmt IP [%v]", eachFaInt.MgmtEndPoint)
			return err
		}

		if eachFaInt.MgmtEndPoint == faMgmtIP {
			isEnabled, err := pureutils.IsNetworkInterfaceEnabled(faClient, iface)
			if err != nil {
				log.Errorf(fmt.Sprintf("Interface [%v] is not enabled on FA [%v]", iface, eachFaInt.MgmtEndPoint))
				return err
			}
			if isEnabled {
				log.Infof("Network Interface [%v] is already enabled on cluster [%v]", iface, eachFaInt.MgmtEndPoint)
				return nil
			}

			// Ignore the check here as there is an issue with API's that we are using
			_, _ = pureutils.EnableNetworkInterface(faClient, iface)
			isEnabled, errDis := pureutils.IsNetworkInterfaceEnabled(faClient, iface)
			if isEnabled && errDis == nil {
				return nil
			}
		}
	}
	return fmt.Errorf("Enabling Interface failed for interface [%v] on Mgmt Ip [%v]", iface, faMgmtIP)
}

// GetFBDetailsFromCluster Returns list of FlashBlades used in the cluster
func GetFBDetailsFromCluster() ([]pureutils.FlashBladeEntry, error) {
	//get the flash array details
	volDriverNamespace, err := Inst().V.GetVolumeDriverNamespace()
	if err != nil {
		return nil, fmt.Errorf("Failed to get details on FlashBlade used in the cluster")
	}

	pxPureSecret, err := pureutils.GetPXPureSecret(volDriverNamespace)
	if err != nil {
		return nil, fmt.Errorf("Unable to get Px Pure Secret")
	}

	if len(pxPureSecret.Blades) > 0 {
		return pxPureSecret.Blades, nil
	}
	return nil, fmt.Errorf("Failed to list available blades from FB ")
}

// FilterAllPureVolumes returns filtered Pure Volumes from list of Volumes
func FilterAllPureVolumes(volumes []*volume.Volume) ([]volume.Volume, error) {
	pureVolumes := []volume.Volume{}
	for _, eachVol := range volumes {
		isPureVol, err := Inst().V.IsPureVolume(eachVol)
		log.FailOnError(err, "validating pureVolume returned err")
		if isPureVol {
			pureVolumes = append(pureVolumes, *eachVol)
		}
	}
	return pureVolumes, nil
}

// GetFADetailsFromVolumeName returns FA Struct of FA where the volume is created
func GetFADetailsFromVolumeName(volumeName string) ([]pureutils.FlashArrayEntry, error) {
	faEntries := []pureutils.FlashArrayEntry{}
	allFAs, err := GetFADetailsUsed()
	if err != nil {
		return nil, err
	}
	for _, eachFA := range allFAs {
		// Connect to FA Client
		log.Info("Connecting to FA [%v]", eachFA.MgmtEndPoint)
		faClient, err := pureutils.PureCreateClientAndConnect(eachFA.MgmtEndPoint, eachFA.APIToken)
		if err != nil {
			log.Errorf("Failed to connect to FA using Mgmt IP [%v]", eachFA.MgmtEndPoint)
			return nil, err
		}

		// List all the Volumes present in FA
		allVolumes, err := pureutils.ListAllTheVolumesFromSpecificFA(faClient)
		if err != nil {
			return nil, err
		}
		for _, eachVol := range allVolumes {
			if strings.Contains(eachVol.Name, volumeName) {
				log.Infof("Volume [%v] present on Host [%v]", eachVol.Name, eachFA.MgmtEndPoint)
				faEntries = append(faEntries, eachFA)
			}
		}
	}
	return faEntries, nil
}

// GetVolumeCompleteNameOnFA returns volume Name with Prefix from FA
func GetVolumeCompleteNameOnFA(faClient *flasharray.Client, volName string) (string, error) {
	// List all the Volumes present in FA
	allVolumes, err := pureutils.ListAllTheVolumesFromSpecificFA(faClient)
	if err != nil {
		return "", err
	}
	for _, eachVol := range allVolumes {
		if strings.Contains(eachVol.Name, volName) {
			return eachVol.Name, nil
		}
	}
	return "", nil
}

// GetConnectedHostToVolume returns the host details attached to Volume
func GetConnectedHostToVolume(faClient *flasharray.Client, volumeName string) (string, error) {
	allHostVolumes, err := pureutils.ListVolumesFromHosts(faClient)
	if err != nil {
		return "", err
	}
	for eachHost, volumeDetails := range allHostVolumes {
		for _, eachVol := range volumeDetails {
			if eachVol.Vol == volumeName {
				log.Infof("Volume [%v] is present in Host [%v]", eachVol.Vol, eachHost)
				return eachHost, nil
			}
		}
	}
	return "", fmt.Errorf("Failed to get details of Host for Volume [%v]", volumeName)
}

// DeleteVolumeFromFABackend returns true if volume is deleted from backend
func DeleteVolumeFromFABackend(fa pureutils.FlashArrayEntry, volumeName string) (bool, error) {
	faClient, err := pureutils.PureCreateClientAndConnect(fa.MgmtEndPoint, fa.APIToken)
	if err != nil {
		log.Errorf("Failed to connect to FA using Mgmt IP [%v]", fa.MgmtEndPoint)
		return false, err
	}

	volName, err := GetVolumeCompleteNameOnFA(faClient, volumeName)
	if err != nil {
		return false, err
	}
	log.Infof("Name of the Volume is [%v]", volName)

	// Get details of Host from Volume Name
	hostName, err := GetConnectedHostToVolume(faClient, volName)
	if err != nil {
		return false, err
	}
	log.Infof("Host Name attached to Volume [%v] is [%v]", volName, hostName)

	// Disconnect Host from Volume
	connVol, err := pureutils.DisConnectVolumeFromHost(faClient, hostName, volName)
	if err != nil {
		return false, err
	}
	log.Infof("Details of Disconnected Volume [%v]", connVol)

	// Verify if volume exists in specific FA
	isExists, err := pureutils.IsFAVolumeExists(faClient, volName)
	if err != nil {
		return false, err
	}
	if !isExists {
		log.Infof("Volume [%v] doesn't exist on the FA Cluster [%v]", volName, fa.MgmtEndPoint)
		return true, nil
	}
	// Delete the Volume from FA Backend
	log.Infof("Deleting volume with Name [%v]", volName)
	_, err = pureutils.DeleteVolumeOnFABackend(faClient, volName)
	if err != nil {
		return false, err
	}
	// Verify if the volume is still exists on FA
	// Verify if volume exists in specific FA
	isExists, err = pureutils.IsFAVolumeExists(faClient, volName)
	if err != nil {
		return false, err
	}
	if isExists {
		return false, fmt.Errorf("Volume [%v] still exist in backend", volName)
	}
	return true, nil

}

type MultipathDevices struct {
	DevId  string
	DmID   string
	Size   string
	Status string
	Type   string
	Paths  []PathInfo
}

// PathInfo represents information about a path of a multipath device.
type PathInfo struct {
	Devnode string
	Status  string
}

// Returns all the list of multipath devices present in the cluster nodes
func GetAllMultipathDevicesPresent(n *node.Node) ([]MultipathDevices, error) {
	multiPathDevs := []MultipathDevices{}
	output, err := runCmdOnce(fmt.Sprintf("multipath -ll"), *n)
	log.Infof("%v", err)
	if err != nil {
		return nil, err
	}
	log.Infof("Output Details before parsing [%v]", output)

	devDetailsPattern := regexp.MustCompile(`^(.*)\s+(dm-.*)\s+(.*)\,.*`)
	sizeMatchPattern := regexp.MustCompile(`.*size=([0-9]+G)\s+.*`)
	statusMatchPatern := regexp.MustCompile(`.*status\=(\w+)`)
	sdMatchPattern := regexp.MustCompile(`.*\s+(\d+:\d+:\d+:\d+)+\s+(sd\w+)\s+([0-9:]+)\s+(.*)`)

	// Create a Reader from the string
	reader := strings.Split(output, "\n")
	log.Infof("Output Details [%v]", reader)

	initPatternFound := false
	multipathDevices := MultipathDevices{}
	for i, eachLine := range reader {
		matched := devDetailsPattern.FindStringSubmatch(eachLine)
		if len(matched) > 1 {
			if initPatternFound {
				multiPathDevs = append(multiPathDevs, multipathDevices)
			}
			multipathDevices = MultipathDevices{}
			multipathDevices.DevId = matched[1]
			multipathDevices.DmID = matched[2]
			multipathDevices.Type = matched[3]
			initPatternFound = true
		}

		matched = sizeMatchPattern.FindStringSubmatch(eachLine)
		if len(matched) > 1 {
			multipathDevices.Size = matched[1]
		}

		matched = statusMatchPatern.FindStringSubmatch(eachLine)
		if len(matched) > 1 {
			multipathDevices.Status = matched[1]
		}

		matched = sdMatchPattern.FindStringSubmatch(eachLine)
		if len(matched) > 1 {
			paths := PathInfo{}
			paths.Devnode = matched[2]
			paths.Status = matched[4]
			multipathDevices.Paths = append(multipathDevices.Paths, paths)
		}
		// Validate Last Line
		if i == len(reader)-1 {
			multiPathDevs = append(multiPathDevs, multipathDevices)
		}
	}

	return multiPathDevs, nil
}

// GetMultipathDeviceIDsOnNode returns List of all Multipath Devices on Node
func GetMultipathDeviceIDsOnNode(n *node.Node) ([]string, error) {
	multiPathDev := []string{}
	multiPathDevices, err := GetAllMultipathDevicesPresent(n)
	if err != nil {
		return nil, err
	}
	for _, eachMultipathDev := range multiPathDevices {
		multiPathDev = append(multiPathDev, eachMultipathDev.DevId)
	}
	return multiPathDev, nil
}

// CreateFlashStorageClass Creates storage class for Purity Backend
// ReclaimPolicy can be v1.PersistentVolumeReclaimDelete, v1.PersistentVolumeReclaimRetain, v1.PersistentVolumeReclaimRecycle
// volumeBinding storageapi.VolumeBindingImmediate, storageapi.VolumeBindingWaitForFirstConsumer
func CreateFlashStorageClass(scName string,
	scType string,
	ReclaimPolicy corev1.PersistentVolumeReclaimPolicy,
	params map[string]string,
	MountOptions []string,
	AllowVolumeExpansion *bool,
	VolumeBinding storageapi.VolumeBindingMode,
	AllowedTopologies map[string][]string) error {

	var reclaimPolicy corev1.PersistentVolumeReclaimPolicy
	param := make(map[string]string)
	for key, value := range params {
		param[key] = value
	}
	// add pure backend type
	param["backend"] = scType
	param["repl"] = "1"

	v1obj := metav1.ObjectMeta{
		Name: scName,
	}

	if VolumeBinding != storageapi.VolumeBindingImmediate && VolumeBinding != storageapi.VolumeBindingWaitForFirstConsumer {
		return fmt.Errorf("Unsupported binding mode specified , please use storageapi.VolumeBindingImmediate or storageapi.VolumeBindingWaitForFirstConsumer")
	}

	// Declare Reclaim Policies
	switch ReclaimPolicy {
	case corev1.PersistentVolumeReclaimDelete:
		reclaimPolicy = corev1.PersistentVolumeReclaimDelete
	case corev1.PersistentVolumeReclaimRetain:
		reclaimPolicy = corev1.PersistentVolumeReclaimRetain
	case corev1.PersistentVolumeReclaimRecycle:
		reclaimPolicy = corev1.PersistentVolumeReclaimRecycle
	}

	var allowedTopologies []corev1.TopologySelectorTerm = nil
	if AllowedTopologies != nil {
		topologySelector := corev1.TopologySelectorTerm{}
		topologyList := []corev1.TopologySelectorLabelRequirement{}
		for key, value := range AllowedTopologies {
			topology := corev1.TopologySelectorLabelRequirement{}
			topology.Key = key
			topology.Values = value
			topologyList = append(topologyList, topology)
		}
		topologySelector.MatchLabelExpressions = topologyList
		allowedTopologies = append(allowedTopologies, topologySelector)
	}

	scObj := storageapi.StorageClass{
		ObjectMeta:           v1obj,
		Provisioner:          k8s.CsiProvisioner,
		Parameters:           param,
		MountOptions:         MountOptions,
		ReclaimPolicy:        &reclaimPolicy,
		AllowVolumeExpansion: AllowVolumeExpansion,
		VolumeBindingMode:    &VolumeBinding,
		AllowedTopologies:    allowedTopologies,
	}

	k8storage := schedstorage.Instance()
	_, err := k8storage.CreateStorageClass(&scObj)
	return err
}

// CreateFlashPVCOnCluster Creates PVC on the Cluster
func CreateFlashPVCOnCluster(pvcName string, scName string, nameSpace string, sizeGb string) error {
	log.InfoD("creating PVC [%s] in namespace [%s]", pvcName, nameSpace)
	pvcObj := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: nameSpace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &scName,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(sizeGb),
				},
			},
		},
	}
	_, err := core.Instance().CreatePersistentVolumeClaim(pvcObj)
	return err
}

// GetAllPVCFromNs returns all PVC's Created on specific NameSpace
func GetAllPVCFromNs(nsName string, labelSelector map[string]string) ([]corev1.PersistentVolumeClaim, error) {
	pvcList, err := core.Instance().GetPersistentVolumeClaims(nsName, labelSelector)
	if err != nil {
		return nil, err
	}
	return pvcList.Items, nil
}

// Returns details about cloud drives present in the cluster
type CloudConfig struct {
	Type              string            `json:"Type"`
	Size              int               `json:"Size"`
	ID                string            `json:"ID"`
	PoolID            string            `json:"PoolID"`
	Path              string            `json:"Path"`
	Iops              int               `json:"Iops"`
	Vpus              int               `json:"Vpus"`
	PXType            string            `json:"PXType"`
	State             string            `json:"State"`
	Labels            map[string]string `json:"labels"`
	AttachOptions     interface{}       `json:"AttachOptions"`
	Provisioner       string            `json:"Provisioner"`
	EncryptionKeyInfo string            `json:"EncryptionKeyInfo"`
}

type CloudData struct {
	Configs            map[string]CloudConfig `json:"Configs"`
	NodeID             string                 `json:"NodeID"`
	ReservedInstanceID string                 `json:"ReservedInstanceID"`
	SchedulerNodeName  string                 `json:"SchedulerNodeName"`
	NodeIndex          int                    `json:"NodeIndex"`
	CreateTimestamp    time.Time              `json:"CreateTimestamp"`
	InstanceID         string                 `json:"InstanceID"`
	Zone               string                 `json:"Zone"`
	State              string                 `json:"State"`
	Labels             map[string]string      `json:"labels"`
}

// GetCloudDriveDetailsOnCluster returns list of cloud drives on the cluster
func GetCloudDriveList() (*map[string]CloudData, error) {
	var data map[string]CloudData
	allNodes := node.GetStorageNodes()
	for _, eachNode := range allNodes {

		stNode, err := Inst().V.GetDriverNode(&eachNode)
		if err != nil {
			return nil, err
		}
		if stNode.Status != opsapi.Status_STATUS_OK {
			continue
		}
		command := "pxctl cd list -j"
		output, err := runCmdOnce(command, eachNode)
		log.Infof("%v", err)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal([]byte(output), &data)
		if err != nil {
			fmt.Println("Error:", err)
			return nil, err
		}
		break
	}
	log.Infof("%v", &data)
	return &data, nil

}

// GetCloudDrivesOnSpecificNode Returns details of all Cloud drives from specific Node
func GetCloudDrivesOnSpecificNode(n *node.Node) (*CloudData, error) {
	allCloudDrives, err := GetCloudDriveList()
	if err != nil {
		return nil, err
	}
	for nodeId, cList := range *allCloudDrives {
		if nodeId == n.Id {
			return &cList, nil
		}
	}
	return nil, fmt.Errorf("Failed to get Cloud Drive on Specific Node [%v]", n.Name)
}

// IsPureCluster returns True if backend Type is Pure
func IsPureCluster() bool {
	if stc, err := Inst().V.GetDriver(); err == nil {
		if oputil.IsPure(stc) {
			logrus.Infof("Pure installation with PX operator detected.")
			return true
		}
	}
	return false
}

// IsPureCloudProvider Returns true if cloud Provider is Pure
func IsPureCloudProvider() bool {
	if stc, err := Inst().V.GetDriver(); err == nil {
		if oputil.GetCloudProvider(stc) == "pure" {
			return true
		}
	}
	return false
}

// GetMultipathDeviceOnPool Returns list of multipath devices on Pool
func GetMultipathDeviceOnPool(n *node.Node) (map[string][]string, error) {
	multipathMap := make(map[string][]string)

	// Get All Multipath Devices on the perticular Node
	allMultipathDev, err := GetMultipathDeviceIDsOnNode(n)
	if err != nil {
		return nil, err
	}

	allPools, err := Inst().V.GetPoolDrives(n)
	if err != nil {
		return nil, err
	}

	for eachPoolId, eachDev := range allPools {
		for _, dev := range eachDev {
			for _, multiDev := range allMultipathDev {
				if strings.Contains(dev.Device, multiDev) {
					multipathMap[eachPoolId] = append(multipathMap[eachPoolId], multiDev)
				}
			}
		}
	}
	return multipathMap, nil
}

func CreatePortworxStorageClass(scName string, ReclaimPolicy corev1.PersistentVolumeReclaimPolicy, VolumeBinding storageapi.VolumeBindingMode, params map[string]string) (*storageapi.StorageClass, error) {
	v1obj := metav1.ObjectMeta{
		Name: scName,
	}
	scObj := storageapi.StorageClass{
		ObjectMeta:        v1obj,
		Provisioner:       k8s.PortworxVolumeProvisioner,
		Parameters:        params,
		ReclaimPolicy:     &ReclaimPolicy,
		VolumeBindingMode: &VolumeBinding,
	}
	k8sStorage := schedstorage.Instance()
	sc, err := k8sStorage.CreateStorageClass(&scObj)
	if err != nil {
		return nil, fmt.Errorf("failed to create CsiSnapshot storage class: %s.Error: %v", scName, err)
	}
	return sc, err
}

// Checks if list of volumes are present in group of flash arrays declared in pure.json and vice versa
func CheckVolumesExistinFA(flashArrays []pureutils.FlashArrayEntry, listofFadaPvc []string, NoVolumeExists bool) error {
	/*
		pvcFadaMap is a map which has volume name as key and value will NoVolume boolean value , once we find volume we mark it as true and after the loop
		we check if all volumes are marked as true and vice versa once we try to check deleted volumes
	*/
	/*
		NoVolumeExists is a boolean value which is used to check if we are checking for volume existence or deleted volume existence
		NoVolumeExists -- false , means all volume names initially marked false and if we find volume in FA mark it as true
		NoVolumeExists -- true , means all volume names initially marked true and if we don't find volume in FA mark it as false
	*/
	pvcFadaMap := make(map[string]bool)
	for _, volumeName := range listofFadaPvc {
		pvcFadaMap[volumeName] = NoVolumeExists
	}
	for _, fa := range flashArrays {
		faClient, err := pureutils.PureCreateClientAndConnect(fa.MgmtEndPoint, fa.APIToken)
		if err != nil {
			log.InfoD("Failed to connect to FA using Mgmt IP [%v]", fa.MgmtEndPoint)
			return err
		}
		for _, volumeName := range listofFadaPvc {
			if !NoVolumeExists {
				//This is to make sure we dont iterate through volumes which are already found in one FA,which means the value for that volume name is already true
				if pvcFadaMap[volumeName] {
					continue
				}
			}
			volName, err := GetVolumeCompleteNameOnFA(faClient, volumeName)
			if volName != "" {
				// As we found the volume we mark corresponding volume as true
				log.Infof("Volume [%v] exists on FA [%v]", volName, fa.MgmtEndPoint)
				pvcFadaMap[volumeName] = true

			} else if err != nil && volName == "" {
				log.FailOnError(err, fmt.Sprintf("Failed to get volume name for volume [%v] on FA [%v]", volumeName, fa.MgmtEndPoint))

			} else {
				log.Infof("Volume [%v] does not exist on FA [%v]", volumeName, fa.MgmtEndPoint)
				pvcFadaMap[volumeName] = false
			}
		}
	}
	// Loop through the map to check the volume status
	for FadaVol, volStatus := range pvcFadaMap {
		if NoVolumeExists {
			// when NoVolumeExists is true, we dont want any volume to be present in FA
			if volStatus {
				return fmt.Errorf("PVC %s exists in FA", FadaVol)
			}
		} else {
			// when Novolume is false, we want all volumes to be present in FA
			if !volStatus {
				return fmt.Errorf("PVC %s does not exist", FadaVol)
			}
		}
	}
	return nil
}

// Checks if list of volumes are present in group of flash blades declared in pure.json and vice versa
func CheckVolumesExistinFB(flashBlades []pureutils.FlashBladeEntry, listofFbdaPvc []string, NoVolumeExists bool) error {
	/*
		pvcFbdaMap is a map which has volume name as key and value will NoVolume boolean value , once we find volume we mark it as true and after the loop
		we check if all volumes are marked as true and vice versa once we try to check deleted volumes
	*/
	/*
		NoVolumeExists is a boolean value which is used to check if we are checking for volume existence or deleted volume existence
		NoVolumeExists -- false , means all volume names initially marked false and if we find volume in FA mark it as true
		NoVolumeExists -- true , means all volume names initially marked true and if we don't find volume in FA mark it as false
	*/
	pvcFbdaMap := make(map[string]bool)
	for _, volumeName := range listofFbdaPvc {
		pvcFbdaMap[volumeName] = NoVolumeExists
	}
	for _, fb := range flashBlades {
		fbClient, err := pureutils.PureCreateFbClientAndConnect(fb.MgmtEndPoint, fb.APIToken)
		if err != nil {
			return err
		}
		for _, volumeName := range listofFbdaPvc {
			if !NoVolumeExists {
				if pvcFbdaMap[volumeName] {
					continue
				}
			}
			FsFullName, nameErr := pureutils.GetFilesystemFullName(fbClient, volumeName)
			log.FailOnError(nameErr, fmt.Sprintf("Failed to get volume name for volume [%v] on FB [%v]", volumeName, fb.MgmtEndPoint))
			isExists, err := pureutils.IsFileSystemExists(fbClient, FsFullName)

			if isExists && err == nil {
				log.Infof("Volume [%v] exists on FB [%v]", volumeName, fb.MgmtEndPoint)
				pvcFbdaMap[volumeName] = true
			} else if !isExists && err == nil {
				log.Infof("Volume [%v] does not exist on FB [%v]", volumeName, fb.MgmtEndPoint)
				pvcFbdaMap[volumeName] = false
			}
			log.FailOnError(err, fmt.Sprintf("Failed to get volume name for volume [%v] on FB [%v]", volumeName, fb.MgmtEndPoint))
		}
	}
	for FbdaVol, volStatus := range pvcFbdaMap {
		if NoVolumeExists {
			if volStatus {
				return fmt.Errorf("PVC %s exists in FB", FbdaVol)
			}
		} else {
			if !volStatus {
				return fmt.Errorf("PVC %s does not exist", FbdaVol)
			}
		}
	}
	return nil
}
func CheckIopsandBandwidthinFA(flashArrays []pureutils.FlashArrayEntry, listofFadaPvc []string, reqBandwidth uint64, reqIops uint64) error {
	pvcFadaMap := make(map[string]bool)
	for _, volumeName := range listofFadaPvc {
		pvcFadaMap[volumeName] = false
	}
	for _, fa := range flashArrays {
		faClient, err := pureutils.PureCreateClientAndConnectRest2_x(fa.MgmtEndPoint, fa.APIToken)
		log.FailOnError(err, fmt.Sprintf("Failed to connect to FA using Mgmt IP [%v]", fa.MgmtEndPoint))
		volumes, err := pureutils.ListAllVolumesFromFA(faClient)
		log.FailOnError(err, "Failed to list all volumes from FA")
		for _, volname := range listofFadaPvc {
			if pvcFadaMap[volname] {
				continue
			}
			for _, volume := range volumes {
				for _, volItem := range volume.Items {
					if strings.Contains(volItem.Name, volname) {
						bandwidth := volItem.QoS.BandwidthLimit
						bandwidth = bandwidth / units.GiB
						log.InfoD("bandwidth for volume [%v] is [%v]", volname, bandwidth)
						iops := volItem.QoS.IopsLimit
						log.FailOnError(err, "Failed to convert iops to int")
						log.InfoD("iops for volume [%v] is [%v]", volname, iops)
						//compare bandwidth and iops with max_iops and max_bandwidth
						if bandwidth < reqBandwidth || iops < reqIops {
							pvcFadaMap[volname] = false
						} else {
							pvcFadaMap[volname] = true
						}
					}
				}
			}
		}
	}
	for FadaVol, volStatus := range pvcFadaMap {
		if !volStatus {
			return fmt.Errorf("PVC %s does not have required iops and bandwidth", FadaVol)
		}
	}
	return nil

}

// RunCmdsOnAllMasterNodes Runs a set of commands on all the master nodes
func RunCmdsOnAllMasterNodes(cmds []string) error {
	for _, node := range node.GetMasterNodes() {
		for _, cmd := range cmds {
			log.InfoD(fmt.Sprintf("Running command %s on %s", cmd, node.Name))
			_, err := runCmdOnceNonRoot(cmd, node)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ConfigureClusterLevelPSA Configure cluster level PSA settings where all newly created namespaces will be affected by
// it and will exclude kube-system, default, px-backup and portworx namespace
func ConfigureClusterLevelPSA(psaProfile string, skipNamespace []string) error {

	// Get the namespace where portworx is present
	pxNs, err := Inst().S.GetPortworxNamespace()
	if err != nil {
		return err
	}

	// Get the namespace where px-backup is present
	pxBackupNamespace, err := backup.GetPxBackupNamespace()
	if err != nil {
		log.InfoD("%s", err)
	}

	// Create a list of all the namespaces which need to be excluded
	namespaces := []string{"default", "kube-system"}
	if pxNs != "kube-system" {
		namespaces = append(namespaces, pxNs)
	}
	if pxBackupNamespace != "" {
		namespaces = append(namespaces, pxBackupNamespace)
	}
	namespaces = append(namespaces, skipNamespace...)
	joined := "\"" + strings.Join(namespaces, "\",\"") + "\""

	tempFilePath := kubeApiServerConfigFilePath + ".tmp"

	cmds := []string{
		fmt.Sprintf("mkdir -p /etc/kubernetes/admission"),
		fmt.Sprintf("curl -o %s http://kubevirt-disk-registry.pwx.dev.purestorage.com/more_images/admissioncontroller.yaml", KubeAdmissionControllerFilePath),
		fmt.Sprintf("sed -i 's/{Profile}/%s/' %s", psaProfile, KubeAdmissionControllerFilePath),
		fmt.Sprintf("sed -i 's/{NS}/%s/' %s", joined, KubeAdmissionControllerFilePath),
		fmt.Sprintf("cat  %s > %s", kubeApiServerConfigFilePath, tempFilePath),
		fmt.Sprintf(`sed -i -e '/- kube-apiserver/a\ \ \  - --admission-control-config-file=/etc/kubernetes/admission/admissioncontroller.yaml' %s`, tempFilePath),
		fmt.Sprintf(`sed -i -e '/volumeMounts:/a\ \ \  - mountPath: /etc/kubernetes/admission/\n\ \ \ \ \ \ name: admission-conf\n\ \ \ \ \ \ readOnly: true' %s`, tempFilePath),
		fmt.Sprintf(`sed -i -e '/volumes:/a\  - hostPath:\n\ \ \ \ \ \ path: /etc/kubernetes/admission/\n\ \ \ \ \ \ type: DirectoryOrCreate\n\ \ \ \ name: admission-conf' %s`, tempFilePath),
		fmt.Sprintf("cat  %s > %s", kubeApiServerConfigFilePath, kubeApiServerConfigFilePathBkp),
		fmt.Sprintf("mv %s %s", tempFilePath, kubeApiServerConfigFilePath),
	}
	log.Infof(fmt.Sprintf("%s", strings.Join(cmds, "\n")))
	// Run the above set of commands in all the master nodes
	err = RunCmdsOnAllMasterNodes(cmds)
	if err != nil {
		return err
	}

	// Sleeping till the kubeAPI server comes up
	time.Sleep(KubeApiServerWait)

	// Wait for cluster to be in normal state
	t := func() (interface{}, bool, error) {
		if _, err := core.Instance().GetPods("kube-system", nil); err == nil {
			return "", false, nil
		}

		return "", true, nil
	}

	_, err = task.DoRetryWithTimeout(t, kubeApiServerBringUpTimeout, KubeApiServerWait)
	if err != nil {
		return fmt.Errorf("API server didn't come up after change")
	}
	return nil
}

// RevertClusterLevelPSA Revert cluster level PSA settings set in the previous release
func RevertClusterLevelPSA() error {

	tempFilePath := kubeApiServerConfigFilePath + ".tmp"

	cmds := []string{
		fmt.Sprintf("mv %s %s", kubeApiServerConfigFilePath, tempFilePath),
		fmt.Sprintf("mv %s %s", kubeApiServerConfigFilePathBkp, kubeApiServerConfigFilePath),
		fmt.Sprintf("rm -rf %s", tempFilePath),
		fmt.Sprintf("rm -rf /etc/kubernetes/admission"),
	}

	// Run the above set of commands in all the master nodes
	err := RunCmdsOnAllMasterNodes(cmds)
	if err != nil {
		return err
	}

	// Sleeping till the kubeAPI server comes up
	time.Sleep(KubeApiServerWait)

	// Wait for cluster to be in normal state
	t := func() (interface{}, bool, error) {
		if _, err := core.Instance().GetPods("kube-system", nil); err == nil {
			return "", false, nil
		}

		return "", true, nil
	}

	_, err = task.DoRetryWithTimeout(t, kubeApiServerBringUpTimeout, KubeApiServerWait)
	if err != nil {
		return fmt.Errorf("API server didn't come up after change")
	}
	return nil
}

// VerifyClusterLevelPSA Verify if the cluster level PSA settings are set correctly
func VerifyClusterlevelPSA() error {
	pods, err := core.Instance().GetPods("kube-system", map[string]string{"component": "kube-apiserver"})
	if err != nil {
		return err
	}
	command := pods.Items[0].Spec.Containers[0].Command
	commandOpt := "--admission-control-config-file=/etc/kubernetes/admission/admissioncontroller.yaml"
	if !strings.Contains(strings.Join(command, ""), commandOpt) {
		return fmt.Errorf("PSA settings not reflecting in pod!")
	}
	return nil
}

// DeleteFilesFromS3Bucket deletes any supplied file from the supplied bucket
func DeleteFilesFromS3Bucket(bucketName string, fileName string) error {
	id, secret, endpoint, s3Region, disableSslBool := s3utils.GetAWSDetailsFromEnv()
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(id, secret, ""),
		Region:           aws.String(s3Region),
		DisableSSL:       aws.Bool(disableSslBool),
		S3ForcePathStyle: aws.Bool(true),
	},
	)
	if err != nil {
		return fmt.Errorf("Failed to get S3 session to remove specific files: [%v]", err)
	}

	s3Client := s3.New(sess)

	// List objects in the bucket.
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}
	listOutput, err := s3Client.ListObjectsV2(listInput)
	if err != nil {
		return fmt.Errorf("Failed to list objects in S3 bucket: [%v]", err)
	}

	// Filter objects that contain the fileName in their key.
	var objectsToDelete []*s3.ObjectIdentifier
	for _, item := range listOutput.Contents {
		if strings.Contains(*item.Key, fileName) {
			objectsToDelete = append(objectsToDelete, &s3.ObjectIdentifier{
				Key: item.Key,
			})
		}
	}
	if len(objectsToDelete) == 0 {
		return nil
	}

	// Create a batch delete request.
	deleteInput := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &s3.Delete{
			Objects: objectsToDelete,
		},
	}
	keys := make([]string, len(deleteInput.Delete.Objects))
	for i, obj := range deleteInput.Delete.Objects {
		keys[i] = *obj.Key
	}
	bucket := *deleteInput.Bucket

	// Delete the filtered objects.
	_, err = s3Client.DeleteObjects(deleteInput)
	if err != nil {
		return fmt.Errorf("Failed to delete objects from S3 bucket: [%v]", err)
	}
	log.Infof("The files %v are successfully deleted from the bucket [%s]", keys, bucket)
	return nil
}

// CreateNamespaceAndAssignLabels Creates a namespace and assigns labels to it
func CreateNamespaceAndAssignLabels(namespace string, labels map[string]string) error {
	t := func() (interface{}, bool, error) {
		nsSpec := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:   namespace,
				Labels: labels,
			},
		}
		ns, err := k8sCore.CreateNamespace(nsSpec)

		if k8serrors.IsAlreadyExists(err) {
			if ns, err = k8sCore.GetNamespace(namespace); err == nil {
				return ns, false, nil
			}
		}
		return ns, false, nil
	}

	_, err := task.DoRetryWithTimeout(t, NSWaitTimeout, NSWaitTimeoutRetry)
	if err != nil {
		return err
	}
	return nil
}
