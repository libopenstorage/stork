package tests

import (
	"bytes"
	context1 "context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	appType "github.com/portworx/torpedo/drivers/applications/apptypes"

	"github.com/portworx/sched-ops/k8s/kubevirt"
	"github.com/portworx/sched-ops/k8s/storage"

	migration "github.com/libopenstorage/stork/pkg/migration/controllers"
	"github.com/portworx/torpedo/drivers"
	"github.com/portworx/torpedo/drivers/backup/portworx"

	appsapi "k8s.io/api/apps/v1"

	volsnapv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	appDriver "github.com/portworx/torpedo/drivers/applications/driver"
	appUtils "github.com/portworx/torpedo/drivers/utilities"

	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/torpedo/pkg/osutils"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/hashicorp/go-version"
	v1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	. "github.com/onsi/ginkgo/v2"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/operator"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"encoding/base64"
	"encoding/json"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	tektoncdv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	storageapi "k8s.io/api/storage/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

// TestcaseAuthor List
const (
	Ak             TestcaseAuthor = "ak-px"
	Apimpalgaonkar TestcaseAuthor = "apimpalgaonkar"
	KPhalgun       TestcaseAuthor = "kphalgun-px"
	Kshithijiyer   TestcaseAuthor = "kshithijiyer-px"
	Mkoppal        TestcaseAuthor = "mkoppal-px"
	Sagrawal       TestcaseAuthor = "sagrawal-px"
	Skonda         TestcaseAuthor = "skonda-px"
	Sn             TestcaseAuthor = "sn-px"
	Tthurlapati    TestcaseAuthor = "tthurlapati-px"
	Vpinisetti     TestcaseAuthor = "vpinisetti-px"
	Sabrarhussaini TestcaseAuthor = "sabrarhussaini"
	ATrivedi       TestcaseAuthor = "atrivedi-px"
)

// TestcaseQuarter List
const (
	Q4FY23 TestcaseQuarter = "Q4FY23"
	Q1FY24 TestcaseQuarter = "Q1FY24"
	Q2FY24 TestcaseQuarter = "Q2FY24"
	Q3FY24 TestcaseQuarter = "Q3FY24"
	Q4FY24 TestcaseQuarter = "Q4FY24"
	Q1FY25 TestcaseQuarter = "Q1FY25"
	Q2FY25 TestcaseQuarter = "Q2FY25"
	Q3FY25 TestcaseQuarter = "Q3FY25"
)

const (
	CloudAccountDeleteTimeout                 = 5 * time.Minute
	CloudAccountDeleteRetryTime               = 30 * time.Second
	storkDeploymentName                       = "stork"
	defaultStorkDeploymentNamespace           = "kube-system"
	UpgradeStorkImage                         = "TARGET_STORK_VERSION"
	LatestStorkImage                          = "23.9.0"
	restoreNamePrefix                         = "tp-restore"
	DestinationClusterName                    = "destination-cluster"
	AppReadinessTimeout                       = 10 * time.Minute
	TaskNamePrefix                            = "pxb"
	BackupOrgID                               = "default"
	UsersToBeCreated                          = "USERS_TO_CREATE"
	GroupsToBeCreated                         = "GROUPS_TO_CREATE"
	MaxUsersInGroup                           = "MAX_USERS_IN_GROUP"
	MaxBackupsToBeCreated                     = "MAX_BACKUPS"
	MaxWaitPeriodForBackupCompletionInMinutes = 40
	MaxWaitPeriodForRestoreCompletionInMinute = 40
	MaxWaitPeriodForBackupJobCancellation     = 20
	MaxWaitPeriodForRestoreJobCancellation    = 20
	RestoreJobCancellationRetryTime           = 30
	RestoreJobProgressRetryTime               = 1
	BackupJobCancellationRetryTime            = 5
	K8sNodeReadyTimeout                       = 10
	K8sNodeRetryInterval                      = 30
	GlobalAWSBucketPrefix                     = "global-aws"
	GlobalAzureBucketPrefix                   = "global-azure"
	GlobalGCPBucketPrefix                     = "global-gcp"
	GlobalNFSBucketPrefix                     = "global-nfs"
	GlobalAWSLockedBucketPrefix               = "global-aws-locked"
	GlobalAzureLockedBucketPrefix             = "global-azure-locked"
	GlobalGCPLockedBucketPrefix               = "global-gcp-locked"
	MongodbStatefulset                        = "pxc-backup-mongodb"
	PxBackupDeployment                        = "px-backup"
	BackupDeleteTimeout                       = 60 * time.Minute
	BackupDeleteRetryTime                     = 30 * time.Second
	BackupLocationDeleteTimeout               = 60 * time.Minute
	BackupLocationDeleteRetryTime             = 30 * time.Second
	RebootNodeTimeout                         = 1 * time.Minute
	RebootNodeTimeBeforeRetry                 = 5 * time.Second
	LatestPxBackupVersion                     = "2.7.0"
	defaultPxBackupHelmBranch                 = "2.7.0"
	pxCentralPostInstallHookJobName           = "pxcentral-post-install-hook"
	quickMaintenancePod                       = "quick-maintenance-repo"
	fullMaintenancePod                        = "full-maintenance-repo"
	jobDeleteTimeout                          = 5 * time.Minute
	jobDeleteRetryTime                        = 10 * time.Second
	PodStatusTimeOut                          = 30 * time.Minute
	PodStatusRetryTime                        = 30 * time.Second
	licenseCountUpdateTimeout                 = 15 * time.Minute
	licenseCountUpdateRetryTime               = 1 * time.Minute
	podReadyTimeout                           = 15 * time.Minute
	storkPodReadyTimeout                      = 20 * time.Minute
	podReadyRetryTime                         = 30 * time.Second
	namespaceDeleteTimeout                    = 10 * time.Minute
	ClusterCreationRetryTime                  = 10 * time.Second
	ClusterDeleteTimeout                      = 10 * time.Minute
	ClusterDeleteRetryTime                    = 5 * time.Second
	vmStartStopTimeout                        = 10 * time.Minute
	vmStartStopRetryTime                      = 30 * time.Second
	cloudCredConfigMap                        = "cloud-config"
	volumeSnapshotClassEnv                    = "VOLUME_SNAPSHOT_CLASS"
	RancherActiveCluster                      = "local"
	RancherProjectDescription                 = "new project"
	MultiAppNfsPodDeploymentNamespace         = "kube-system"
	backupScheduleDeleteTimeout               = 60 * time.Minute
	backupScheduleDeleteRetryTime             = 30 * time.Second
	sshPodName                                = "ssh-pod"
	sshPodNamespace                           = "ssh-pod-namespace"
	VirtLauncherContainerName                 = "compute"
	storkControllerConfigMap                  = "stork-controller-config"
	storkControllerConfigMapUpdateTimeout     = 15 * time.Minute
	storkControllerConfigMapRetry             = 30 * time.Second
	BackupLocationValidationTimeout           = 10 * time.Minute
	BackupLocationValidationRetryTime         = 30 * time.Second
)

var (
	// User should keep updating preRuleApp, postRuleApp, appsWithCRDsAndWebhooks
	PreRuleApp                  = []string{"cassandra", "postgres"}
	PostRuleApp                 = []string{"cassandra"}
	AppsWithCRDsAndWebhooks     = []string{"elasticsearch-crd-webhook"} // The apps which have CRDs and webhooks
	GlobalAWSBucketName         string
	GlobalAzureBucketName       string
	GlobalGCPBucketName         string
	GlobalNFSBucketName         string
	GlobalAWSLockedBucketName   string
	GlobalAzureLockedBucketName string
	GlobalGCPLockedBucketName   string
	GlobalPreRuleName           string
	GlobalPostRuleName          string
	GlobalPreRuleUid            string
	GlobalPostRuleUid           string
	GlobalRuleFlag              bool
	cloudProviders              = []string{"aws"}
	CommonPassword              string
	backupPodLabels             = []map[string]string{
		{"app": "px-backup"}, {"app.kubernetes.io/component": "pxcentral-apiserver"},
		{"app.kubernetes.io/component": "pxcentral-backend"},
		{"app.kubernetes.io/component": "pxcentral-frontend"},
		{"app.kubernetes.io/component": "keycloak"},
		{"app.kubernetes.io/component": "pxcentral-lh-middleware"},
		{"app.kubernetes.io/component": "pxcentral-mysql"}}
	cloudPlatformList          = []string{"rke", "aws", "azure", "gke"}
	NfsBackupExecutorPodLabel  = map[string]string{"kdmp.portworx.com/driver-name": "nfsbackup"}
	NfsRestoreExecutorPodLabel = map[string]string{"kdmp.portworx.com/driver-name": "nfsrestore"}
	queryCountForValidation    = 10
	IsBackupLongevityRun       = false
	PvcListBeforeRun           []string
	PvcListAfterRun            []string
)

type UserRoleAccess struct {
	User     string
	Roles    backup.PxBackupRole
	Accesses BackupAccess
	Context  context1.Context
}

type UserAccessContext struct {
	User     string
	Accesses BackupAccess
	Context  context1.Context
}

var BackupAccessKeyValue = map[BackupAccess]string{
	1: "ViewOnlyAccess",
	2: "RestoreAccess",
	3: "FullAccess",
}

var StorkLabel = map[string]string{
	"name": "stork",
}

// Type for customResourceObjects created by backup
type customResourceObjectDetails struct {
	Group         string
	Version       string
	Resource      string
	SkipResources []string // If this list is populated that particular CR will not be considered for cleanup
}

// List of all the CRs to be considered for cleanup
var crListMap = map[string]customResourceObjectDetails{
	"applicationbackups": {
		Group:         "stork.libopenstorage.org",
		Version:       "v1alpha1",
		Resource:      "applicationbackups",
		SkipResources: []string{},
	},
	"applicationrestores": {
		Group:         "stork.libopenstorage.org",
		Version:       "v1alpha1",
		Resource:      "applicationrestores",
		SkipResources: []string{},
	},
}

type BackupAccess int32

type ReplacePolicyType int32

const (
	ReplacePolicyInvalid ReplacePolicyType = 0
	ReplacePolicyRetain  ReplacePolicyType = 1
	ReplacePolicyDelete  ReplacePolicyType = 2
)

const (
	ViewOnlyAccess BackupAccess = 1
	RestoreAccess               = 2
	FullAccess                  = 3
)

type ExecutionMode int32

const (
	Sequential ExecutionMode = iota
	Parallel
)

var (
	// AppRuleMaster is a map of struct for all the value for rules
	// This map needs to be updated for new applications as and whe required
	AppRuleMaster = map[string]backup.AppRule{
		"cassandra": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"nodetool flush -- keyspace1;", "echo 'test"}, PodSelectorList: []string{"app=cassandra", "app=cassandra1"}, Background: []string{"false", "false"}, RunInSinglePod: []string{"false", "false"}, Container: []string{},
				},
			},
			PostRule: backup.PostRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"nodetool verify -- keyspace1;", "nodetool verify -- keyspace1;"}, PodSelectorList: []string{"app=cassandra", "app=cassandra1"}, Background: []string{"false", "false"}, RunInSinglePod: []string{"false", "false"}, Container: []string{},
				},
			},
		},
		"mysql": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"mysql --user=root --password=$MYSQL_ROOT_PASSWORD -Bse 'FLUSH TABLES WITH READ LOCK;system ${WAIT_CMD};'"}, PodSelectorList: []string{"app=mysql"}, Background: []string{"true"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
			PostRule: backup.PostRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"mysql --user=root --password=$MYSQL_ROOT_PASSWORD -Bse 'FLUSH LOGS; UNLOCK TABLES;'"}, PodSelectorList: []string{"app=mysql"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"mysql-backup": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"mysql --user=root --password=$MYSQL_ROOT_PASSWORD -Bse 'FLUSH TABLES WITH READ LOCK;system ${WAIT_CMD};'"}, PodSelectorList: []string{"app=mysql"}, Background: []string{"true"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
			PostRule: backup.PostRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"mysql --user=root --password=$MYSQL_ROOT_PASSWORD -Bse 'FLUSH LOGS; UNLOCK TABLES;'"}, PodSelectorList: []string{"app=mysql"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"mysql-ibm": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"mysql --user=root --password=$MYSQL_ROOT_PASSWORD -Bse 'FLUSH TABLES WITH READ LOCK;system ${WAIT_CMD};'"}, PodSelectorList: []string{"app=mysql"}, Background: []string{"true"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
			PostRule: backup.PostRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"mysql --user=root --password=$MYSQL_ROOT_PASSWORD -Bse 'FLUSH LOGS; UNLOCK TABLES;'"}, PodSelectorList: []string{"app=mysql"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"postgres": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\""}, PodSelectorList: []string{"app=postgres"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"postgres-backup": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\""}, PodSelectorList: []string{"app=postgres"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"postgres-csi": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\""}, PodSelectorList: []string{"app=postgres"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"postgres-fada": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\""}, PodSelectorList: []string{"app=postgres"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"postgres-fbda": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\""}, PodSelectorList: []string{"app=postgres"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"postgres-azure-disk": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\""}, PodSelectorList: []string{"app=postgres"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"postgres-gce-pd": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\""}, PodSelectorList: []string{"app=postgres"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"postgres-pd-csi-storage-gke": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\""}, PodSelectorList: []string{"app=postgres"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"postgres-rbd-csi": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\""}, PodSelectorList: []string{"app=postgres"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"postgres-rgw-csi": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\""}, PodSelectorList: []string{"app=postgres"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
		"postgres-cephfs-csi": {
			PreRule: backup.PreRule{
				Rule: backup.RuleSpec{
					ActionList: []string{"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\""}, PodSelectorList: []string{"app=postgres"}, Background: []string{"false"}, RunInSinglePod: []string{"false"}, Container: []string{},
				},
			},
		},
	}
)

var (
	dataAfterBackupSuffix = "-after-backup"
)

// CloudProviderProvisionerSnapshotMap maps cloud provider names to their corresponding sub-maps containing provisioner-snapshot class mappings
// TODO: Need to update the map for gke,aks and aws
var CloudProviderProvisionerSnapshotMap = map[string]map[string]struct {
	snapshotClasses []string // List of snapshot classes for the provisioner
	defaultSnapshot string   // Default snapshot class for the provisioner
	appList         []string
}{
	"gke": {
		"pd.csi.storage.gke.io": {
			snapshotClasses: []string{},
			defaultSnapshot: "gke-snapshot-class-1",
			appList:         []string{"postgres-gke-csi"},
		},
	},
	"ibm": {
		"vpc.block.csi.ibm.io": {
			snapshotClasses: []string{},
			defaultSnapshot: "ibmc-vpcblock-snapshot",
			appList:         []string{"postgres-csi"},
		},
	},
	"aws": {
		"aws-provisioner": {
			snapshotClasses: []string{"aws-snapshot-class", "aws-snapshot-class-2"},
			defaultSnapshot: "aws-snapshot-class",
			appList:         []string{},
		},
	},
	"aks": {
		"aks-provisioner": {
			snapshotClasses: []string{"aks-snapshot-class", "aks-snapshot-class-2"},
			defaultSnapshot: "aks-snapshot-class",
			appList:         []string{},
		},
	},
	"openshift": {
		"cephfs-csi": {
			snapshotClasses: []string{"ocs-storagecluster-cephfsplugin-snapclass"},
			defaultSnapshot: "ocs-storagecluster-cephfsplugin-snapclass",
			appList:         []string{"postgres-cephfs-csi"},
		}, "rbd-csi": {
			snapshotClasses: []string{"ocs-storagecluster-rbdplugin-snapclass"},
			defaultSnapshot: "ocs-storagecluster-rbdplugin-snapclass",
			appList:         []string{"postgres-rbd-csi"},
		},
	},
}

// GetProvisionerDefaultSnapshotMap returns a map with provisioner to default volumeSnapshotClass mappings for the specified cloud provider
func GetProvisionerDefaultSnapshotMap(cloudProvider string) map[string]string {
	provisionerSnapshotMap := make(map[string]string)
	provisionerMap, ok := CloudProviderProvisionerSnapshotMap[cloudProvider]
	if !ok {
		return provisionerSnapshotMap
	}

	for provisioner, info := range provisionerMap {
		if info.defaultSnapshot != "" {
			provisionerSnapshotMap[provisioner] = info.defaultSnapshot
		}
	}

	return provisionerSnapshotMap
}

// GetProvisionerSnapshotClassesMap returns a map of provisioners with their corresponding list of SnapshotClasses for the specified provider
func GetProvisionerSnapshotClassesMap(cloudProvider string) map[string]string {
	provisionerSnapshotClasses := make(map[string]string)

	// Check if the provider exists in the provisioner map
	providerProvisioners, ok := CloudProviderProvisionerSnapshotMap[cloudProvider]
	if !ok {
		return provisionerSnapshotClasses
	}

	// Iterate over the provisioners for the specified provider
	for provisioner, info := range providerProvisioners {
		if len(info.snapshotClasses) > 0 {
			// Get a random index
			randomIndex := rand.Intn(len(info.snapshotClasses))
			provisionerSnapshotClasses[provisioner] = info.snapshotClasses[randomIndex]
		}
	}

	return provisionerSnapshotClasses
}

// GetApplicationSpecForProvisioner returns application specification for the specified provisioner
func GetApplicationSpecForProvisioner(cloudProvider string, provisionerName string) ([]string, error) {
	//var speclist []string

	provisionerInfo, ok := CloudProviderProvisionerSnapshotMap[cloudProvider]
	if !ok {
		return []string{}, fmt.Errorf("provisioner %s not found for cloud provider %s", provisionerName, cloudProvider)
	}

	info, ok := provisionerInfo[provisionerName]
	if !ok {
		return []string{}, fmt.Errorf("provisioner %s not found for cloud provider %s", provisionerName, cloudProvider)
	}

	/*	for _, appName := range info.appList {
		if strings.Contains(appName, applicationName) {
			speclist = append(speclist, appName)
		}
	}*/

	return info.appList, nil
}

// Set default provider as aws
func GetBackupProviders() []string {
	providersStr := os.Getenv("PROVIDERS")
	if providersStr != "" {
		return strings.Split(providersStr, ",")
	}
	return cloudProviders
}

// getPXNamespace fetches px namespace from env else sends backup kube-system
func getPXNamespace() string {
	namespace := os.Getenv("PX_NAMESPACE")
	if namespace != "" {
		return namespace
	}
	return defaultStorkDeploymentNamespace
}

// CreateBackup creates backup and checks for success
func CreateBackup(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, ctx context1.Context) error {
	_, err := CreateBackupByNamespacesWithoutCheck(backupName, clusterName, bLocation, bLocationUID, namespaces, labelSelectors, orgID, uid, preRuleName, preRuleUid, postRuleName, postRuleUid, ctx)
	if err != nil {
		return err
	}

	err = BackupSuccessCheck(backupName, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Backup [%s] created successfully", backupName)
	return nil
}

// CreateBackupWithVscMapping creates backup and checks for success
func CreateBackupWithVscMapping(backupName string, clusterName string, bLocation string, bLocationUID string, namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, ctx context1.Context, provisionerVolumeSnapshotClassMap map[string]string, forceKdmp bool) error {
	_, err := CreateBackupByNamespacesWithoutCheckWithVscMapping(backupName, clusterName, bLocation, bLocationUID, namespaces, labelSelectors, orgID, uid, preRuleName, preRuleUid, postRuleName, postRuleUid, ctx, provisionerVolumeSnapshotClassMap, forceKdmp)
	if err != nil {
		return err
	}

	err = BackupSuccessCheck(backupName, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Backup [%s] created successfully", backupName)
	return nil
}

// CreateVMBackup creates a VM backup and checks for success
func CreateVMBackup(backupName string, vms []kubevirtv1.VirtualMachine, clusterName string, bLocation string, bLocationUID string,
	labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, skipVMAutoExecRules bool, ctx context1.Context) error {
	_, err := CreateVMBackupByNamespacesWithoutCheck(backupName, vms, clusterName, bLocation, bLocationUID, labelSelectors, orgID, uid, preRuleName, preRuleUid, postRuleName, postRuleUid, skipVMAutoExecRules, ctx)
	if err != nil {
		return err
	}

	err = BackupSuccessCheck(backupName, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Backup [%s] created successfully", backupName)
	return nil
}

// CreateBackupWithCRValidation creates backup and checks for success along with Backup CR Validation
func CreateBackupWithCRValidation(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, ctx context1.Context) error {
	backupInspectResponse, err := CreateBackupByNamespacesWithoutCheck(backupName, clusterName, bLocation, bLocationUID, namespaces, labelSelectors, orgID, uid, preRuleName, preRuleUid, postRuleName, postRuleUid, ctx)
	if err != nil {
		return err
	}

	err = ValidateBackupCR(backupInspectResponse, ctx)
	if err != nil {
		return err
	}

	err = BackupSuccessCheck(backupName, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Backup [%s] created successfully", backupName)
	return nil
}

// ValidateBackupCR validates the CR creation for backup
func ValidateBackupCR(backupInspectResponse *api.BackupInspectResponse, ctx context1.Context) error {

	// Getting the backup object from backupInspectResponse
	backupObject := backupInspectResponse.GetBackup()

	backupDriver := Inst().Backup
	clusterUID, err := backupDriver.GetClusterUID(ctx, BackupOrgID, backupObject.Cluster)
	if err != nil {
		return err
	}

	return validateBackupCRs(
		backupObject.Name,
		backupObject.Cluster,
		backupObject.OrgId,
		clusterUID,
		backupObject.Namespaces,
		ctx)

}

// GetCsiSnapshotClassName returns the name of CSI Volume Snapshot class based on the env variable - VOLUME_SNAPSHOT_CLASS
func GetCsiSnapshotClassName() (string, error) {
	var snapShotClasses *volsnapv1.VolumeSnapshotClassList
	var err error
	if snapShotClasses, err = Inst().S.GetAllSnapshotClasses(); err != nil {
		return "", err
	}
	if len(snapShotClasses.Items) > 0 {
		log.InfoD("Volume snapshot classes found in the cluster - ")
		for _, snapshotClass := range snapShotClasses.Items {
			log.InfoD(snapshotClass.GetName())
		}
		volumeSnapshotClass, present := os.LookupEnv(volumeSnapshotClassEnv)
		if present {
			log.InfoD("Picking the volume snapshot class [%s] from env variable [%s]", volumeSnapshotClass, volumeSnapshotClassEnv)
			return volumeSnapshotClass, nil
		} else {
			log.InfoD("Env variable %s not set hence returning empty volume snapshot class name", volumeSnapshotClassEnv)
			return "", nil
		}
	} else {
		log.InfoD("no volume snapshot classes found in the cluster")
		return "", nil
	}
}

func FilterAppContextsByNamespace(appContexts []*scheduler.Context, namespaces []string) (filteredAppContexts []*scheduler.Context) {
	for _, appContext := range appContexts {
		if Contains(namespaces, appContext.ScheduleOptions.Namespace) {
			filteredAppContexts = append(filteredAppContexts, appContext)
		}
	}
	return
}

func InsertDataForBackupValidation(namespaces []string, ctx context1.Context, existingAppHandler []appDriver.ApplicationDriver, backupName string,
	commandBeforeBackup map[appDriver.ApplicationDriver]map[string][]string) ([]appDriver.ApplicationDriver, map[appDriver.ApplicationDriver]map[string][]string, error) {

	// afterBackup - Check if the data is being inserted before or after backup

	// Getting app handlers for deployed apps in the namespace and inserting data to same
	var err error
	var allHandlers []appDriver.ApplicationDriver
	var dataCommands = make(map[appDriver.ApplicationDriver]map[string][]string)

	if len(existingAppHandler) == 0 {
		for _, eachNamespace := range namespaces {
			if handler, ok := NamespaceAppWithDataMap[eachNamespace]; ok {
				log.Infof("App with data support found under namespace - [%s]", eachNamespace)
				for _, eachHandler := range handler {
					log.Infof("Adding data to app in namespace  - [%s]", eachNamespace)
					dataCommands[eachHandler] = eachHandler.GetRandomDataCommands(queryCountForValidation)
					err = eachHandler.InsertBackupData(ctx, backupName, dataCommands[eachHandler]["insert"])
					if err != nil {
						return nil, nil, err
					}
					allHandlers = append(allHandlers, eachHandler)
				}
			}
		}
	} else {

		backupDriver := Inst().Backup
		backupUid, err := backupDriver.GetBackupUID(ctx, backupName, BackupOrgID)

		for _, eachHandler := range existingAppHandler {
			log.Infof("Backup UUID while adding data - [%s]", backupUid)
			eachHandler.AddDataCommands(backupUid, commandBeforeBackup[eachHandler])
			restoreIdentifier := fmt.Sprintf("%s%s", backupUid, dataAfterBackupSuffix)
			eachHandler.UpdateDataCommands(queryCountForValidation, restoreIdentifier)
			err = eachHandler.InsertBackupData(ctx, restoreIdentifier, []string{})
			if err != nil {
				return nil, nil, err
			}
		}
	}

	return allHandlers, dataCommands, nil
}

// CreateBackupWithValidation creates backup, checks for success, and validates the backup
func CreateBackupWithValidation(ctx context1.Context, backupName string, clusterName string, bLocation string, bLocationUID string, scheduledAppContextsToBackup []*scheduler.Context, labelSelectors map[string]string, orgID string, uid string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string) error {
	namespaces := make([]string, 0)
	for _, scheduledAppContext := range scheduledAppContextsToBackup {
		namespace := scheduledAppContext.ScheduleOptions.Namespace
		if !Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}

	log.InfoD("Backup [%s] started at [%s]", backupName, time.Now().Format("2006-01-02 15:04:05"))
	// Insert data before backup which is expected to be present after restore
	appHandlers, commandBeforeBackup, err := InsertDataForBackupValidation(namespaces, ctx, []appDriver.ApplicationDriver{}, backupName, nil)
	if err != nil {
		return fmt.Errorf("Some error occurred while inserting data for backup validation. Error - [%s]", err.Error())
	}

	err = CreateBackup(backupName, clusterName, bLocation, bLocationUID, namespaces, labelSelectors, orgID, uid, preRuleName, preRuleUid, postRuleName, postRuleUid, ctx)
	if err != nil {
		return err
	}

	// Insert data after backup which is expected NOT to be present after restore
	_, _, err = InsertDataForBackupValidation(namespaces, ctx, appHandlers, backupName, commandBeforeBackup)
	if err != nil {
		return fmt.Errorf("Some error occurred while inserting data for backup validation after backup success check. Error - [%s]", err.Error())
	}

	return ValidateBackup(ctx, backupName, orgID, scheduledAppContextsToBackup, make([]string, 0))
}

// CreateBackupWithValidationWithVscMapping creates backup, checks for success, and validates the backup
func CreateBackupWithValidationWithVscMapping(ctx context1.Context, backupName string, clusterName string, bLocation string, bLocationUID string, scheduledAppContextsToBackup []*scheduler.Context, labelSelectors map[string]string, orgID string, uid string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, provisionerVolumeSnapshotClassMap map[string]string, forceKdmp bool) error {
	namespaces := make([]string, 0)
	for _, scheduledAppContext := range scheduledAppContextsToBackup {
		namespace := scheduledAppContext.ScheduleOptions.Namespace
		if !Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}

	// Insert data before backup which is expected to be present after restore
	appHandlers, commandBeforeBackup, err := InsertDataForBackupValidation(namespaces, ctx, []appDriver.ApplicationDriver{}, backupName, nil)
	if err != nil {
		return fmt.Errorf("Some error occurred while inserting data for backup validation. Error - [%s]", err.Error())
	}

	err = CreateBackupWithVscMapping(backupName, clusterName, bLocation, bLocationUID, namespaces, labelSelectors, orgID, uid, preRuleName, preRuleUid, postRuleName, postRuleUid, ctx, provisionerVolumeSnapshotClassMap, forceKdmp)
	if err != nil {
		return err
	}

	// Insert data after backup which is expected NOT to be present after restore
	_, _, err = InsertDataForBackupValidation(namespaces, ctx, appHandlers, backupName, commandBeforeBackup)
	if err != nil {
		return fmt.Errorf("Some error occurred while inserting data for backup validation after backup success check. Error - [%s]", err.Error())
	}

	return ValidateBackup(ctx, backupName, orgID, scheduledAppContextsToBackup, make([]string, 0))
}

// CreateVMBackupWithValidation creates VM backup, checks for success, and validates the VM backup
func CreateVMBackupWithValidation(ctx context1.Context, backupName string, vms []kubevirtv1.VirtualMachine, clusterName string, bLocation string, bLocationUID string, scheduledAppContextsToBackup []*scheduler.Context, labelSelectors map[string]string, orgID string, uid string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, skipVMAutoExecRules bool) error {
	namespaces := make([]string, 0)
	var removeSpecs []interface{}
	for _, scheduledAppContext := range scheduledAppContextsToBackup {
		namespace := scheduledAppContext.ScheduleOptions.Namespace
		if !Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}

		// Removing specs which are outside the scope of VM Backup
		for _, spec := range scheduledAppContext.App.SpecList {
			// VM Backup will not consider service object for now
			if appSpec, ok := spec.(*corev1.Service); ok {
				removeSpecs = append(removeSpecs, appSpec)
			}
			// TODO: Add more types of specs to remove depending on the app context
		}
		err := Inst().S.RemoveAppSpecsByName(scheduledAppContext, removeSpecs)
		if err != nil {
			return err
		}
	}

	// Insert data before backup which is expected to be present after restore
	appHandlers, commandBeforeBackup, err := InsertDataForBackupValidation(namespaces, ctx, []appDriver.ApplicationDriver{}, backupName, nil)
	if err != nil {
		return fmt.Errorf("Some error occurred while inserting data for backup validation. Error - [%s]", err.Error())
	}

	err = CreateVMBackup(backupName, vms, clusterName, bLocation, bLocationUID, labelSelectors, orgID, uid, preRuleName, preRuleUid, postRuleName, postRuleUid, skipVMAutoExecRules, ctx)
	if err != nil {
		return err
	}

	// Insert data after backup which is expected NOT to be present after restore
	_, _, err = InsertDataForBackupValidation(namespaces, ctx, appHandlers, backupName, commandBeforeBackup)
	if err != nil {
		return fmt.Errorf("Some error occurred while inserting data for backup validation after backup success check. Error - [%s]", err.Error())
	}

	return ValidateBackup(ctx, backupName, orgID, scheduledAppContextsToBackup, make([]string, 0))
}

func UpdateBackup(backupName string, backupUid string, orgId string, cloudCred string, cloudCredUID string, ctx context1.Context) (*api.BackupUpdateResponse, error) {
	backupDriver := Inst().Backup
	bkpUpdateRequest := &api.BackupUpdateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  backupName,
			OrgId: orgId,
			Uid:   backupUid,
		},
		CloudCredential: cloudCred,
		CloudCredentialRef: &api.ObjectRef{
			Name: cloudCred,
			Uid:  cloudCredUID,
		},
	}
	status, err := backupDriver.UpdateBackup(ctx, bkpUpdateRequest)
	return status, err
}

// CreateBackupWithCustomResourceTypeWithoutValidation creates backup with custom resources without validation
func CreateBackupWithCustomResourceTypeWithoutValidation(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, resourceTypes []string, ctx context1.Context) error {

	if GlobalRuleFlag {
		preRuleName = GlobalPreRuleName
		if GlobalPreRuleName != "" {
			preRuleUid = GlobalPreRuleUid
		}

		postRuleName = GlobalPostRuleName
		if GlobalPostRuleName != "" {
			postRuleUid = GlobalPostRuleUid
		}
	}

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
		Cluster:        clusterName,
		Namespaces:     namespaces,
		LabelSelectors: labelSelectors,
		ClusterRef: &api.ObjectRef{
			Name: clusterName,
			Uid:  uid,
		},
		PreExecRuleRef: &api.ObjectRef{
			Name: preRuleName,
			Uid:  preRuleUid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: postRuleName,
			Uid:  postRuleUid,
		},
		ResourceTypes: resourceTypes,
	}

	err := AdditionalBackupRequestParams(bkpCreateRequest)
	if err != nil {
		return err
	}
	_, err = backupDriver.CreateBackup(ctx, bkpCreateRequest)
	if err != nil {
		return err
	}
	log.Infof("Backup [%s] created successfully", backupName)
	return nil
}

// CreateBackupWithCustomResourceTypeWithValidation creates backup with custom resources selected through resourceTypesFilter, checks for success, and validates the backup
func CreateBackupWithCustomResourceTypeWithValidation(ctx context1.Context, backupName string, clusterName string, bLocation string, bLocationUID string, scheduledAppContextsToBackup []*scheduler.Context, resourceTypesFilter []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string) error {
	namespaces := make([]string, 0)
	for _, scheduledAppContext := range scheduledAppContextsToBackup {
		namespace := scheduledAppContext.ScheduleOptions.Namespace
		if !Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}
	err := CreateBackupWithCustomResourceTypeWithoutValidation(backupName, clusterName, bLocation, bLocationUID, namespaces, labelSelectors, orgID, uid, preRuleName, preRuleUid, postRuleName, postRuleUid, resourceTypesFilter, ctx)
	if err != nil {
		return err
	}
	return ValidateBackup(ctx, backupName, orgID, scheduledAppContextsToBackup, resourceTypesFilter)
}

// CreateScheduleBackup creates a schedule backup and checks for success of first (immediately triggered) backup
func CreateScheduleBackup(scheduleName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, schPolicyName string, schPolicyUID string, ctx context1.Context) error {
	_, err := CreateScheduleBackupWithoutCheck(scheduleName, clusterName, bLocation, bLocationUID, namespaces, labelSelectors, orgID, preRuleName, preRuleUid, postRuleName, postRuleUid, schPolicyName, schPolicyUID, ctx)
	if err != nil {
		return err
	}
	time.Sleep(1 * time.Minute)
	firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
	if err != nil {
		return err
	}
	err = BackupSuccessCheck(firstScheduleBackupName, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Schedule backup [%s] created successfully", firstScheduleBackupName)
	return nil
}

// CreateScheduleBackupWithValidationWithVscMapping creates a schedule backup, checks for success of first (immediately triggered) backup, validates that backup and returns the name of that first scheduled backup
func CreateScheduleBackupWithValidationWithVscMapping(ctx context1.Context, scheduleName string, clusterName string, bLocation string, bLocationUID string, scheduledAppContextsToBackup []*scheduler.Context, labelSelectors map[string]string, orgID string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, schPolicyName string, schPolicyUID string, provisionerVolumeSnapshotClassMap map[string]string, forceKdmp bool) (string, error) {
	namespaces := make([]string, 0)
	for _, scheduledAppContext := range scheduledAppContextsToBackup {
		namespace := scheduledAppContext.ScheduleOptions.Namespace
		if !Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}
	_, err := CreateScheduleBackupWithoutCheckWithVscMapping(scheduleName, clusterName, bLocation, bLocationUID, namespaces, labelSelectors, orgID, preRuleName, preRuleUid, postRuleName, postRuleUid, schPolicyName, schPolicyUID, ctx, provisionerVolumeSnapshotClassMap, forceKdmp)
	if err != nil {
		return "", err
	}
	time.Sleep(1 * time.Minute)
	firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	log.InfoD("first schedule backup for schedule name [%s] is [%s]", scheduleName, firstScheduleBackupName)
	return firstScheduleBackupName, BackupSuccessCheckWithValidation(ctx, firstScheduleBackupName, scheduledAppContextsToBackup, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
}

// CreateScheduleBackupWithValidation creates a schedule backup, checks for success of first (immediately triggered) backup, validates that backup and returns the name of that first scheduled backup
func CreateScheduleBackupWithValidation(ctx context1.Context, scheduleName string, clusterName string, bLocation string, bLocationUID string, scheduledAppContextsToBackup []*scheduler.Context, labelSelectors map[string]string, orgID string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, schPolicyName string, schPolicyUID string, resourceTypes ...string) (string, error) {
	namespaces := make([]string, 0)
	for _, scheduledAppContext := range scheduledAppContextsToBackup {
		namespace := scheduledAppContext.ScheduleOptions.Namespace
		if !Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}
	_, err := CreateScheduleBackupWithoutCheck(scheduleName, clusterName, bLocation, bLocationUID, namespaces, labelSelectors, orgID, preRuleName, preRuleUid, postRuleName, postRuleUid, schPolicyName, schPolicyUID, ctx, resourceTypes...)
	if err != nil {
		return "", err
	}
	time.Sleep(1 * time.Minute)
	firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	log.InfoD("first schedule backup for schedule name [%s] is [%s]", scheduleName, firstScheduleBackupName)
	return firstScheduleBackupName, BackupSuccessCheckWithValidation(ctx, firstScheduleBackupName, scheduledAppContextsToBackup, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, resourceTypes...)
}

// CreateVMScheduledBackupWithValidation creates a VM scheduled backup and checks for success
func CreateVMScheduledBackupWithValidation(scheduleName string, vms []kubevirtv1.VirtualMachine, clusterName string, bLocation string, bLocationUID string, scheduledAppContextsToBackup []*scheduler.Context,
	labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, skipVMAutoExecRules bool, schPolicyName string, schPolicyUID string, ctx context1.Context) (string, error) {

	var removeSpecs []interface{}
	for _, scheduledAppContext := range scheduledAppContextsToBackup {
		// Removing specs which are outside the scope of VM Backup
		for _, spec := range scheduledAppContext.App.SpecList {
			// VM Backup will not consider service object for now
			if appSpec, ok := spec.(*corev1.Service); ok {
				removeSpecs = append(removeSpecs, appSpec)
			}
			// TODO: Add more types of specs to remove depending on the app context
		}
		err := Inst().S.RemoveAppSpecsByName(scheduledAppContext, removeSpecs)
		if err != nil {
			return "", err
		}
	}

	_, err := CreateVMScheduleBackupByNamespacesWithoutCheck(scheduleName, vms, clusterName, bLocation, bLocationUID, labelSelectors, orgID, uid, preRuleName, preRuleUid, postRuleName, postRuleUid, skipVMAutoExecRules, schPolicyName, schPolicyUID, ctx)
	if err != nil {
		return "", err
	}
	time.Sleep(1 * time.Minute)
	firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	log.InfoD("first schedule backup for schedule name [%s] is [%s]", scheduleName, firstScheduleBackupName)
	return firstScheduleBackupName, BackupSuccessCheckWithValidation(ctx, firstScheduleBackupName, scheduledAppContextsToBackup, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
}

// CreateScheduleBackupWithValidation creates a schedule backup, checks for success of first (immediately triggered) backup, validates that backup and returns the name of that first scheduled backup along with CR validation
func CreateScheduleBackupWithCRValidation(ctx context1.Context, scheduleName string, clusterName string, bLocation string, bLocationUID string, scheduledAppContextsToBackup []*scheduler.Context, labelSelectors map[string]string, orgID string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, schPolicyName string, schPolicyUID string) (string, error) {
	namespaces := make([]string, 0)
	for _, scheduledAppContext := range scheduledAppContextsToBackup {
		namespace := scheduledAppContext.ScheduleOptions.Namespace
		if !Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}
	backupScheduleInspectReponse, err := CreateScheduleBackupWithoutCheck(scheduleName, clusterName, bLocation, bLocationUID, namespaces, labelSelectors, orgID, preRuleName, preRuleUid, postRuleName, postRuleUid, schPolicyName, schPolicyUID, ctx)
	if err != nil {
		return "", err
	}
	time.Sleep(1 * time.Minute)
	firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	log.InfoD("first schedule backup for schedule name [%s] is [%s]", scheduleName, firstScheduleBackupName)

	err = ValidateScheduleBackupCR(firstScheduleBackupName, backupScheduleInspectReponse, ctx)
	if err != nil {
		return "", err
	}
	return firstScheduleBackupName, BackupSuccessCheckWithValidation(ctx, firstScheduleBackupName, scheduledAppContextsToBackup, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
}

// ValidateScheduleBackupCR validates creation of backup CR
func ValidateScheduleBackupCR(backupName string, backupScheduleInspectReponse *api.BackupScheduleInspectResponse, ctx context1.Context) error {

	// Getting the backup schedule object from backupScheduleInspectReponse
	backupSchedule := backupScheduleInspectReponse.BackupSchedule

	backupDriver := Inst().Backup
	clusterUID, err := backupDriver.GetClusterUID(ctx, backupSchedule.Metadata.OrgId, backupSchedule.BackupScheduleInfo.Cluster)
	if err != nil {
		return err
	}

	return validateBackupCRs(
		backupName,
		backupSchedule.BackupScheduleInfo.Cluster,
		backupSchedule.Metadata.OrgId,
		clusterUID,
		backupSchedule.BackupScheduleInfo.Namespaces,
		ctx)
}

// CreateBackupByNamespacesWithoutCheck creates backup of provided namespaces without waiting for success.
func CreateBackupByNamespacesWithoutCheck(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, ctx context1.Context) (*api.BackupInspectResponse, error) {

	if GlobalRuleFlag {
		preRuleName = GlobalPreRuleName
		if GlobalPreRuleName != "" {
			preRuleUid = GlobalPreRuleUid
		}

		postRuleName = GlobalPostRuleName
		if GlobalPostRuleName != "" {
			postRuleUid = GlobalPostRuleUid
		}
	}

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
		Cluster:        clusterName,
		Namespaces:     namespaces,
		LabelSelectors: labelSelectors,
		ClusterRef: &api.ObjectRef{
			Name: clusterName,
			Uid:  uid,
		},
		PreExecRuleRef: &api.ObjectRef{
			Name: preRuleName,
			Uid:  preRuleUid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: postRuleName,
			Uid:  postRuleUid,
		},
	}

	err := AdditionalBackupRequestParams(bkpCreateRequest)
	if err != nil {
		return nil, err
	}
	log.InfoD("Backup without check [%s] started at [%s]", backupName, time.Now().Format("2006-01-02 15:04:05"))
	_, err = backupDriver.CreateBackup(ctx, bkpCreateRequest)
	if err != nil {
		return nil, err
	}
	backupUid, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return nil, err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUid,
		OrgId: orgID,
	}
	resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// CreateBackupByNamespacesWithoutCheckWithVscMapping creates backup of provided namespaces without waiting for success.
func CreateBackupByNamespacesWithoutCheckWithVscMapping(backupName string, clusterName string, bLocation string, bLocationUID string, namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, ctx context1.Context, provisionerVolumeSnapshotClassMap map[string]string, forceKdmp bool) (*api.BackupInspectResponse, error) {

	if GlobalRuleFlag {
		preRuleName = GlobalPreRuleName
		if GlobalPreRuleName != "" {
			preRuleUid = GlobalPreRuleUid
		}

		postRuleName = GlobalPostRuleName
		if GlobalPostRuleName != "" {
			postRuleUid = GlobalPostRuleUid
		}
	}

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
		Cluster:        clusterName,
		Namespaces:     namespaces,
		LabelSelectors: labelSelectors,
		ClusterRef: &api.ObjectRef{
			Name: clusterName,
			Uid:  uid,
		},
		PreExecRuleRef: &api.ObjectRef{
			Name: preRuleName,
			Uid:  preRuleUid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: postRuleName,
			Uid:  postRuleUid,
		},
		VolumeSnapshotClassMapping: provisionerVolumeSnapshotClassMap,
		DirectKdmp:                 forceKdmp,
	}

	err := AdditionalBackupRequestParams(bkpCreateRequest)
	if err != nil {
		return nil, err
	}

	_, err = backupDriver.CreateBackup(ctx, bkpCreateRequest)
	if err != nil {
		return nil, err
	}
	backupUid, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return nil, err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUid,
		OrgId: orgID,
	}
	resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// CreateVMBackupByNamespacesWithoutCheck creates VM backup of provided namespaces without waiting for success.
func CreateVMBackupByNamespacesWithoutCheck(backupName string, vms []kubevirtv1.VirtualMachine, clusterName string, bLocation string, bLocationUID string,
	labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, skipVMAutoExecRules bool, ctx context1.Context) (*api.BackupInspectResponse, error) {

	backupDriver := Inst().Backup
	includeResource := GenerateResourceInfo(vms)
	namespaces := GetNamespacesFromVMs(vms)
	bkpCreateRequest := &api.BackupCreateRequest{
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
		ClusterRef: &api.ObjectRef{
			Name: clusterName,
			Uid:  uid,
		},
		PreExecRuleRef: &api.ObjectRef{
			Name: preRuleName,
			Uid:  preRuleUid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: postRuleName,
			Uid:  postRuleUid,
		},
		IncludeResources: includeResource,
		BackupObjectType: &api.BackupCreateRequest_BackupObjectType{
			Type: api.BackupCreateRequest_BackupObjectType_VirtualMachine,
		},
		SkipVmAutoExecRules: skipVMAutoExecRules,
	}

	err := AdditionalBackupRequestParams(bkpCreateRequest)
	if err != nil {
		return nil, err
	}

	_, err = backupDriver.CreateBackup(ctx, bkpCreateRequest)
	if err != nil {
		return nil, err
	}
	backupUid, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return nil, err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUid,
		OrgId: orgID,
	}
	resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// GetNamespacesFromVMs get a slice of namespaces from VMs
func GetNamespacesFromVMs(vms []kubevirtv1.VirtualMachine) []string {
	namespaces := make([]string, 0)
	for _, v := range vms {
		if !Contains(namespaces, v.Namespace) {
			namespaces = append(namespaces, v.Namespace)
		}
	}
	return namespaces
}

// GetNamespacesToVMsMap gets a map of namespaces to VM names
func GetNamespacesToVMsMap(vms []kubevirtv1.VirtualMachine) map[string][]string {
	namespacesToVMsMap := make(map[string][]string, len(vms))
	for _, v := range vms {
		namespacesToVMsMap[v.Namespace] = append(namespacesToVMsMap[v.Namespace], v.Name)
	}
	return namespacesToVMsMap

}

// GenerateResourceInfo generates []*api.ResourceInfo
func GenerateResourceInfo(vms []kubevirtv1.VirtualMachine) []*api.ResourceInfo {
	var includeResource []*api.ResourceInfo

	for _, v := range vms {
		includeResource = append(includeResource, &api.ResourceInfo{
			Group:     "kubevirt.io",
			Kind:      "VirtualMachine",
			Name:      v.Name,
			Namespace: v.Namespace,
			Version:   "v1",
		})
	}

	return includeResource
}

// CreateBackupWithoutCheck creates backup without waiting for success
func CreateBackupWithoutCheck(ctx context1.Context, backupName string, clusterName string, bLocation string, bLocationUID string, scheduledAppContextsToBackup []*scheduler.Context, labelSelectors map[string]string, orgID string, uid string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string) (*api.BackupInspectResponse, error) {
	namespaces := make([]string, 0)
	for _, scheduledAppContext := range scheduledAppContextsToBackup {
		namespace := scheduledAppContext.ScheduleOptions.Namespace
		if !Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}

	return CreateBackupByNamespacesWithoutCheck(backupName, clusterName, bLocation, bLocationUID, namespaces, labelSelectors, orgID, uid, preRuleName, preRuleUid, postRuleName, postRuleUid, ctx)
}

// CreateScheduleBackupWithoutCheck creates a schedule backup without waiting for success
func CreateScheduleBackupWithoutCheck(scheduleName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, schPolicyName string, schPolicyUID string, ctx context1.Context, resourceTypes ...string) (*api.BackupScheduleInspectResponse, error) {

	if GlobalRuleFlag {
		preRuleName = GlobalPreRuleName
		if GlobalPreRuleName != "" {
			preRuleUid = GlobalPreRuleUid
		}

		postRuleName = GlobalPostRuleName
		if GlobalPostRuleName != "" {
			postRuleUid = GlobalPostRuleUid
		}
	}

	backupDriver := Inst().Backup
	bkpSchCreateRequest := &api.BackupScheduleCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  scheduleName,
			OrgId: orgID,
		},
		SchedulePolicyRef: &api.ObjectRef{
			Name: schPolicyName,
			Uid:  schPolicyUID,
		},
		BackupLocationRef: &api.ObjectRef{
			Name: bLocation,
			Uid:  bLocationUID,
		},
		SchedulePolicy: schPolicyName,
		Cluster:        clusterName,
		Namespaces:     namespaces,
		LabelSelectors: labelSelectors,
		PreExecRuleRef: &api.ObjectRef{
			Name: preRuleName,
			Uid:  preRuleUid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: postRuleName,
			Uid:  postRuleUid,
		},
		ResourceTypes: resourceTypes,
	}

	err := AdditionalScheduledBackupRequestParams(bkpSchCreateRequest)
	if err != nil {
		return nil, err
	}
	_, err = backupDriver.CreateBackupSchedule(ctx, bkpSchCreateRequest)
	if err != nil {
		return nil, err
	}
	backupScheduleInspectRequest := &api.BackupScheduleInspectRequest{
		OrgId: orgID,
		Name:  scheduleName,
		Uid:   "",
	}
	resp, err := backupDriver.InspectBackupSchedule(ctx, backupScheduleInspectRequest)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// CreateVMScheduleBackupByNamespacesWithoutCheck creates VM schedule backup of provided namespaces without waiting for success.
func CreateVMScheduleBackupByNamespacesWithoutCheck(scheduleName string, vms []kubevirtv1.VirtualMachine, clusterName string, bLocation string, bLocationUID string,
	labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, skipVMAutoExecRules bool, schPolicyName string, schPolicyUID string, ctx context1.Context) (*api.BackupScheduleInspectResponse, error) {

	backupDriver := Inst().Backup
	includeResource := GenerateResourceInfo(vms)
	namespaces := GetNamespacesFromVMs(vms)
	bkpScheduleCreateRequest := &api.BackupScheduleCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  scheduleName,
			OrgId: orgID,
		},
		BackupLocationRef: &api.ObjectRef{
			Name: bLocation,
			Uid:  bLocationUID,
		},
		SchedulePolicyRef: &api.ObjectRef{
			Name: schPolicyName,
			Uid:  schPolicyUID,
		},
		SchedulePolicy: schPolicyName,
		Cluster:        clusterName,
		Namespaces:     namespaces,
		LabelSelectors: labelSelectors,
		ClusterRef: &api.ObjectRef{
			Name: clusterName,
			Uid:  uid,
		},
		PreExecRuleRef: &api.ObjectRef{
			Name: preRuleName,
			Uid:  preRuleUid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: postRuleName,
			Uid:  postRuleUid,
		},
		IncludeResources: includeResource,
		BackupObjectType: &api.BackupScheduleCreateRequest_BackupObjectType{
			Type: api.BackupScheduleCreateRequest_BackupObjectType_VirtualMachine,
		},
		SkipVmAutoExecRules: skipVMAutoExecRules,
	}

	err := AdditionalScheduledBackupRequestParams(bkpScheduleCreateRequest)
	if err != nil {
		return nil, err
	}

	_, err = backupDriver.CreateBackupSchedule(ctx, bkpScheduleCreateRequest)
	if err != nil {
		return nil, err
	}
	backupScheduleInspectRequest := &api.BackupScheduleInspectRequest{
		OrgId: orgID,
		Name:  scheduleName,
		Uid:   "",
	}
	resp, err := backupDriver.InspectBackupSchedule(ctx, backupScheduleInspectRequest)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// CreateScheduleBackupWithoutCheckWithVscMapping creates a schedule backup without waiting for success
func CreateScheduleBackupWithoutCheckWithVscMapping(scheduleName string, clusterName string, bLocation string, bLocationUID string, namespaces []string, labelSelectors map[string]string, orgID string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, schPolicyName string, schPolicyUID string, ctx context1.Context, provisionerVolumeSnapshotClassMap map[string]string, forceKdmp bool) (*api.BackupScheduleInspectResponse, error) {

	if GlobalRuleFlag {
		preRuleName = GlobalPreRuleName
		if GlobalPreRuleName != "" {
			preRuleUid = GlobalPreRuleUid
		}

		postRuleName = GlobalPostRuleName
		if GlobalPostRuleName != "" {
			postRuleUid = GlobalPostRuleUid
		}
	}

	backupDriver := Inst().Backup
	bkpSchCreateRequest := &api.BackupScheduleCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  scheduleName,
			OrgId: orgID,
		},
		SchedulePolicyRef: &api.ObjectRef{
			Name: schPolicyName,
			Uid:  schPolicyUID,
		},
		BackupLocationRef: &api.ObjectRef{
			Name: bLocation,
			Uid:  bLocationUID,
		},
		SchedulePolicy: schPolicyName,
		Cluster:        clusterName,
		Namespaces:     namespaces,
		LabelSelectors: labelSelectors,
		PreExecRuleRef: &api.ObjectRef{
			Name: preRuleName,
			Uid:  preRuleUid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: postRuleName,
			Uid:  postRuleUid,
		},
		VolumeSnapshotClassMapping: provisionerVolumeSnapshotClassMap,
		DirectKdmp:                 forceKdmp,
	}

	err := AdditionalScheduledBackupRequestParams(bkpSchCreateRequest)
	if err != nil {
		return nil, err
	}
	_, err = backupDriver.CreateBackupSchedule(ctx, bkpSchCreateRequest)
	if err != nil {
		return nil, err
	}
	backupScheduleInspectRequest := &api.BackupScheduleInspectRequest{
		OrgId: orgID,
		Name:  scheduleName,
		Uid:   "",
	}
	resp, err := backupDriver.InspectBackupSchedule(ctx, backupScheduleInspectRequest)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// ShareBackup provides access to the mentioned groups or/add users
func ShareBackup(backupName string, groupNames []string, userNames []string, accessLevel BackupAccess, ctx context1.Context) error {
	var bkpUid string
	backupDriver := Inst().Backup
	groupIDs := make([]string, 0)
	userIDs := make([]string, 0)

	bkpUid, err := backupDriver.GetBackupUID(ctx, backupName, BackupOrgID)
	if err != nil {
		return err
	}
	log.Infof("Backup UID for %s - %s", backupName, bkpUid)

	for _, groupName := range groupNames {
		groupID, err := backup.FetchIDOfGroup(groupName)
		if err != nil {
			return err
		}
		groupIDs = append(groupIDs, groupID)
	}

	for _, userName := range userNames {
		userID, err := backup.FetchIDOfUser(userName)
		if err != nil {
			return err
		}
		userIDs = append(userIDs, userID)
	}

	groupBackupShareAccessConfigs := make([]*api.BackupShare_AccessConfig, 0)

	for _, groupName := range groupNames {
		groupBackupShareAccessConfig := &api.BackupShare_AccessConfig{
			Id:     groupName,
			Access: api.BackupShare_AccessType(accessLevel),
		}
		groupBackupShareAccessConfigs = append(groupBackupShareAccessConfigs, groupBackupShareAccessConfig)
	}

	userBackupShareAccessConfigs := make([]*api.BackupShare_AccessConfig, 0)

	for _, userID := range userIDs {
		userBackupShareAccessConfig := &api.BackupShare_AccessConfig{
			Id:     userID,
			Access: api.BackupShare_AccessType(accessLevel),
		}
		userBackupShareAccessConfigs = append(userBackupShareAccessConfigs, userBackupShareAccessConfig)
	}

	shareBackupRequest := &api.BackupShareUpdateRequest{
		OrgId: BackupOrgID,
		Name:  backupName,
		Backupshare: &api.BackupShare{
			Groups:        groupBackupShareAccessConfigs,
			Collaborators: userBackupShareAccessConfigs,
		},
		Uid: bkpUid,
	}

	_, err = backupDriver.UpdateBackupShare(ctx, shareBackupRequest)
	return err

}

// ClusterUpdateBackupShare shares all backup with the users and/or groups provided for a given cluster
// addUsersOrGroups - provide true if the mentioned users/groups needs to be added
// addUsersOrGroups - provide false if the mentioned users/groups needs to be deleted or removed
func ClusterUpdateBackupShare(clusterName string, groupNames []string, userNames []string, accessLevel BackupAccess, addUsersOrGroups bool, ctx context1.Context) error {
	backupDriver := Inst().Backup
	groupIDs := make([]string, 0)
	userIDs := make([]string, 0)
	clusterUID, err := backupDriver.GetClusterUID(ctx, BackupOrgID, clusterName)
	if err != nil {
		return err
	}

	for _, groupName := range groupNames {
		groupID, err := backup.FetchIDOfGroup(groupName)
		if err != nil {
			return err
		}
		groupIDs = append(groupIDs, groupID)
	}

	for _, userName := range userNames {
		userID, err := backup.FetchIDOfUser(userName)
		if err != nil {
			return err
		}
		userIDs = append(userIDs, userID)
	}

	groupBackupShareAccessConfigs := make([]*api.BackupShare_AccessConfig, 0)

	for _, groupName := range groupNames {
		groupBackupShareAccessConfig := &api.BackupShare_AccessConfig{
			Id:     groupName,
			Access: api.BackupShare_AccessType(accessLevel),
		}
		groupBackupShareAccessConfigs = append(groupBackupShareAccessConfigs, groupBackupShareAccessConfig)
	}

	userBackupShareAccessConfigs := make([]*api.BackupShare_AccessConfig, 0)

	for _, userID := range userIDs {
		userBackupShareAccessConfig := &api.BackupShare_AccessConfig{
			Id:     userID,
			Access: api.BackupShare_AccessType(accessLevel),
		}
		userBackupShareAccessConfigs = append(userBackupShareAccessConfigs, userBackupShareAccessConfig)
	}

	backupShare := &api.BackupShare{
		Groups:        groupBackupShareAccessConfigs,
		Collaborators: userBackupShareAccessConfigs,
	}

	var clusterBackupShareUpdateRequest *api.ClusterBackupShareUpdateRequest

	if addUsersOrGroups {
		clusterBackupShareUpdateRequest = &api.ClusterBackupShareUpdateRequest{
			OrgId:          BackupOrgID,
			Name:           clusterName,
			AddBackupShare: backupShare,
			DelBackupShare: nil,
			Uid:            clusterUID,
		}
	} else {
		clusterBackupShareUpdateRequest = &api.ClusterBackupShareUpdateRequest{
			OrgId:          BackupOrgID,
			Name:           clusterName,
			AddBackupShare: nil,
			DelBackupShare: backupShare,
			Uid:            clusterUID,
		}
	}

	_, err = backupDriver.ClusterUpdateBackupShare(ctx, clusterBackupShareUpdateRequest)
	if err != nil {
		return err
	}

	clusterBackupShareStatusCheck := func() (interface{}, bool, error) {
		clusterReq := &api.ClusterInspectRequest{OrgId: BackupOrgID, Name: clusterName, IncludeSecrets: true, Uid: clusterUID}
		clusterResp, err := backupDriver.InspectCluster(ctx, clusterReq)
		if err != nil {
			return "", true, err
		}
		if clusterResp.GetCluster().BackupShareStatusInfo.GetStatus() != api.ClusterInfo_BackupShareStatusInfo_Success {
			return "", true, fmt.Errorf("cluster backup share status for cluster %s is still %s", clusterName,
				clusterResp.GetCluster().BackupShareStatusInfo.GetStatus())
		}
		log.Infof("Cluster %s has status - [%d]", clusterName, clusterResp.GetCluster().BackupShareStatusInfo.GetStatus())
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(clusterBackupShareStatusCheck, 1*time.Minute, 10*time.Second)
	if err != nil {
		return err
	}
	log.Infof("Cluster backup share check complete")
	return nil
}

func GetAllBackupsForUser(username, password string) ([]string, error) {
	backupNames := make([]string, 0)
	backupDriver := Inst().Backup
	ctx, err := backup.GetNonAdminCtx(username, password)
	if err != nil {
		return nil, err
	}

	backupEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: BackupOrgID,
	}
	currentBackups, err := backupDriver.EnumerateBackup(ctx, backupEnumerateReq)
	if err != nil {
		return nil, err
	}
	for _, backup := range currentBackups.GetBackups() {
		backupNames = append(backupNames, backup.GetName())
	}
	return backupNames, nil
}

// CreateRestore creates restore
func CreateRestore(restoreName string, backupName string, namespaceMapping map[string]string, clusterName string,
	orgID string, ctx context1.Context, storageClassMapping map[string]string) error {

	var bkpUid string

	// Check if the backup used is in successful state or not
	bkpUid, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   bkpUid,
		OrgId: orgID,
	}
	resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return err
	}
	actual := resp.GetBackup().GetStatus().Status
	reason := resp.GetBackup().GetStatus().Reason
	if actual != api.BackupInfo_StatusInfo_Success {
		return fmt.Errorf("backup status for [%s] expected was [%s] but got [%s] because of [%s]", backupName, api.BackupInfo_StatusInfo_Success, actual, reason)
	}
	backupDriver := Inst().Backup
	createRestoreReq := &api.RestoreCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  restoreName,
			OrgId: orgID,
		},
		Backup:              backupName,
		Cluster:             clusterName,
		NamespaceMapping:    namespaceMapping,
		StorageClassMapping: storageClassMapping,
		BackupRef: &api.ObjectRef{
			Name: backupName,
			Uid:  bkpUid,
		},
	}
	_, err = backupDriver.CreateRestore(ctx, createRestoreReq)
	if err != nil {
		return err
	}
	err = RestoreSuccessCheck(restoreName, orgID, MaxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Restore [%s] created successfully", restoreName)
	return nil
}

// CreateRestoreWithCRValidation creates a restore along with restore CR validation
func CreateRestoreWithCRValidation(restoreName string, backupName string, namespaceMapping map[string]string, clusterName string,
	orgID string, ctx context1.Context, storageClassMapping map[string]string) error {

	var bkpUid string

	// Check if the backup used is in successful state or not
	bkpUid, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   bkpUid,
		OrgId: orgID,
	}
	resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return err
	}
	actual := resp.GetBackup().GetStatus().Status
	reason := resp.GetBackup().GetStatus().Reason
	if actual != api.BackupInfo_StatusInfo_Success {
		return fmt.Errorf("backup status for [%s] expected was [%s] but got [%s] because of [%s]", backupName, api.BackupInfo_StatusInfo_Success, actual, reason)
	}
	backupDriver := Inst().Backup
	createRestoreReq := &api.RestoreCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  restoreName,
			OrgId: orgID,
		},
		Backup:              backupName,
		Cluster:             clusterName,
		NamespaceMapping:    namespaceMapping,
		StorageClassMapping: storageClassMapping,
		BackupRef: &api.ObjectRef{
			Name: backupName,
			Uid:  bkpUid,
		},
	}
	_, err = backupDriver.CreateRestore(ctx, createRestoreReq)
	if err != nil {
		return err
	}
	// Commenting out restore validation for now as it's happening so fast that
	// program is not able to detect the custom resource created and destroyed
	clusterUID, err := backupDriver.GetClusterUID(ctx, orgID, clusterName)
	if err != nil {
		return err
	}
	err = ValidateRestoreCRs(restoreName, clusterName, orgID, clusterUID, namespaceMapping, ctx)
	if err != nil {
		log.Warnf(err.Error())
	}
	err = RestoreSuccessCheck(restoreName, orgID, MaxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Restore [%s] created successfully", restoreName)
	return nil
}

// CreateRestoreWithReplacePolicy Creates in-place restore and waits for it to complete
func CreateRestoreWithReplacePolicy(restoreName string, backupName string, namespaceMapping map[string]string, clusterName string,
	orgID string, ctx context1.Context, storageClassMapping map[string]string, replacePolicy ReplacePolicyType) error {

	var bkp *api.BackupObject
	var bkpUid string
	backupDriver := Inst().Backup
	log.Infof("Getting the UID of the backup %s needed to be restored", backupName)
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID}
	curBackups, err := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	if err != nil {
		return err
	}
	for _, bkp = range curBackups.GetBackups() {
		if bkp.Name == backupName {
			bkpUid = bkp.Uid
			break
		}
	}
	createRestoreReq := &api.RestoreCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  restoreName,
			OrgId: orgID,
		},
		Backup:              backupName,
		Cluster:             clusterName,
		NamespaceMapping:    namespaceMapping,
		StorageClassMapping: storageClassMapping,
		BackupRef: &api.ObjectRef{
			Name: backupName,
			Uid:  bkpUid,
		},
		ReplacePolicy: api.ReplacePolicy_Type(replacePolicy),
	}
	_, err = backupDriver.CreateRestore(ctx, createRestoreReq)
	if err != nil {
		return err
	}
	err = restoreSuccessWithReplacePolicy(restoreName, orgID, MaxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx, replacePolicy)
	if err != nil {
		return err
	}
	log.Infof("Restore [%s] created successfully", restoreName)
	return nil
}

// CreateRestoreWithReplacePolicyWithValidation Creates in-place restore and waits for it to complete and then validates the restore
func CreateRestoreWithReplacePolicyWithValidation(restoreName string, backupName string, namespaceMapping map[string]string, clusterName string,
	orgID string, ctx context1.Context, storageClassMapping map[string]string, replacePolicy ReplacePolicyType, scheduledAppContexts []*scheduler.Context) (err error) {
	err = CreateRestoreWithReplacePolicy(restoreName, backupName, namespaceMapping, clusterName, orgID, ctx, storageClassMapping, replacePolicy)
	if err != nil {
		return
	}
	originalClusterConfigPath := CurrentClusterConfigPath
	if clusterConfigPath, ok := ClusterConfigPathMap[clusterName]; !ok {
		err = fmt.Errorf("switching cluster context: couldn't find clusterConfigPath for cluster [%s]", clusterName)
		return
	} else {
		log.InfoD("Switching cluster context to cluster [%s]", clusterName)
		err = SetClusterContext(clusterConfigPath)
		if err != nil {
			return
		}
	}
	defer func() {
		log.InfoD("Switching cluster context back to cluster path [%s]", originalClusterConfigPath)
		err = SetClusterContext(originalClusterConfigPath)
		if err != nil {
			err = fmt.Errorf("failed switching cluster context back to cluster path [%s]. Err: [%v]", originalClusterConfigPath, err)
		}
	}()
	expectedRestoredAppContexts := make([]*scheduler.Context, 0)
	for _, scheduledAppContext := range scheduledAppContexts {
		expectedRestoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, namespaceMapping, storageClassMapping, true)
		if err != nil {
			log.Errorf("TransformAppContextWithMappings: %v", err)
			continue
		}
		expectedRestoredAppContexts = append(expectedRestoredAppContexts, expectedRestoredAppContext)
	}
	err = ValidateRestore(ctx, restoreName, orgID, expectedRestoredAppContexts, make([]string, 0))
	if err != nil {
		return
	}
	return
}

// CreateRestoreWithUID creates restore with UID
func CreateRestoreWithUID(restoreName string, backupName string, namespaceMapping map[string]string, clusterName string,
	orgID string, ctx context1.Context, storageClassMapping map[string]string, backupUID string) error {

	backupDriver := Inst().Backup
	log.Infof("Getting the UID of the backup needed to be restored")

	createRestoreReq := &api.RestoreCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  restoreName,
			OrgId: orgID,
		},
		Backup:              backupName,
		Cluster:             clusterName,
		NamespaceMapping:    namespaceMapping,
		StorageClassMapping: storageClassMapping,
		BackupRef: &api.ObjectRef{
			Name: backupName,
			Uid:  backupUID,
		},
	}
	_, err := backupDriver.CreateRestore(ctx, createRestoreReq)
	if err != nil {
		return err
	}
	err = RestoreSuccessCheck(restoreName, orgID, MaxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Restore [%s] created successfully", restoreName)
	return nil
}

// CreateRestoreWithoutCheck creates restore without waiting for completion
func CreateRestoreWithoutCheck(restoreName string, backupName string,
	namespaceMapping map[string]string, clusterName string, orgID string, ctx context1.Context) (*api.RestoreInspectResponse, error) {

	var bkp *api.BackupObject
	var bkpUid string
	backupDriver := Inst().Backup
	log.Infof("Getting the UID of the backup needed to be restored")
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID}
	curBackups, _ := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	log.Debugf("Enumerate backup response -\n%v", curBackups)
	for _, bkp = range curBackups.GetBackups() {
		if bkp.Name == backupName {
			bkpUid = bkp.Uid
			break
		}
	}
	createRestoreReq := &api.RestoreCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  restoreName,
			OrgId: orgID,
		},
		Backup:           backupName,
		Cluster:          clusterName,
		NamespaceMapping: namespaceMapping,
		BackupRef: &api.ObjectRef{
			Name: backupName,
			Uid:  bkpUid,
		},
	}
	_, err := backupDriver.CreateRestore(ctx, createRestoreReq)
	if err != nil {
		return nil, err
	}
	backupScheduleInspectRequest := &api.RestoreInspectRequest{
		OrgId: orgID,
		Name:  restoreName,
	}
	resp, err := backupDriver.InspectRestore(ctx, backupScheduleInspectRequest)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateRestoreWithValidation creates restore, waits and checks for success and validates the backup
func CreateRestoreWithValidation(ctx context1.Context, restoreName, backupName string, namespaceMapping, storageClassMapping map[string]string, clusterName string, orgID string, scheduledAppContexts []*scheduler.Context) error {
	startTime := time.Now()
	err := CreateRestore(restoreName, backupName, namespaceMapping, clusterName, orgID, ctx, storageClassMapping)
	if err != nil {
		return err
	}
	originalClusterConfigPath := CurrentClusterConfigPath
	if clusterConfigPath, ok := ClusterConfigPathMap[clusterName]; !ok {
		err = fmt.Errorf("switching cluster context: couldn't find clusterConfigPath for cluster [%s]", clusterName)
		return err
	} else {
		log.InfoD("Switching cluster context to cluster [%s]", clusterName)
		err = SetClusterContext(clusterConfigPath)
		if err != nil {
			return err
		}
	}
	defer func() {
		log.InfoD("Switching cluster context back to cluster path [%s]", originalClusterConfigPath)
		err := SetClusterContext(originalClusterConfigPath)
		if err != nil {
			log.FailOnError(err, "Failed switching cluster context back to cluster path [%s]", originalClusterConfigPath)
		}
	}()
	expectedRestoredAppContexts := make([]*scheduler.Context, 0)
	for _, scheduledAppContext := range scheduledAppContexts {
		expectedRestoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, namespaceMapping, storageClassMapping, true)
		if err != nil {
			log.Errorf("TransformAppContextWithMappings: %v", err)
			continue
		}
		expectedRestoredAppContexts = append(expectedRestoredAppContexts, expectedRestoredAppContext)
	}
	err = ValidateRestore(ctx, restoreName, orgID, expectedRestoredAppContexts, make([]string, 0))
	if err != nil {
		return err
	}

	log.Infof("Namespace mapping from CreateRestoreWithValidation [%v]", namespaceMapping)
	err = ValidateDataAfterRestore(expectedRestoredAppContexts, restoreName, ctx, backupName, namespaceMapping, startTime)

	return err
}

func GetSizeOfMountPoint(podName string, namespace string, kubeConfigFile string, volumeMount string, containerName ...string) (int, error) {
	var number int
	var str string
	output, err := kubectlExec([]string{fmt.Sprintf("--kubeconfig=%v", kubeConfigFile), "exec", "-it", podName, "-n", namespace, "--", "/bin/df"})
	if err != nil {
		return 0, err
	}
	for _, line := range strings.SplitAfter(output, "\n") {
		if strings.Contains(line, volumeMount) {
			str = strings.Fields(line)[3]
			break
		}
	}
	if str == "" {
		log.Infof("Could not find any mount points for the volume mount [%s] in the pod [%s] in namespace [%s] ", volumeMount, podName, namespace)
		log.Infof("Trying to check if there is a sym link for [%s]", volumeMount)
		if len(containerName) == 0 {
			return number, err
		}
		symlinkPath, err := core.Instance().RunCommandInPod([]string{"readlink", "-f", volumeMount}, podName, containerName[0], namespace)
		if err != nil {
			return number, err
		}
		if symlinkPath == "" {
			return 0, fmt.Errorf("no matching symlink for path [%s] was found in the pod [%s] in namespace [%s]", volumeMount, podName, namespace)
		} else {
			log.Infof("Symlink for volume mount [%s] found - [%s]", volumeMount, symlinkPath)
		}
		for _, line := range strings.SplitAfter(output, "\n") {
			if strings.Contains(line, symlinkPath) {
				str = strings.Fields(line)[3]
				break
			}
		}
	}
	if str != "" {
		number, err = strconv.Atoi(str)
		return number, err
	}
	return 0, fmt.Errorf("no matching volume mount with path [%s] was found in the pod [%s] in namespace [%s]", volumeMount, podName, namespace)
}

func kubectlExec(arguments []string) (string, error) {
	if len(arguments) == 0 {
		return "", fmt.Errorf("no arguments supplied for kubectl command")
	}
	cmd := exec.Command("kubectl", arguments...)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("error on executing kubectl command, Err: %+v", err)
	}
	return string(output), err
}

func CreateUsers(numberOfUsers int) []string {
	users := make([]string, 0)
	log.InfoD("Creating %d users", numberOfUsers)
	var wg sync.WaitGroup
	for i := 1; i <= numberOfUsers; i++ {
		userName := fmt.Sprintf("tp-user%d-%s", i, RandomString(4))
		firstName := fmt.Sprintf("FirstName%d", i)
		lastName := fmt.Sprintf("LastName%d", i)
		email := fmt.Sprintf("%s@cnbu.com", userName)
		wg.Add(1)
		go func(userName, firstName, lastName, email string) {
			defer GinkgoRecover()
			defer wg.Done()
			err := backup.AddUser(userName, firstName, lastName, email, CommonPassword)
			Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Creating user - %s", userName))
			users = append(users, userName)
		}(userName, firstName, lastName, email)
	}
	wg.Wait()
	return users
}

// CleanupCloudSettingsAndClusters removes the backup location(s), cloud accounts and source/destination clusters for the given context
func CleanupCloudSettingsAndClusters(backupLocationMap map[string]string, credName string, cloudCredUID string, ctx context1.Context) {
	log.InfoD("Cleaning backup locations in map [%v], cloud credential [%s], source [%s] and destination [%s] cluster", backupLocationMap, credName, SourceClusterName, DestinationClusterName)
	if len(backupLocationMap) != 0 {
		for backupLocationUID, bkpLocationName := range backupLocationMap {
			// Delete the backup location object
			err := DeleteBackupLocationWithContext(bkpLocationName, backupLocationUID, BackupOrgID, true, ctx)
			Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of backup location [%s]", bkpLocationName))
			backupLocationDeleteStatusCheck := func() (interface{}, bool, error) {
				status, err := IsBackupLocationPresent(bkpLocationName, ctx, BackupOrgID)
				if err != nil {
					return "", true, fmt.Errorf("backup location %s still present with error %v", bkpLocationName, err)
				}
				if status {
					backupLocationInspectRequest := api.BackupLocationInspectRequest{
						Name:  bkpLocationName,
						Uid:   backupLocationUID,
						OrgId: BackupOrgID,
					}
					backupLocationObject, err := Inst().Backup.InspectBackupLocation(ctx, &backupLocationInspectRequest)
					if err != nil {
						return "", true, fmt.Errorf("inspect backup location - backup location %s still present with error %v", bkpLocationName, err)
					}
					backupLocationStatus := backupLocationObject.BackupLocation.BackupLocationInfo.GetStatus()
					return "", true, fmt.Errorf("backup location %s is not deleted yet. Status - [%s]", bkpLocationName, backupLocationStatus)
				}
				return "", false, nil
			}
			_, err = task.DoRetryWithTimeout(backupLocationDeleteStatusCheck, BackupLocationDeleteTimeout, BackupLocationDeleteRetryTime)
			Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup location deletion status %s", bkpLocationName))
		}
		status, err := IsCloudCredPresent(credName, ctx, BackupOrgID)
		Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Verifying if cloud cred [%s] is present", credName))
		if status {
			err = DeleteCloudCredentialWithContext(credName, BackupOrgID, cloudCredUID, ctx)
			Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of cloud cred [%s]", credName))
			cloudCredDeleteStatus := func() (interface{}, bool, error) {
				status, err = IsCloudCredPresent(credName, ctx, BackupOrgID)
				if err != nil {
					return "", true, fmt.Errorf("cloud cred %s still present with error %v", credName, err)
				}
				if status {
					return "", true, fmt.Errorf("cloud cred %s is not deleted yet", credName)
				}
				return "", false, nil
			}
			_, err = task.DoRetryWithTimeout(cloudCredDeleteStatus, CloudAccountDeleteTimeout, CloudAccountDeleteRetryTime)
			Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cloud cred %s", credName))
		}
	}

	// Deleting clusters and the corresponding cloud cred
	enumerateClusterRequest := &api.ClusterEnumerateRequest{
		OrgId: BackupOrgID,
	}
	enumerateClusterResponse, err := Inst().Backup.EnumerateAllCluster(ctx, enumerateClusterRequest)
	Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Verifying enumerate cluster in organization %s", BackupOrgID))
	for _, clusterObj := range enumerateClusterResponse.GetClusters() {
		clusterProvider := GetClusterProviders()
		for _, provider := range clusterProvider {
			var clusterCredName, clusterCredUID string
			switch provider {
			case drivers.ProviderRke:
				if clusterObj.PlatformCredentialRef != nil {
					clusterCredName = clusterObj.PlatformCredentialRef.Name
					clusterCredUID = clusterObj.PlatformCredentialRef.Uid
				} else {
					log.Warnf("the platform credential ref of the cluster [%s] is nil", clusterObj.GetName())
				}
			default:
				if clusterObj.CloudCredentialRef != nil {
					clusterCredName = clusterObj.CloudCredentialRef.Name
					clusterCredUID = clusterObj.CloudCredentialRef.Uid
				} else {
					log.Warnf("the cloud credential ref of the cluster [%s] is nil", clusterObj.GetName())
				}
			}
			err = DeleteClusterWithUID(clusterObj.GetName(), clusterObj.GetUid(), BackupOrgID, ctx, true)
			Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", clusterObj.GetName()))
			if clusterCredName != "" {
				err = DeleteCloudCredential(clusterCredName, BackupOrgID, clusterCredUID)
				Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of cluster cloud cred [%s]", clusterCredName))
			}
		}
	}
}

// AddRoleAndAccessToUsers assigns role and access level to the users
// AddRoleAndAccessToUsers return map whose key is userRoleAccess and value is backup for each user
func AddRoleAndAccessToUsers(users []string, backupNames []string) (map[UserRoleAccess]string, error) {
	var access BackupAccess
	var role backup.PxBackupRole
	roleAccessUserBackupContext := make(map[UserRoleAccess]string)
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(users); i++ {
		userIndex := i % 9
		switch userIndex {
		case 0:
			access = ViewOnlyAccess
			role = backup.ApplicationOwner
		case 1:
			access = RestoreAccess
			role = backup.ApplicationOwner
		case 2:
			access = FullAccess
			role = backup.ApplicationOwner
		case 3:
			access = ViewOnlyAccess
			role = backup.ApplicationUser
		case 4:
			access = RestoreAccess
			role = backup.ApplicationUser
		case 5:
			access = FullAccess
			role = backup.ApplicationUser
		case 6:
			access = ViewOnlyAccess
			role = backup.InfrastructureOwner
		case 7:
			access = RestoreAccess
			role = backup.InfrastructureOwner
		case 8:
			access = FullAccess
			role = backup.InfrastructureOwner
		default:
			access = ViewOnlyAccess
			role = backup.ApplicationOwner
		}
		ctxNonAdmin, err := backup.GetNonAdminCtx(users[i], CommonPassword)
		if err != nil {
			return nil, err
		}
		userRoleAccessContext := UserRoleAccess{users[i], role, access, ctxNonAdmin}
		roleAccessUserBackupContext[userRoleAccessContext] = backupNames[i]
		err = backup.AddRoleToUser(users[i], role, "Adding role to user")
		if err != nil {
			err = fmt.Errorf("failed to add role %s to user %s with err %v", role, users[i], err)
			return nil, err
		}
		err = ShareBackup(backupNames[i], nil, []string{users[i]}, access, ctx)
		if err != nil {
			return nil, err
		}
		log.Infof(" Backup %s shared with user %s", backupNames[i], users[i])
	}
	return roleAccessUserBackupContext, nil
}
func ValidateSharedBackupWithUsers(user string, access BackupAccess, backupName string, restoreName string) {
	ctx, err := backup.GetAdminCtxFromSecret()
	Inst().Dash.VerifyFatal(err, nil, "Fetching px-central-admin ctx")
	userCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
	Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching %s user ctx", user))
	log.InfoD("Registering Source and Destination clusters from user context")
	err = CreateApplicationClusters(BackupOrgID, "", "", userCtx)
	Inst().Dash.VerifyFatal(err, nil, "Creating source and destination cluster")
	log.InfoD("Validating if user [%s] with access [%v] can restore and delete backup %s or not", user, BackupAccessKeyValue[access], backupName)
	backupDriver := Inst().Backup
	switch access {
	case ViewOnlyAccess:
		// Try restore with user having ViewOnlyAccess and it should fail
		err := CreateRestore(restoreName, backupName, make(map[string]string), DestinationClusterName, BackupOrgID, userCtx, make(map[string]string))
		log.Infof("The expected error returned is %v", err)
		Inst().Dash.VerifyFatal(strings.Contains(err.Error(), "failed to retrieve backup location"), true, "Verifying backup restore is not possible")
		// Try to delete the backup with user having ViewOnlyAccess, and it should not pass
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, BackupOrgID)
		Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Getting backup UID for- %s", backupName))
		// Delete backup to confirm that the user has ViewOnlyAccess and cannot delete backup
		_, err = DeleteBackup(backupName, backupUID, BackupOrgID, userCtx)
		log.Infof("The expected error returned is %v", err)
		Inst().Dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")

	case RestoreAccess:
		// Try restore with user having RestoreAccess and it should pass
		err := CreateRestore(restoreName, backupName, make(map[string]string), DestinationClusterName, BackupOrgID, userCtx, make(map[string]string))
		Inst().Dash.VerifyFatal(err, nil, "Verifying that restore is possible")
		// Try to delete the backup with user having RestoreAccess, and it should not pass
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, BackupOrgID)
		Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Getting backup UID for- %s", backupName))
		// Delete backup to confirm that the user has Restore Access and delete backup should fail
		_, err = DeleteBackup(backupName, backupUID, BackupOrgID, userCtx)
		log.Infof("The expected error returned is %v", err)
		Inst().Dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")

	case FullAccess:
		// Try restore with user having FullAccess, and it should pass
		err := CreateRestore(restoreName, backupName, make(map[string]string), DestinationClusterName, BackupOrgID, userCtx, make(map[string]string))
		Inst().Dash.VerifyFatal(err, nil, "Verifying that restore is possible")
		// Try to delete the backup with user having FullAccess, and it should pass
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, BackupOrgID)
		Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Getting backup UID for- %s", backupName))
		// Delete backup to confirm that the user has Full Access
		_, err = DeleteBackup(backupName, backupUID, BackupOrgID, userCtx)
		Inst().Dash.VerifyFatal(err, nil, "Verifying that delete backup is possible")
	}
}

func GetEnv(environmentVariable string, defaultValue string) string {
	value, present := os.LookupEnv(environmentVariable)
	if !present {
		value = defaultValue
	}
	return value
}

// ShareBackupWithUsersAndAccessAssignment shares backup with multiple users with different access levels
// It returns a map with key as UserAccessContext and value as backup shared
func ShareBackupWithUsersAndAccessAssignment(backupNames []string, users []string, ctx context1.Context) (map[UserAccessContext]string, error) {
	log.InfoD("Sharing backups with users with different access level")
	accessUserBackupContext := make(map[UserAccessContext]string)
	var err error
	var ctxNonAdmin context1.Context
	var access BackupAccess
	for i, user := range users {
		userIndex := i % 3
		switch userIndex {
		case 0:
			access = ViewOnlyAccess
			err = ShareBackup(backupNames[i], nil, []string{user}, access, ctx)
		case 1:
			access = RestoreAccess
			err = ShareBackup(backupNames[i], nil, []string{user}, access, ctx)
		case 2:
			access = FullAccess
			err = ShareBackup(backupNames[i], nil, []string{user}, access, ctx)
		default:
			access = ViewOnlyAccess
			err = ShareBackup(backupNames[i], nil, []string{user}, access, ctx)
		}
		if err != nil {
			return accessUserBackupContext, fmt.Errorf("unable to share backup %s with user %s Error: %v", backupNames[i], user, err)
		}
		ctxNonAdmin, err = backup.GetNonAdminCtx(users[i], CommonPassword)
		if err != nil {
			return accessUserBackupContext, fmt.Errorf("unable to get user context: %v", err)
		}
		accessContextUser := UserAccessContext{users[i], access, ctxNonAdmin}
		accessUserBackupContext[accessContextUser] = backupNames[i]
	}
	return accessUserBackupContext, nil
}

// GetAllBackupsAdmin returns all the backups that px-central-admin has access to
func GetAllBackupsAdmin() ([]string, error) {
	var bkp *api.BackupObject
	backupNames := make([]string, 0)
	backupDriver := Inst().Backup
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return nil, err
	}
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: BackupOrgID}
	curBackups, err := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	if err != nil {
		return nil, err
	}
	for _, bkp = range curBackups.GetBackups() {
		backupNames = append(backupNames, bkp.GetName())
	}
	return backupNames, nil
}

// GetAllRestoresAdmin returns all the backups that px-central-admin has access to
func GetAllRestoresAdmin() ([]string, error) {
	restoreNames := make([]string, 0)
	backupDriver := Inst().Backup
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return restoreNames, err
	}

	restoreEnumerateRequest := &api.RestoreEnumerateRequest{
		OrgId: BackupOrgID,
	}
	restoreResponse, err := backupDriver.EnumerateRestore(ctx, restoreEnumerateRequest)
	if err != nil {
		return restoreNames, err
	}
	for _, restore := range restoreResponse.GetRestores() {
		restoreNames = append(restoreNames, restore.Name)
	}
	return restoreNames, nil
}

func GenerateEncryptionKey() string {
	var lower = []byte("abcdefghijklmnopqrstuvwxyz")
	var upper = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	var number = []byte("0123456789")
	var special = []byte("~=+%^*/()[]{}/!@#$?|")
	allChar := append(lower, upper...)
	allChar = append(allChar, number...)
	allChar = append(allChar, special...)

	b := make([]byte, 12)
	// select 1 upper, 1 lower, 1 number and 1 special
	b[0] = lower[rand.Intn(len(lower))]
	b[1] = upper[rand.Intn(len(upper))]
	b[2] = number[rand.Intn(len(number))]
	b[3] = special[rand.Intn(len(special))]
	for i := 4; i < 12; i++ {
		// randomly select 1 character from given charset
		b[i] = allChar[rand.Intn(len(allChar))]
	}

	//shuffle character
	rand.Shuffle(len(b), func(i, j int) {
		b[i], b[j] = b[j], b[i]
	})

	return string(b)
}

func GetScheduleUID(scheduleName string, orgID string, ctx context1.Context) (string, error) {
	backupDriver := Inst().Backup
	backupScheduleInspectRequest := &api.BackupScheduleInspectRequest{
		Name:  scheduleName,
		Uid:   "",
		OrgId: orgID,
	}
	resp, err := backupDriver.InspectBackupSchedule(ctx, backupScheduleInspectRequest)
	if err != nil {
		return "", err
	}
	scheduleUid := resp.GetBackupSchedule().GetUid()
	return scheduleUid, err
}

func RemoveStringItemFromSlice(itemList []string, item []string) []string {
	sort.Sort(sort.StringSlice(itemList))
	for _, element := range item {
		index := sort.StringSlice(itemList).Search(element)
		itemList = append(itemList[:index], itemList[index+1:]...)
	}
	return itemList
}

func removeIntItemFromSlice(itemList []int, item []int) []int {
	sort.Sort(sort.IntSlice(itemList))
	for _, element := range item {
		index := sort.IntSlice(itemList).Search(element)
		itemList = append(itemList[:index], itemList[index+1:]...)
	}
	return itemList
}

func GetAllBackupLocations(ctx context1.Context) (map[string]string, error) {
	backupLocationMap := make(map[string]string, 0)
	backupDriver := Inst().Backup
	backupLocationEnumerateRequest := &api.BackupLocationEnumerateRequest{
		OrgId: BackupOrgID,
	}
	response, err := backupDriver.EnumerateBackupLocation(ctx, backupLocationEnumerateRequest)
	if err != nil {
		return backupLocationMap, err
	}
	if len(response.BackupLocations) > 0 {
		for _, backupLocation := range response.BackupLocations {
			backupLocationMap[backupLocation.Uid] = backupLocation.Name
		}
		log.Infof("The backup locations and their UID are %v", backupLocationMap)
	} else {
		log.Info("No backup locations found")
	}
	return backupLocationMap, nil
}

func GetAllCloudCredentials(ctx context1.Context) (map[string]string, error) {
	cloudCredentialMap := make(map[string]string, 0)
	backupDriver := Inst().Backup
	cloudCredentialEnumerateRequest := &api.CloudCredentialEnumerateRequest{
		OrgId: BackupOrgID,
	}
	response, err := backupDriver.EnumerateCloudCredential(ctx, cloudCredentialEnumerateRequest)
	if err != nil {
		return cloudCredentialMap, err
	}
	if len(response.CloudCredentials) > 0 {
		for _, cloudCredential := range response.CloudCredentials {
			cloudCredentialMap[cloudCredential.Uid] = cloudCredential.Name
		}
		log.Infof("The cloud credentials and their UID are %v", cloudCredentialMap)
	} else {
		log.Info("No cloud credentials found")
	}
	return cloudCredentialMap, nil
}

func GetAllRestoresNonAdminCtx(ctx context1.Context) ([]string, error) {
	restoreNames := make([]string, 0)
	backupDriver := Inst().Backup
	restoreEnumerateRequest := &api.RestoreEnumerateRequest{
		OrgId: BackupOrgID,
	}
	restoreResponse, err := backupDriver.EnumerateRestore(ctx, restoreEnumerateRequest)
	if err != nil {
		return restoreNames, err
	}
	for _, restore := range restoreResponse.GetRestores() {
		restoreNames = append(restoreNames, restore.Name)
	}
	return restoreNames, nil
}

// DeletePodWithWithoutLabelInNamespace kills pod with the given label in the given namespace or skip pod with the given label
// and delete all pods
func DeletePodWithWithoutLabelInNamespace(namespace string, label map[string]string, ignoreLabel bool) error {
	var pods *corev1.PodList
	var err error
	// TODO: Revisit this function and remove the below code if not needed
	podList := func() (interface{}, bool, error) {
		if ignoreLabel {
			nolabel := make(map[string]string)
			pods, err = core.Instance().GetPods(namespace, nolabel)
		} else {
			pods, err = core.Instance().GetPods(namespace, label)
		}
		if err != nil {
			if strings.Contains(err.Error(), "no pod found with the label") {
				return "", true, fmt.Errorf("waiting for pod with the given label %v to come up in namespace %s", label, namespace)
			} else {
				return "", false, err
			}
		}
		if len(pods.Items) < 1 {
			return "", true, fmt.Errorf("waiting for atleast one pod with the given label %v to come up in namespace %s", label, namespace)
		}
		return "", false, nil
	}
	_, err = DoRetryWithTimeoutWithGinkgoRecover(podList, 5*time.Minute, 30*time.Second)
	if err != nil {
		return err
	}

	// fetch the newest set of pods post wait for pods to come up
	if ignoreLabel {
		nolabel := make(map[string]string)
		pods, err = core.Instance().GetPods(namespace, nolabel)
	} else {
		pods, err = core.Instance().GetPods(namespace, label)
	}
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		skipPod := false
		if ignoreLabel {
			for key, podlabel := range pod.GetLabels() {
				if podlabel2, exists := label[key]; exists && podlabel == podlabel2 {
					skipPod = true
					break
				}
			}
			if skipPod {
				break
			}
		}

		log.Infof("Deleting pod %s with label %v", pod.GetName(), label)
		err = core.Instance().DeletePod(pod.GetName(), namespace, false)
		if err != nil {
			return err
		}
		err = core.Instance().WaitForPodDeletion(pod.GetUID(), namespace, 5*time.Minute)
		if err != nil {
			return err
		}
	}
	return nil
}

// backupSuccessCheck inspects backup task
func BackupSuccessCheck(backupName string, orgID string, retryDuration time.Duration, retryInterval time.Duration, ctx context1.Context) error {
	bkpUid, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   bkpUid,
		OrgId: orgID,
	}
	statusesExpected := [...]api.BackupInfo_StatusInfo_Status{
		api.BackupInfo_StatusInfo_Success,
	}
	statusesUnexpected := [...]api.BackupInfo_StatusInfo_Status{
		api.BackupInfo_StatusInfo_Invalid,
		api.BackupInfo_StatusInfo_Aborted,
		api.BackupInfo_StatusInfo_Failed,
	}
	backupSuccessCheckFunc := func() (interface{}, bool, error) {
		resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
		if err != nil {
			return "", false, err
		}
		actual := resp.GetBackup().GetStatus().Status
		reason := resp.GetBackup().GetStatus().Reason
		for _, status := range statusesExpected {
			if actual == status {
				return "", false, nil
			}
		}
		for _, status := range statusesUnexpected {
			if actual == status {
				return "", false, fmt.Errorf("backup status for [%s] expected was [%s] but got [%s] because of [%s]", backupName, statusesExpected, actual, reason)
			}
		}

		return "", true, fmt.Errorf("backup status for [%s] expected was [%s] but got [%s] because of [%s]", backupName, statusesExpected, actual, reason)

	}
	_, err = task.DoRetryWithTimeout(backupSuccessCheckFunc, retryDuration, retryInterval)
	log.InfoD("Backup success check for backup %s finished at [%s]", backupName, time.Now().Format("2006-01-02 15:04:05"))
	if err != nil {
		return err
	}
	return nil
}

// BackupSuccessCheckWithValidation checks if backup is Success and then validates the backup
func BackupSuccessCheckWithValidation(ctx context1.Context, backupName string, scheduledAppContextsToBackup []*scheduler.Context, orgID string, retryDuration time.Duration, retryInterval time.Duration, resourceTypeFilter ...string) error {
	err := BackupSuccessCheck(backupName, orgID, retryDuration, retryInterval, ctx)
	if err != nil {
		return err
	}
	return ValidateBackup(ctx, backupName, orgID, scheduledAppContextsToBackup, resourceTypeFilter)
}

// ValidateBackup validates a backup's spec's objects (resources) and volumes. resourceTypesFilter can be used to select specific types to validate (nil means all types). This function must be called after switching to the context on which `scheduledAppContexts` exists. Cluster level resources aren't validated.
func ValidateBackup(ctx context1.Context, backupName string, orgID string, scheduledAppContexts []*scheduler.Context, resourceTypesFilter []string) error {
	var backupInspectResponse *api.BackupInspectResponse
	log.InfoD("Validating backup [%s] in org [%s]", backupName, orgID)
	log.Infof("Obtaining backup info for backup [%s]", backupName)
	backupDriver := Inst().Backup
	backupUid, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return fmt.Errorf("GetBackupUID Err: %v", err)
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUid,
		OrgId: orgID,
	}
	backupStatusCheck := func() (interface{}, bool, error) {
		backupInspectResponse, err = backupDriver.InspectBackup(ctx, backupInspectRequest)
		if err != nil {
			return "", false, fmt.Errorf("InspectBackup Err: %v", err)
		}
		backupStatus := backupInspectResponse.GetBackup().GetStatus().Status
		if backupStatus == api.BackupInfo_StatusInfo_Success ||
			backupStatus == api.BackupInfo_StatusInfo_PartialSuccess {
			return "", false, nil
		}
		return "", true, fmt.Errorf("ValidateBackup requires backup [%s] to have a status of Success or PartialSuccess,got -%v", backupName, backupStatus)
	}
	_, err = DoRetryWithTimeoutWithGinkgoRecover(backupStatusCheck, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
	if err != nil {
		return err
	}
	// Check size of backup taken is non-zero
	resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return err
	}
	Volumes := resp.GetBackup().GetVolumes()
	if len(Volumes) > 0 {
		for _, volume := range Volumes {
			size := volume.GetTotalSize()
			actualSize := volume.GetActualSize()
			if !(size > 0 || actualSize > 0) {
				return fmt.Errorf("backup size for [%s] is [%d] and actual size is [%d] which is not greater than 0 ", backupName, size, actualSize)
			}
		}
	}

	var errors []error
	theBackup := backupInspectResponse.GetBackup()
	backupName = theBackup.GetName()
	resourceInfos := theBackup.GetResources()
	backedUpVolumes := theBackup.GetVolumes()
	backupNamespaces := theBackup.GetNamespaces()

	for _, scheduledAppContext := range scheduledAppContexts {

		scheduledAppContextNamespace := scheduledAppContext.ScheduleOptions.Namespace
		log.InfoD("Validating specs for the namespace (scheduledAppContext) [%s] in backup [%s]", scheduledAppContextNamespace, backupName)

		if !Contains(backupNamespaces, scheduledAppContextNamespace) {
			err := fmt.Errorf("the namespace (scheduledAppContext) [%s] provided to the ValidateBackup, is not present in the backup [%s]", scheduledAppContextNamespace, backupName)
			errors = append(errors, err)
			continue
		}

		// collect the backup resources whose specs should be present in this scheduledAppContext (namespace)
		resourceInfoBackupObjs := make([]*api.ResourceInfo, 0)
		for _, resource := range resourceInfos {
			if resource.GetNamespace() == scheduledAppContextNamespace {
				resourceInfoBackupObjs = append(resourceInfoBackupObjs, resource)
			}
		}

		var updatedSpec interface{}
		var name, kind, ns string
	specloop:
		for _, spec := range scheduledAppContext.App.SpecList {

			if tektonspec, ok := spec.(*tektoncdv1.Task); ok {
				updatedSpec, err = k8s.GetUpdatedSpec(tektonspec)
				if err != nil {
					err := fmt.Errorf("error in GetSpecNameKindNamepace: [%s] in namespace (appCtx) [%s], spec: [%+v]", err, scheduledAppContextNamespace, spec)
					errors = append(errors, err)
					continue specloop
				}
			} else if tektonpipelinespec, ok := spec.(*tektoncdv1.Pipeline); ok {
				updatedSpec, err = k8s.GetUpdatedSpec(tektonpipelinespec)
				if err != nil {
					err = fmt.Errorf("error in GetUpdatedSpec: [%s] in namespace (appCtx) [%s], spec: [%+v]", err, scheduledAppContextNamespace, spec)
					errors = append(errors, err)
					continue specloop
				}
			} else if tektonpipelinerunspec, ok := spec.(*tektoncdv1.PipelineRun); ok {
				updatedSpec, err = k8s.GetUpdatedSpec(tektonpipelinerunspec)
				if err != nil {
					err = fmt.Errorf("error in GetUpdatedSpec: [%s] in namespace (appCtx) [%s], spec: [%+v]", err, scheduledAppContextNamespace, spec)
					errors = append(errors, err)
					continue specloop
				}
			} else if tektontaskrunspec, ok := spec.(*tektoncdv1.TaskRun); ok {
				updatedSpec, err = k8s.GetUpdatedSpec(tektontaskrunspec)
				if err != nil {
					err = fmt.Errorf("error in GetUpdatedSpec: [%s] in namespace (appCtx) [%s], spec: [%+v]", err, scheduledAppContextNamespace, spec)
					errors = append(errors, err)
					continue specloop
				}
			}

			if updatedSpec != nil {
				name, kind, ns, err = GetSpecNameKindNamepace(updatedSpec)
			} else {
				name, kind, ns, err = GetSpecNameKindNamepace(spec)
			}
			if err != nil {
				err := fmt.Errorf("error in GetSpecNameKindNamepace: [%s] in namespace (appCtx) [%s], spec: [%+v]", err, scheduledAppContextNamespace, spec)
				errors = append(errors, err)
				continue specloop
			}

			if name == "" || kind == "" {
				err := fmt.Errorf("error: GetSpecNameKindNamepace returned values with Spec Name: [%s], Kind: [%s], Namespace: [%s], in local Context (NS): [%s], where some of the values are empty, so this spec will be ignored", name, kind, ns, scheduledAppContextNamespace)
				errors = append(errors, err)
				continue specloop
			}

			if kind == "StorageClass" || kind == "VolumeSnapshot" {
				// we don't backup "StorageClass"s and "VolumeSnapshot"s
				continue specloop
			}

			if len(resourceTypesFilter) > 0 && !Contains(resourceTypesFilter, kind) {
				log.Infof("kind: [%s] is not in resourceTypes [%v], so spec (name: [%s], kind: [%s], namespace: [%s]) in scheduledAppContext [%s] will not be checked for in backup [%s]", kind, resourceTypesFilter, name, kind, ns, scheduledAppContextNamespace, backupName)
				continue specloop
			}

			// we only validate namespace level resource
			if ns != "" {
				for _, backupObj := range resourceInfoBackupObjs {
					if name == backupObj.GetName() && kind == backupObj.GetKind() {
						continue specloop
					}
				}

				// The following error means that something was NOT backed up,
				// OR it wasn't supposed to be backed up, and we forgot to exclude the check.
				err := fmt.Errorf("the spec (name: [%s], kind: [%s], namespace: [%s]) found in the scheduledAppContext [%s], is not in the backup [%s]", name, kind, ns, scheduledAppContextNamespace, backupName)
				errors = append(errors, err)
				continue specloop
			}
		}

		log.InfoD("Validating backed up volumes for the namespace (scheduledAppContext) [%s] in backup [%s]", scheduledAppContextNamespace, backupName)

		// collect the backup resources whose VOLUMES should be present in this scheduledAppContext (namespace)
		namespacedBackedUpVolumes := make([]*api.BackupInfo_Volume, 0)
		for _, vol := range backedUpVolumes {
			if vol.GetNamespace() == scheduledAppContextNamespace {
				if vol.Status.Status != api.BackupInfo_StatusInfo_Success /*Can this also be partialsuccess?*/ {
					err := fmt.Errorf("the status of the backedup volume [%s] was not Success. It was [%s] with reason [%s]", vol.Name, vol.Status.Status, vol.Status.Reason)
					errors = append(errors, err)
				}
				namespacedBackedUpVolumes = append(namespacedBackedUpVolumes, vol)
			}
		}

		// Collect all volumes belonging to a context
		log.Infof("getting the volumes bounded to the PVCs in the namespace (scheduledAppContext) [%s]", scheduledAppContextNamespace)
		volumeMap := make(map[string]*volume.Volume)
		scheduledVolumes, err := Inst().S.GetVolumes(scheduledAppContext)
		if err != nil {
			err := fmt.Errorf("error in Inst().S.GetVolumes: [%s] in namespace (appCtx) [%s]", err, scheduledAppContextNamespace)
			errors = append(errors, err)
			continue
		}
		for _, scheduledVol := range scheduledVolumes {
			volumeMap[scheduledVol.ID] = scheduledVol
		}
		log.Infof("volumes bounded to the PVCs in the context [%s] are [%+v]", scheduledAppContextNamespace, scheduledVolumes)

		if len(resourceTypesFilter) == 0 ||
			(len(resourceTypesFilter) > 0 && Contains(resourceTypesFilter, "PersistentVolumeClaim")) {
			// Verify if volumes are present
		volloop:
			for _, spec := range scheduledAppContext.App.SpecList {
				// Obtaining the volume from the PVC
				pvcSpecObj, ok := spec.(*corev1.PersistentVolumeClaim)
				if !ok {
					continue volloop
				}

				updatedSpec, err := k8s.GetUpdatedSpec(pvcSpecObj)
				if err != nil {
					err := fmt.Errorf("unable to fetch updated version of PVC(name: [%s], namespace: [%s]) present in the context [%s]. Error: %v", pvcSpecObj.GetName(), pvcSpecObj.GetNamespace(), scheduledAppContextNamespace, err)
					errors = append(errors, err)
					continue volloop
				}

				pvcObj, ok := updatedSpec.(*corev1.PersistentVolumeClaim)
				if !ok {
					err := fmt.Errorf("unable to fetch updated version of PVC(name: [%s], namespace: [%s]) present in the context [%s]. Error: %v", pvcSpecObj.GetName(), pvcSpecObj.GetNamespace(), scheduledAppContextNamespace, err)
					errors = append(errors, err)
					continue volloop
				}

				scheduledVol, ok := volumeMap[pvcObj.Spec.VolumeName]
				if !ok {
					err := fmt.Errorf("unable to find the volume corresponding to PVC(name: [%s], namespace: [%s]) in the cluster corresponding to the PVC's context, which is [%s]", pvcSpecObj.GetName(), pvcSpecObj.GetNamespace(), scheduledAppContextNamespace)
					errors = append(errors, err)
					continue volloop
				}

				// Finding the volume in the backup
				for _, backedupVol := range namespacedBackedUpVolumes {
					if backedupVol.GetName() == scheduledVol.ID {

						if backedupVol.Pvc != pvcObj.Name {
							err := fmt.Errorf("the PVC of the volume as per the backup [%s] is [%s], but the one found in the scheduled namesapce is [%s]", backedupVol.GetName(), backedupVol.Pvc, pvcObj.Name)
							errors = append(errors, err)
						}

						var expectedVolumeDriver string
						switch strings.ToLower(os.Getenv("BACKUP_TYPE")) {
						case string(NativeCSIWithOffloadToS3):
							expectedVolumeDriver = "kdmp"
						case string(NativeCSI):
							expectedVolumeDriver = "csi"
						case string(NativeAzure):
							expectedVolumeDriver = "azure"
						case string(DirectKDMP):
							expectedVolumeDriver = "kdmp"
						default:
							expectedVolumeDriver = Inst().V.String()
						}

						if backedupVol.DriverName != expectedVolumeDriver {
							err := fmt.Errorf("the Driver Name of the volume as per the backup [%s] is [%s], but the one expected is [%s]", backedupVol.GetName(), backedupVol.DriverName, expectedVolumeDriver)
							errors = append(errors, err)
						}

						if backedupVol.StorageClass != *pvcObj.Spec.StorageClassName {
							switch strings.ToLower(os.Getenv("BACKUP_TYPE")) {
							case string(NativeCSI):
								log.Infof("in case of native CSI backup volumes in backup object is not updated with storage class")
							case string(NativeAzure):
								log.Infof("in case of native azure backup volumes in backup object is not updated with storage class")
							default:
								err := fmt.Errorf("the Storage Class of the volume as per the backup [%s] is [%s], but the one found in the scheduled namesapce is [%s]", backedupVol.GetName(), backedupVol.StorageClass, *pvcObj.Spec.StorageClassName)
								errors = append(errors, err)
							}
						}

						continue volloop
					}
				}

				// The following error means that something WAS not backed up, OR it wasn't supposed to be backed up, and we forgot to exclude the check.
				err = fmt.Errorf("the volume [%s] corresponding to PVC(name: [%s], namespace: [%s]) was present in the cluster with the namespace containing that PVC, but the volume was not in the backup [%s]", pvcObj.Spec.VolumeName, pvcObj.GetName(), pvcObj.GetNamespace(), backupName)
				errors = append(errors, err)
			}
		} else {
			log.Infof("volumes in scheduledAppContext [%s] will not be checked for in backup [%s] as PersistentVolumeClaims are not backed up", scheduledAppContextNamespace, backupName)
		}

	}

	errStrings := make([]string, 0)
	for _, err := range errors {
		if err != nil {
			errStrings = append(errStrings, err.Error())
		}
	}

	if len(errStrings) > 0 {
		return fmt.Errorf("ValidateBackup Errors: {%s}", strings.Join(errStrings, "}\n{"))
	}

	err = validateCRCleanup(theBackup, ctx)

	return err
}

// restoreSuccessCheck inspects restore task to check for status being "success". NOTE: If the status is different, it retries every `retryInterval` for `retryDuration` before returning `err`
func RestoreSuccessCheck(restoreName string, orgID string, retryDuration time.Duration, retryInterval time.Duration, ctx context1.Context) error {
	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}
	statusesExpected := [...]api.RestoreInfo_StatusInfo_Status{
		api.RestoreInfo_StatusInfo_PartialSuccess,
		api.RestoreInfo_StatusInfo_Success,
	}
	statusesUnexpected := [...]api.RestoreInfo_StatusInfo_Status{
		api.RestoreInfo_StatusInfo_Invalid,
		api.RestoreInfo_StatusInfo_Aborted,
		api.RestoreInfo_StatusInfo_Failed,
	}
	restoreSuccessCheckFunc := func() (interface{}, bool, error) {
		resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
		if err != nil {
			return "", false, err
		}
		actual := resp.GetRestore().GetStatus().Status
		reason := resp.GetRestore().GetStatus().Reason

		for _, status := range statusesExpected {
			if actual == status {
				return "", false, nil
			}
		}
		for _, status := range statusesUnexpected {
			if actual == status {
				return "", false, fmt.Errorf("restore status for [%s] expected was [%v] but got [%s] because of [%s]", restoreName, statusesExpected, actual, reason)
			}
		}
		return "", true, fmt.Errorf("restore status for [%s] expected was [%v] but got [%s] because of [%s]", restoreName, statusesExpected, actual, reason)
	}
	_, err := task.DoRetryWithTimeout(restoreSuccessCheckFunc, retryDuration, retryInterval)
	log.InfoD("Restore success check for restore : %s finished at [%s]", restoreName, time.Now().Format("2006-01-02 15:04:05"))

	if err != nil {
		return err
	}
	return nil
}

// restoreSuccessWithReplacePolicy inspects restore task status as per ReplacePolicyType
func restoreSuccessWithReplacePolicy(restoreName string, orgID string, retryDuration time.Duration, retryInterval time.Duration, ctx context1.Context, replacePolicy ReplacePolicyType) error {
	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}
	var statusesExpected api.RestoreInfo_StatusInfo_Status
	if replacePolicy == ReplacePolicyDelete {
		statusesExpected = api.RestoreInfo_StatusInfo_Success
	} else if replacePolicy == ReplacePolicyRetain {
		statusesExpected = api.RestoreInfo_StatusInfo_PartialSuccess
	}
	statusesUnexpected := [...]api.RestoreInfo_StatusInfo_Status{
		api.RestoreInfo_StatusInfo_Invalid,
		api.RestoreInfo_StatusInfo_Aborted,
		api.RestoreInfo_StatusInfo_Failed,
	}
	restoreSuccessCheckFunc := func() (interface{}, bool, error) {
		resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
		if err != nil {
			return "", false, err
		}
		actual := resp.GetRestore().GetStatus().Status
		reason := resp.GetRestore().GetStatus().Reason
		if actual == statusesExpected {
			return "", false, nil
		}

		for _, status := range statusesUnexpected {
			if actual == status {
				return "", false, fmt.Errorf("restore status for [%s] expected was [%v] but got [%s] because of [%s]", restoreName, statusesExpected, actual, reason)
			}
		}
		return "", true, fmt.Errorf("restore status for [%s] expected was [%v] but got [%s] because of [%s]", restoreName, statusesExpected, actual, reason)
	}
	_, err := task.DoRetryWithTimeout(restoreSuccessCheckFunc, retryDuration, retryInterval)
	return err
}

func ValidateDataAfterRestore(expectedRestoredAppContexts []*scheduler.Context, restoreName string, context context1.Context,
	backupName string, namespaceMapping map[string]string, startTime time.Time) error {
	var k8sCore = core.Instance()
	var allBackupNamespaces []string
	var dataBeforeBackup = make(map[string]map[string][][]string)
	var dataAfterBackup = make(map[string]map[string][][]string)

	var allRestoreHandlers []appDriver.ApplicationDriver
	var allErrors []string

	backupDriver := Inst().Backup
	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: BackupOrgID,
	}
	restoreInspectResponse, err := backupDriver.InspectRestore(context, restoreInspectRequest)
	if err != nil {
		return err
	}

	backupUid, err := backupDriver.GetBackupUID(context, backupName, BackupOrgID)

	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUid,
		OrgId: BackupOrgID,
	}
	backupInspectResponse, _ := backupDriver.InspectBackup(context, backupInspectRequest)
	theBackup := backupInspectResponse.GetBackup()
	allBackupNamespaces = theBackup.Namespaces
	theRestore := restoreInspectResponse.GetRestore()

	log.Infof("Backup UUID - [%s]", backupUid)
	// Fetching the Data commands from Source
	for _, eachBackupNamespace := range allBackupNamespaces {
		log.Infof("All Handler Namespaces - [%+v]", NamespaceAppWithDataMap)
		for _, eachHandler := range NamespaceAppWithDataMap[eachBackupNamespace] {
			beforeBackup := eachHandler.GetBackupData(backupUid)
			afterBackup := eachHandler.GetBackupData(fmt.Sprintf("%s%s", backupUid, dataAfterBackupSuffix))

			if restoreNamespace, ok := namespaceMapping[eachBackupNamespace]; ok {
				if beforeBackup != nil {
					log.InfoD("Data inserted before backup added for validation")
					if _, ok := dataBeforeBackup[restoreNamespace]; !ok {
						dataBeforeBackup[restoreNamespace] = make(map[string][][]string)
					}
					dataBeforeBackup[restoreNamespace][eachHandler.GetApplicationType()] = append(dataAfterBackup[restoreNamespace][eachHandler.GetApplicationType()], beforeBackup)
				}

				if afterBackup != nil {
					log.InfoD("Data inserted after backup added for validation")
					if _, ok := dataAfterBackup[restoreNamespace]; !ok {
						dataAfterBackup[restoreNamespace] = make(map[string][][]string)
					}
					dataAfterBackup[restoreNamespace][eachHandler.GetApplicationType()] = append(dataAfterBackup[restoreNamespace][eachHandler.GetApplicationType()], afterBackup)
				}
			} else {
				if beforeBackup != nil {
					log.InfoD("Data inserted before backup added for validation")
					if _, ok := dataBeforeBackup[eachBackupNamespace]; !ok {
						dataBeforeBackup[eachBackupNamespace] = make(map[string][][]string)
					}
					dataBeforeBackup[eachBackupNamespace][eachHandler.GetApplicationType()] = append(dataAfterBackup[restoreNamespace][eachHandler.GetApplicationType()], beforeBackup)
				}

				if afterBackup != nil {
					log.InfoD("Data inserted after backup added for validation")
					if _, ok := dataAfterBackup[eachBackupNamespace]; !ok {
						dataAfterBackup[eachBackupNamespace] = make(map[string][][]string)
					}
					dataAfterBackup[eachBackupNamespace][eachHandler.GetApplicationType()] = append(dataAfterBackup[restoreNamespace][eachHandler.GetApplicationType()], afterBackup)
				}
			}
		}
	}

	currentPodAge, err := GetBackupPodAge()
	if err != nil {
		return err
	}

	// Creating restore handlers
	log.Infof("Creating all restore app handlers")
	log.Infof("Namespace Mapping - [%+v]", namespaceMapping)
	for _, ctx := range expectedRestoredAppContexts {

		appInfo, err := appUtils.ExtractConnectionInfo(ctx, context)
		if err != nil {
			allErrors = append(allErrors, err.Error())
		}
		if appInfo.StartDataSupport {
			appHandler, _ := appDriver.GetApplicationDriver(
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
			if appInfo.AppType == appType.Kubevirt && appInfo.StartDataSupport {
				err = appHandler.WaitForVMToBoot()
				if err != nil {
					return fmt.Errorf("Unable to boot VM on destination. Error - [%s]", err.Error())
				}
			}
			pods, err := k8sCore.GetPods(appInfo.Namespace, make(map[string]string))
			if err != nil {
				return err
			}
			for _, eachPod := range pods.Items {
				// If the policy is set to retain and the pod is not modified we are skipping the data validation check
				if currentPodAge[appInfo.Namespace][eachPod.ObjectMeta.GetGenerateName()].Before(startTime) {
					log.Infof("Skipping %s from validation as the pod is not changed", eachPod.ObjectMeta.GetGenerateName())
					log.Infof("Current Restore Policy - [%s]", theRestore.ReplacePolicy.String())
					continue
				} else {
					log.Infof("App handler created for [%s] in namespace [%s]", appInfo.Hostname, appInfo.Namespace)
					allRestoreHandlers = append(allRestoreHandlers, appHandler)
				}
			}
		}
	}

	// Verifying data on restored pods
	for _, eachHandler := range allRestoreHandlers {
		// TODO: This needs to be fixed later in case of multiple apps in one namsespace
		if len(dataBeforeBackup) != 0 {
			log.InfoD("Validating data inserted before backup")
			err := verifyDataPresentInApp(eachHandler, dataBeforeBackup[eachHandler.GetNamespace()][eachHandler.GetApplicationType()], context)
			if err != nil {
				allErrors = append(allErrors, fmt.Sprintf("Data validation failed. Rows NOT found after restore. Error - [%s]", err.Error()))
			}
		} else {
			log.InfoD("Skipping data validation added before backup as no data was found")
		}

		if len(dataAfterBackup) != 0 {
			log.InfoD("Validating data inserted after backup")
			err := verifyDataPresentInApp(eachHandler, dataAfterBackup[eachHandler.GetNamespace()][eachHandler.GetApplicationType()], context)
			if err == nil {
				allErrors = append(allErrors, fmt.Sprintf("Data validation failed. Unexpected Rows found after restore. Rows Found - [%v]", dataAfterBackup[eachHandler.GetNamespace()][eachHandler.GetApplicationType()]))
			}
		} else {
			log.InfoD("Skipping data validation added after backup as no data was found")
		}
	}

	if len(allErrors) != 0 {
		return fmt.Errorf("Restore data validation failed - [%s]", strings.Join(allErrors, "\n"))
	}

	return nil
}

func verifyDataPresentInApp(appHandler appDriver.ApplicationDriver, dataExpected [][]string, appContext context1.Context) error {
	var isDataPresent = false
	var allErrorMessage []string
	for _, eachExpectedData := range dataExpected {
		err := appHandler.CheckDataPresent(eachExpectedData, appContext)
		if err == nil {
			isDataPresent = true
			break
		} else {
			allErrorMessage = append(allErrorMessage, err.Error())
		}
	}

	if isDataPresent {
		return nil
	}

	return fmt.Errorf("%s", strings.Join(allErrorMessage, "\n"))

}

// ValidateRestore validates a restore's spec's objects (resources) and volumes using expectedRestoredAppContexts (generated by transforming scheduledAppContexts using TransformAppContextWithMappings). This function must be called after switching to the context on which `expectedRestoredAppContexts` exists. Cluster level resources aren't validated.
func ValidateRestore(ctx context1.Context, restoreName string, orgID string, expectedRestoredAppContexts []*scheduler.Context, resourceTypesFilter []string) error {
	log.InfoD("Validating restore [%s] in org [%s]", restoreName, orgID)

	log.Infof("Obtaining restore info for restore [%s]", restoreName)
	backupDriver := Inst().Backup
	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}
	restoreInspectResponse, err := backupDriver.InspectRestore(ctx, restoreInspectRequest)
	if err != nil {
		return err
	}
	theRestore := restoreInspectResponse.GetRestore()
	restoredResourcesInfo := theRestore.GetResources()
	apparentlyRestoredVolumes := theRestore.GetVolumes()
	namespaceMappings := theRestore.GetNamespaceMapping()

	log.Infof("Checking status of restore [%s]", restoreName)
	restoreStatus := theRestore.GetStatus().Status
	if restoreStatus != api.RestoreInfo_StatusInfo_Success &&
		restoreStatus != api.RestoreInfo_StatusInfo_PartialSuccess {
		restoreStatusReason := theRestore.GetStatus().Reason
		return fmt.Errorf("ValidateRestore requires restore [%s] to have a status of Success or PartialSuccess, but found [%s] with reason [%s]", restoreName, restoreStatus, restoreStatusReason)
	}

	var errors []error

	// check if all the objects in the spec are present in the restore as per what px-backup reports
	for _, expectedRestoredAppContext := range expectedRestoredAppContexts {

		expectedRestoredAppContextNamespace := expectedRestoredAppContext.ScheduleOptions.Namespace
		log.InfoD("Validating specs for the namespace (restoredAppContext) [%s] in restore [%s]", expectedRestoredAppContextNamespace, restoreName)

		NSisPresent := false
		for _, restoredNS := range namespaceMappings {
			if restoredNS == expectedRestoredAppContextNamespace {
				NSisPresent = true
				break
			}
		}
		if !NSisPresent {
			err := fmt.Errorf("the namespace (restoredAppContext) [%s] provided to the ValidateRestore, is apparently not present in the restore [%s], hence cannot validate", expectedRestoredAppContextNamespace, restoreName)
			errors = append(errors, err)
			continue
		}

		// collect the backup resources whose specs should be present in this expectedRestoredAppContext (namespace)
		restoredObjectsInNS := make([]*api.RestoreInfo_RestoredResource, 0)
		for _, resource := range restoredResourcesInfo {
			if resource.GetNamespace() == expectedRestoredAppContextNamespace {
				restoredObjectsInNS = append(restoredObjectsInNS, resource)
			}
		}
		var updatedSpec interface{}
		var name, kind, ns string
	specloop:
		for _, spec := range expectedRestoredAppContext.App.SpecList {
			if tektonspec, ok := spec.(*tektoncdv1.Task); ok {
				updatedSpec, err = k8s.GetUpdatedSpec(tektonspec)
				if err != nil {
					err := fmt.Errorf("error in GetSpecNameKindNamepace: [%s] spec: [%+v]", err, spec)
					errors = append(errors, err)
					continue specloop
				}
			} else if tektonpipelinespec, ok := spec.(*tektoncdv1.Pipeline); ok {
				updatedSpec, err = k8s.GetUpdatedSpec(tektonpipelinespec)
				if err != nil {
					err = fmt.Errorf("error in GetUpdatedSpec: [%s]  spec: [%+v]", err, spec)
					errors = append(errors, err)
					continue specloop
				}
			} else if tektonpipelinerunspec, ok := spec.(*tektoncdv1.PipelineRun); ok {
				updatedSpec, err = k8s.GetUpdatedSpec(tektonpipelinerunspec)
				if err != nil {
					err = fmt.Errorf("error in GetUpdatedSpec: [%s] spec: [%+v]", err, spec)
					errors = append(errors, err)
					continue specloop
				}
			} else if tektontaskrunspec, ok := spec.(*tektoncdv1.TaskRun); ok {
				updatedSpec, err = k8s.GetUpdatedSpec(tektontaskrunspec)
				if err != nil {
					err = fmt.Errorf("error in GetUpdatedSpec: [%s] spec: [%+v]", err, spec)
					errors = append(errors, err)
					continue specloop
				}
			}

			if updatedSpec != nil {
				name, kind, ns, err = GetSpecNameKindNamepace(updatedSpec)
			} else {
				name, kind, ns, err = GetSpecNameKindNamepace(spec)
			}
			if err != nil {
				err := fmt.Errorf("error in GetSpecNameKindNamepace: [%s] in namespace (restoredAppContext) [%s], spec: [%+v]", err, expectedRestoredAppContextNamespace, spec)
				errors = append(errors, err)
				continue specloop
			}

			// we only validate namespace level resources
			if ns != "" {
				if name == "" || kind == "" {
					err := fmt.Errorf("error: GetSpecNameKindNamepace returned values with Spec Name: [%s], Kind: [%s], Namespace: [%s], in local Context (NS): [%s], where some of the values are empty, so this object will be ignored", name, kind, ns, expectedRestoredAppContextNamespace)
					errors = append(errors, err)
					continue specloop
				}

				if kind == "StorageClass" || kind == "VolumeSnapshot" {
					// we don't restore "StorageClass"s and "VolumeSnapshot"s (becuase we don't back them up)
					continue specloop
				}

				if len(resourceTypesFilter) > 0 && !Contains(resourceTypesFilter, kind) {
					log.Infof("kind: [%s] is not in resourceTypesFilter [%v], so object (name: [%s], kind: [%s], namespace: [%s]) in expectedRestoredAppContext [%s] will not be checked for in restore [%s]", kind, resourceTypesFilter, name, kind, ns, expectedRestoredAppContextNamespace, restoreName)
					continue specloop
				}

				for _, restoredObj := range restoredObjectsInNS {
					if name == restoredObj.Name &&
						kind == restoredObj.Kind {
						log.Infof("object (name: [%s], GVK: [%s,%s,%s], namespace: [%s]) was found in restore [%s], as expected by presence in expectedRestoredAppContext [%s]", restoredObj.Name, restoredObj.Group, restoredObj.Version, restoredObj.Kind, restoredObj.Namespace, restoreName, expectedRestoredAppContextNamespace)

						if restoredObj.Status.Status != api.RestoreInfo_StatusInfo_Success /*Can this also be partialsuccess?*/ {
							if restoredObj.Status.Status == api.RestoreInfo_StatusInfo_Retained {
								if theRestore.ReplacePolicy != api.ReplacePolicy_Retain {
									err := fmt.Errorf("object (name: [%s], kind: [%s], namespace: [%s]) was found in the restore [%s] (as expected by presence in expectedRestoredAppContext [%s]), but status was [Retained], with reason [%s], despite the replace policy being [%s]", name, kind, ns, restoreName, expectedRestoredAppContextNamespace, restoredObj.Status.Reason, theRestore.ReplacePolicy)
									errors = append(errors, err)
									IsReplacePolicySetToDelete = true
								}
							} else {
								err := fmt.Errorf("object (name: [%s], kind: [%s], namespace: [%s]) was found in the restore [%s] (as expected by presence in expectedRestoredAppContext [%s]), but status was [%s], with reason [%s]", name, kind, ns, restoreName, expectedRestoredAppContextNamespace, restoredObj.Status.Status, restoredObj.Status.Reason)
								errors = append(errors, err)
							}
						}

						_, err := k8s.GetUpdatedSpec(spec)
						if err == nil {
							log.Infof("object (name: [%s], kind: [%s], namespace: [%s]) found in the restore [%s] was also present on the cluster/namespace [%s]", name, kind, ns, restoreName, expectedRestoredAppContextNamespace)
						} else {
							err := fmt.Errorf("presence of object (name: [%s], kind: [%s], namespace: [%s]) found in the restore [%s] on the cluster/namespace [%s] could not be verified as scheduler is not K8s", name, kind, ns, restoreName, expectedRestoredAppContextNamespace)
							errors = append(errors, err)
						}

						continue specloop
					}
				}

				// The following error means that something was NOT backed up or restored,
				// OR it wasn't supposed to be either backed up or restored, and we forgot to exclude the check.
				err := fmt.Errorf("object (name: [%s], kind: [%s], namespace: [%s]) is not present in restore [%s], but was expected by it's presence in expectedRestoredAppContext [%s]", name, kind, ns, restoreName, expectedRestoredAppContextNamespace)
				errors = append(errors, err)

				if kind == "PersistentVolumeClaim" {
					err := fmt.Errorf("object (name: [%s], namespace: [%s]) is not present in restore [%s] is a PersistentVolumeClaim. Hence verification of existence of the corresponding volumes can't be done", name, ns, restoreName)
					errors = append(errors, err)
				}

				continue specloop
			}
		}

		// VALIDATION OF VOLUMES
		log.InfoD("Validating Restored Volumes for the namespace (restoredAppContext) [%s] in restore [%s]", expectedRestoredAppContextNamespace, restoreName)

		// Collect all volumes belonging to a namespace
		log.Infof("getting the volumes bounded to the PVCs in the namespace (restoredAppContext) [%s] in restore [%s]", expectedRestoredAppContextNamespace, restoreName)
		actualVolumeMap := make(map[string]*volume.Volume)
		for _, appContext := range expectedRestoredAppContexts {
			// Using appContext.ScheduleOptions to get the namespace of the appContext
			// Do not use appContext.App.Namespace as it is not set
			if appContext.ScheduleOptions.Namespace == expectedRestoredAppContextNamespace {
				actualRestoredVolumes, err := Inst().S.GetVolumes(appContext)
				if err != nil {
					err := fmt.Errorf("error getting volumes for namespace (expectedRestoredAppContext) [%s], hence skipping volume validation. Error in Inst().S.GetVolumes: [%v]", expectedRestoredAppContextNamespace, err)
					errors = append(errors, err)
					continue
				}
				for _, restoredVol := range actualRestoredVolumes {
					actualVolumeMap[restoredVol.ID] = restoredVol
				}
				log.Infof("volumes bounded to the PVCs in the context [%s] are [%+v]", expectedRestoredAppContextNamespace, actualRestoredVolumes)
			}
		}

		// looping over the list of volumes that PX-Backup says it restored, to run some checks
		for _, restoredVolInfo := range apparentlyRestoredVolumes {
			log.Infof("Restore volume is %v", restoredVolInfo.RestoreVolume)
			if namespaceMappings[restoredVolInfo.SourceNamespace] == expectedRestoredAppContextNamespace {
				switch restoredVolInfo.Status.Status {
				case api.RestoreInfo_StatusInfo_Success:
					log.Infof("in restore [%s], the status of the restored volume [%s] was Success. It was [%s] with reason [%s]", restoreName, restoredVolInfo.RestoreVolume, restoredVolInfo.Status.Status, restoredVolInfo.Status.Reason)
				case api.RestoreInfo_StatusInfo_Retained:
					if theRestore.ReplacePolicy == api.ReplacePolicy_Retain {
						log.Infof("in restore [%s], the status of the restored volume [%s] was not Success. It was [%s] with reason [%s]", restoreName, restoredVolInfo.RestoreVolume, restoredVolInfo.Status.Status, restoredVolInfo.Status.Reason)
					} else {
						err := fmt.Errorf("in restore [%s], the status of the restored volume [%s] was not Retained. It was [%s] with reason [%s]", restoreName, restoredVolInfo.RestoreVolume, restoredVolInfo.Status.Status, restoredVolInfo.Status.Reason)
						errors = append(errors, err)
						continue
					}
				default:
					err := fmt.Errorf("in restore [%s], the status of the restored volume [%s] was not Success. It was [%s] with reason [%s]", restoreName, restoredVolInfo.RestoreVolume, restoredVolInfo.Status.Status, restoredVolInfo.Status.Reason)
					errors = append(errors, err)
					continue
				}

				var actualVol *volume.Volume
				var ok bool
				if actualVol, ok = actualVolumeMap[restoredVolInfo.RestoreVolume]; !ok {
					err := fmt.Errorf("in restore [%s], said restored volume [%s] cannot be found in the actual cluster [%s]", restoreName, restoredVolInfo.RestoreVolume, expectedRestoredAppContextNamespace)
					errors = append(errors, err)
					continue
				}

				if actualVol.Name != restoredVolInfo.Pvc {
					err := fmt.Errorf("in restore [%s], for restored volume [%s], PVC used is given as [%s], but actual PVC used was found to be [%s]", restoreName, restoredVolInfo.RestoreVolume, restoredVolInfo.Pvc, actualVol.Name)
					errors = append(errors, err)
				}

				var expectedVolumeDriver string
				switch strings.ToLower(os.Getenv("BACKUP_TYPE")) {
				case string(NativeCSIWithOffloadToS3):
					expectedVolumeDriver = "kdmp"
				case string(NativeCSI):
					expectedVolumeDriver = "csi"
				case string(DirectKDMP):
					expectedVolumeDriver = "kdmp"
				case string(NativeAzure):
					expectedVolumeDriver = "azure"
				default:
					expectedVolumeDriver = Inst().V.String()
				}

				if restoredVolInfo.DriverName != expectedVolumeDriver {
					err := fmt.Errorf("in restore [%s], for restored volume [%s], volume driver actually used is given as [%s], but expected is [%s]", restoreName, restoredVolInfo.RestoreVolume, restoredVolInfo.DriverName, expectedVolumeDriver)
					errors = append(errors, err)
				}
			}
		}

		// This part is added when we have taken backup of custom resources and the restored namespace will not have all the resource as that of
		// source namespace, so modifying the expectedRestoredAppContext to have only the resources which are present in the restored namespace
		if len(resourceTypesFilter) != 0 {
			newCtxAppSpecList := make([]interface{}, 0)
			for _, spec := range expectedRestoredAppContext.App.SpecList {
				val := reflect.ValueOf(spec)
				if val.Kind() == reflect.Struct && val.FieldByName("Kind").IsValid() {
					kindField := val.FieldByName("Kind")
					log.Infof("Value of Kind field is [%s]", kindField.String())
					if IsPresent(kindField, resourceTypesFilter) {
						newCtxAppSpecList = append(newCtxAppSpecList, spec)
					}
				}
			}
			newCtx := *expectedRestoredAppContext
			newCtx.App.SpecList = newCtxAppSpecList
		}

		// VALIDATE APPLICATIONS
		log.InfoD("Validate applications in restored namespace [%s] due to restore [%s]", expectedRestoredAppContextNamespace, restoreName)
		errorChan := make(chan error, errorChannelSize)
		ValidateContext(expectedRestoredAppContext, &errorChan)
		for err := range errorChan {
			errors = append(errors, err)
		}
	}

	errStrings := make([]string, 0)
	for _, err := range errors {
		if err != nil {
			errStrings = append(errStrings, err.Error())
		}
	}

	if len(errStrings) > 0 {
		return fmt.Errorf("ValidateRestore Errors: {%s}", strings.Join(errStrings, "}\n{"))
	}

	err = validateCRCleanup(theRestore, ctx)

	return err
}

// CloneAppContextAndTransformWithMappings clones an appContext and transforms it according to the maps provided. Set `forRestore` to true when the transformation is for namespaces restored by px-backup. To be used after switching to k8s context (cluster) which has the restored namespace.
func CloneAppContextAndTransformWithMappings(appContext *scheduler.Context, namespaceMapping map[string]string, storageClassMapping map[string]string, forRestore bool) (*scheduler.Context, error) {
	appContextNamespace := appContext.ScheduleOptions.Namespace
	log.Infof("TransformAppContextWithMappings of appContext [%s] with namespace mapping [%v] and storage Class Mapping [%v]", appContextNamespace, namespaceMapping, storageClassMapping)

	restoreAppContext := *appContext
	if namespace, ok := namespaceMapping[appContextNamespace]; ok {
		restoreAppContext.App.NameSpace = namespaceMapping[namespace]
	}
	var errors []error

	// TODO: remove workaround in future.
	allStorageClassMappingsPresent := true

	specObjects := make([]interface{}, 0)
	for _, appSpecOrig := range appContext.App.SpecList {
		if forRestore {
			// if we are transforming to obtain a restored specs, VolumeSnapshot should be ignored
			if obj, ok := appSpecOrig.(*snapv1.VolumeSnapshot); ok {
				log.Infof("TransformAppContextWithMappings is for restore contexts, ignoring transformation of 'VolumeSnapshot' [%s] in appContext [%s]", obj.Metadata.Name, appContextNamespace)
				continue
			} else if obj, ok := appSpecOrig.(*storageapi.StorageClass); ok {
				log.Infof("TransformAppContextWithMappings is for restore contexts, ignoring transformation of 'StorageClass' [%s] in appContext [%s]", obj.Name, appContextNamespace)
				continue
			}
		}

		appSpec, err := CloneSpec(appSpecOrig) //clone spec to create "restore" specs
		if err != nil {
			err := fmt.Errorf("failed to clone spec: '%v'. Err: %v", appSpecOrig, err)
			errors = append(errors, err)
			continue
		}
		if forRestore {
			err = TransformToRestoredSpec(appSpec, storageClassMapping)
			if err != nil {
				err := fmt.Errorf("failed to TransformToRestoredSpec for %v, with sc map %s. Err: %v", appSpec, storageClassMapping, err)
				errors = append(errors, err)
				continue
			}
		}
		err = UpdateNamespace(appSpec, namespaceMapping)
		if err != nil {
			err := fmt.Errorf("failed to Update the namespace for %v, with ns map %s. Err: %v", appSpec, namespaceMapping, err)
			errors = append(errors, err)
			continue
		}
		specObjects = append(specObjects, appSpec)

		// TODO: remove workaround in future.
		if specObj, ok := appSpecOrig.(*corev1.PersistentVolumeClaim); ok {
			if _, ok := storageClassMapping[*specObj.Spec.StorageClassName]; !ok {
				allStorageClassMappingsPresent = false
			}
		}
	}

	errStrings := make([]string, 0)
	for _, err := range errors {
		if err != nil {
			errStrings = append(errStrings, err.Error())
		}
	}

	if len(errStrings) > 0 {
		return nil, fmt.Errorf("TransformAppContextWithMappings Errors: {%s}", strings.Join(errStrings, "}\n{"))
	}

	app := *appContext.App
	app.SpecList = specObjects
	restoreAppContext.App = &app

	// `CreateScheduleOptions` must be used in order to make it appear as though we scheduled it (rather than it being restored) in order to prove equivalency between scheduling and restoration.
	var options scheduler.ScheduleOptions
	if namespace, ok := namespaceMapping[appContextNamespace]; ok {
		options = CreateScheduleOptions(namespace)
	} else {
		options = CreateScheduleOptions(appContextNamespace)
	}
	restoreAppContext.ScheduleOptions = options

	// TODO: remove workaround in future.
	if !allStorageClassMappingsPresent {
		restoreAppContext.SkipVolumeValidation = true
	}

	return &restoreAppContext, nil
}

// TransformToRestoredSpec transforms a given spec to one expected in case of restoration by px-backup. An error is retuned if any transformation fails. specs with no need for transformation are ignored.
func TransformToRestoredSpec(spec interface{}, storageClassMapping map[string]string) error {
	if specObj, ok := spec.(*corev1.PersistentVolumeClaim); ok {
		if sc, ok := storageClassMapping[*specObj.Spec.StorageClassName]; ok {
			*specObj.Spec.StorageClassName = sc
		}
		return nil
	}

	return nil
}

// IsBackupLocationPresent checks whether the backup location is present or not
func IsBackupLocationPresent(bkpLocation string, ctx context1.Context, orgID string) (bool, error) {
	backupLocationNames := make([]string, 0)
	backupLocationEnumerateRequest := &api.BackupLocationEnumerateRequest{
		OrgId: orgID,
	}
	response, err := Inst().Backup.EnumerateBackupLocation(ctx, backupLocationEnumerateRequest)
	if err != nil {
		return false, err
	}

	for _, backupLocationObj := range response.GetBackupLocations() {
		backupLocationNames = append(backupLocationNames, backupLocationObj.GetName())
		if backupLocationObj.GetName() == bkpLocation {
			log.Infof("Backup location [%s] is present", bkpLocation)
			return true, nil
		}
	}
	log.Infof("Backup locations fetched - %s", backupLocationNames)
	return false, nil
}

// CreateCustomRestoreWithPVCs function can be used to deploy custom deployment with it's PVCs. It cannot be used for any other resource type.
func CreateCustomRestoreWithPVCs(restoreName string, backupName string, namespaceMapping map[string]string, clusterName string,
	orgID string, ctx context1.Context, storageClassMapping map[string]string, namespace string) (deploymentName string, err error) {

	var bkpUid string
	var newResources []*api.ResourceInfo
	var options metav1.ListOptions
	var deploymentPvcMap = make(map[string][]string)
	backupDriver := Inst().Backup
	log.Infof("Getting the UID of the backup needed to be restored")
	bkpUid, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return "", fmt.Errorf("unable to get backup UID for %v with error %v", backupName, err)
	}
	deploymentList, err := apps.Instance().ListDeployments(namespace, options)
	if err != nil {
		return "", fmt.Errorf("unable to list the deployments in namespace %v with error %v", namespace, err)
	}
	if len(deploymentList.Items) == 0 {
		return "", fmt.Errorf("deployment list is null")
	}
	deployments := deploymentList.Items
	for _, deployment := range deployments {
		var pvcs []string
		for _, vol := range deployment.Spec.Template.Spec.Volumes {
			pvcName := vol.PersistentVolumeClaim.ClaimName
			pvcs = append(pvcs, pvcName)
		}
		deploymentPvcMap[deployment.Name] = pvcs
	}
	// select a random index from the slice of deployment names to be restored
	randomIndex := rand.Intn(len(deployments))
	deployment := deployments[randomIndex]
	log.Infof("selected deployment %v", deployment.Name)
	pvcs, exists := deploymentPvcMap[deployment.Name]
	if !exists {
		return "", fmt.Errorf("deploymentName %v not found in the deploymentPvcMap", deployment.Name)
	}
	deploymentStruct := &api.ResourceInfo{
		Version:   "v1",
		Group:     "apps",
		Kind:      "Deployment",
		Name:      deployment.Name,
		Namespace: namespace,
	}
	pvcsStructs := make([]*api.ResourceInfo, len(pvcs))
	for i, pvcName := range pvcs {
		pvcStruct := &api.ResourceInfo{
			Version:   "v1",
			Group:     "core",
			Kind:      "PersistentVolumeClaim",
			Name:      pvcName,
			Namespace: namespace,
		}
		pvcsStructs[i] = pvcStruct
	}
	newResources = append([]*api.ResourceInfo{deploymentStruct}, pvcsStructs...)
	createRestoreReq := &api.RestoreCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  restoreName,
			OrgId: orgID,
		},
		Backup:              backupName,
		Cluster:             clusterName,
		NamespaceMapping:    namespaceMapping,
		StorageClassMapping: storageClassMapping,
		BackupRef: &api.ObjectRef{
			Name: backupName,
			Uid:  bkpUid,
		},
		IncludeResources: newResources,
	}
	_, err = backupDriver.CreateRestore(ctx, createRestoreReq)
	if err != nil {
		return "", fmt.Errorf("fail to create restore with createrestore req %v and error %v", createRestoreReq, err)
	}
	err = RestoreSuccessCheck(restoreName, orgID, MaxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return "", fmt.Errorf("fail to create restore %v with error %v", restoreName, err)
	}
	return deployment.Name, nil
}

// GetOrdinalScheduleBackupName returns the name of the schedule backup at the specified ordinal position for the given schedule
func GetOrdinalScheduleBackupName(ctx context1.Context, scheduleName string, ordinal int, orgID string) (string, error) {
	if ordinal < 1 {
		return "", fmt.Errorf("the provided ordinal value [%d] for schedule backups with schedule name [%s] is invalid. valid values range from 1", ordinal, scheduleName)
	}
	allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	if len(allScheduleBackupNames) == 0 {
		return "", fmt.Errorf("no backups were found for the schedule [%s]", scheduleName)
	}
	if ordinal > len(allScheduleBackupNames) {
		return "", fmt.Errorf("schedule backups with schedule name [%s] have not been created up to the provided ordinal value [%d]", scheduleName, ordinal)
	}
	return allScheduleBackupNames[ordinal-1], nil
}

// GetFirstScheduleBackupName returns the name of the first schedule backup for the given schedule
func GetFirstScheduleBackupName(ctx context1.Context, scheduleName string, orgID string) (string, error) {
	var allScheduleBackupNames []string
	var err error
	getFirstScheduleBackup := func() (interface{}, bool, error) {
		allScheduleBackupNames, err = Inst().Backup.GetAllScheduleBackupNames(ctx, scheduleName, orgID)
		if err != nil {
			return "", false, err
		}
		if len(allScheduleBackupNames) == 0 {
			return "", true, fmt.Errorf("no backups found for schedule %s yet", scheduleName)
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(getFirstScheduleBackup, 20*time.Second, 5*time.Second)
	if err != nil {
		return "", err
	}
	return allScheduleBackupNames[0], nil
}

// GetLatestScheduleBackupName returns the name of the latest schedule backup for the given schedule
func GetLatestScheduleBackupName(ctx context1.Context, scheduleName string, orgID string) (string, error) {
	allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	if len(allScheduleBackupNames) == 0 {
		return "", fmt.Errorf("no backups found for schedule %s", scheduleName)
	}
	return allScheduleBackupNames[len(allScheduleBackupNames)-1], nil
}

// GetOrdinalScheduleBackupUID returns the uid of the schedule backup at the specified ordinal position for the given schedule
func GetOrdinalScheduleBackupUID(ctx context1.Context, scheduleName string, ordinal int, orgID string) (string, error) {
	if ordinal < 1 {
		return "", fmt.Errorf("the provided ordinal value [%d] for schedule backups with schedule name [%s] is invalid. valid values range from 1", ordinal, scheduleName)
	}
	allScheduleBackupUids, err := Inst().Backup.GetAllScheduleBackupUIDs(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	if len(allScheduleBackupUids) == 0 {
		return "", fmt.Errorf("no backups were found for the schedule [%s]", scheduleName)
	}
	if ordinal > len(allScheduleBackupUids) {
		return "", fmt.Errorf("schedule backups with schedule name [%s] have not been created up to the provided ordinal value [%d]", scheduleName, ordinal)
	}
	return allScheduleBackupUids[ordinal-1], nil
}

// GetFirstScheduleBackupUID returns the uid of the first schedule backup for the given schedule
func GetFirstScheduleBackupUID(ctx context1.Context, scheduleName string, orgID string) (string, error) {
	allScheduleBackupUids, err := Inst().Backup.GetAllScheduleBackupUIDs(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	if len(allScheduleBackupUids) == 0 {
		return "", fmt.Errorf("no backups found for schedule %s", scheduleName)
	}
	return allScheduleBackupUids[0], nil
}

// GetLatestScheduleBackupUID returns the uid of the latest schedule backup for the given schedule
func GetLatestScheduleBackupUID(ctx context1.Context, scheduleName string, orgID string) (string, error) {
	allScheduleBackupUids, err := Inst().Backup.GetAllScheduleBackupUIDs(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	if len(allScheduleBackupUids) == 0 {
		return "", fmt.Errorf("no backups found for schedule %s", scheduleName)
	}
	return allScheduleBackupUids[len(allScheduleBackupUids)-1], nil
}

// IsPresent verifies if the given data is present in slice of data
func IsPresent(dataSlice interface{}, data interface{}) bool {
	s := reflect.ValueOf(dataSlice)
	for i := 0; i < s.Len(); i++ {
		if s.Index(i).Interface() == data {
			return true
		}
	}
	return false
}

func DeleteBackupAndWait(backupName string, ctx context1.Context) error {
	backupDriver := Inst().Backup
	backupEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: BackupOrgID,
	}

	backupDeletionSuccessCheck := func() (interface{}, bool, error) {
		currentBackups, err := backupDriver.EnumerateBackup(ctx, backupEnumerateReq)
		if err != nil {
			return "", true, err
		}
		for _, backupObject := range currentBackups.GetBackups() {
			if backupObject.Name == backupName {
				return "", true, fmt.Errorf("backupObject [%s] is not yet deleted . status:[%s] ", backupObject.Name, backupObject.Status)
			}
		}
		return "", false, nil
	}
	_, err := task.DoRetryWithTimeout(backupDeletionSuccessCheck, BackupDeleteTimeout, BackupDeleteRetryTime)
	return err
}

// GetPxBackupVersion return the version of Px Backup as a VersionInfo struct
func GetPxBackupVersion() (*api.VersionInfo, error) {
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return nil, err
	}
	versionResponse, err := Inst().Backup.GetPxBackupVersion(ctx, &api.VersionGetRequest{})
	if err != nil {
		return nil, err
	}
	backupVersion := versionResponse.GetVersion()
	return backupVersion, nil
}

// GetPxBackupVersionString returns the version of Px Backup like 2.4.0-e85b680
func GetPxBackupVersionString() (string, error) {
	backupVersion, err := GetPxBackupVersion()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s.%s.%s-%s", backupVersion.GetMajor(), backupVersion.GetMinor(), backupVersion.GetPatch(), backupVersion.GetGitCommit()), nil
}

// GetPxBackupVersionSemVer returns the version of Px Backup in semver format like 2.4.0
func GetPxBackupVersionSemVer() (string, error) {
	backupVersion, err := GetPxBackupVersion()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s.%s.%s", backupVersion.GetMajor(), backupVersion.GetMinor(), backupVersion.GetPatch()), nil
}

// GetPxBackupBuildDate returns the Px Backup build date
func GetPxBackupBuildDate() (string, error) {
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return "", err
	}
	versionResponse, err := Inst().Backup.GetPxBackupVersion(ctx, &api.VersionGetRequest{})
	if err != nil {
		return "", err
	}
	backupVersion := versionResponse.GetVersion()
	return backupVersion.GetBuildDate(), nil
}

// CompareCurrentPxBackupVersion compares the current PX Backup version against a specified target version using a comparison method provided as a parameter.
func CompareCurrentPxBackupVersion(targetVersionStr string, comparisonMethod func(v1, v2 *version.Version) bool) (bool, error) {
	currentVersionStr, err := GetPxBackupVersionSemVer()
	if err != nil {
		return false, err
	}

	currentVersion, err := version.NewVersion(currentVersionStr)
	if err != nil {
		return false, err
	}

	targetVersion, err := version.NewVersion(targetVersionStr)
	if err != nil {
		return false, err
	}
	return comparisonMethod(currentVersion, targetVersion), nil
}

// PxBackupUpgrade will perform the upgrade tasks for Px Backup to the version passed as string
// Eg: versionToUpgrade := "2.4.0"
func PxBackupUpgrade(versionToUpgrade string) error {
	var cmd string

	// Compare and validate the upgrade path
	currentBackupVersionString, err := GetPxBackupVersionSemVer()
	if err != nil {
		return err
	}
	currentBackupVersion, err := version.NewSemver(currentBackupVersionString)
	if err != nil {
		return err
	}
	versionToUpgradeSemVer, err := version.NewSemver(versionToUpgrade)
	if err != nil {
		return err
	}

	if currentBackupVersion.GreaterThanOrEqual(versionToUpgradeSemVer) {
		return fmt.Errorf("px backup cannot be upgraded from version [%s] to version [%s]", currentBackupVersion.String(), versionToUpgradeSemVer.String())
	} else {
		log.InfoD("Upgrade path chosen (%s) ---> (%s)", currentBackupVersionString, versionToUpgrade)
	}

	// Getting Px Backup Namespace
	pxBackupNamespace, err := backup.GetPxBackupNamespace()
	if err != nil {
		return err
	}

	// Delete the pxcentral-post-install-hook job is it exists
	allJobs, err := batch.Instance().ListAllJobs(pxBackupNamespace, metav1.ListOptions{})
	if err != nil {
		return err
	}
	if len(allJobs.Items) > 0 {
		log.Infof("List of all the jobs in Px Backup Namespace [%s] - ", pxBackupNamespace)
		for _, job := range allJobs.Items {
			log.Infof(job.Name)
		}

		for _, job := range allJobs.Items {
			if strings.Contains(job.Name, pxCentralPostInstallHookJobName) {
				err = deleteJobAndWait(job)
				if err != nil {
					return err
				}
			}
		}
	} else {
		log.Infof("%s job not found", pxCentralPostInstallHookJobName)
	}

	// Get storage class using for px-backup deployment
	statefulSet, err := apps.Instance().GetStatefulSet(MongodbStatefulset, pxBackupNamespace)
	if err != nil {
		return err
	}
	pvcs, err := apps.Instance().GetPVCsForStatefulSet(statefulSet)
	if err != nil {
		return err
	}
	storageClassName := pvcs.Items[0].Spec.StorageClassName

	// Get the tarball required for helm upgrade
	helmBranch, isPresent := os.LookupEnv("PX_BACKUP_HELM_REPO_BRANCH")
	if !isPresent || helmBranch == "" {
		helmBranch = defaultPxBackupHelmBranch
	}
	cmd = fmt.Sprintf("curl -O  https://raw.githubusercontent.com/portworx/helm/%s/stable/px-central-%s.tgz", helmBranch, versionToUpgrade)
	log.Infof("curl command to get tarball: %v ", cmd)
	output, _, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("error downloading of tarball: %v", err)
	}
	log.Infof("Terminal output: %s", output)

	// Checking if all pods are healthy before upgrade
	err = ValidateAllPodsInPxBackupNamespace()
	if err != nil {
		return err
	}

	// Execute helm upgrade using cmd
	log.Infof("Upgrading Px-Backup version from %s to %s", currentBackupVersionString, versionToUpgrade)
	customRegistry := os.Getenv("CUSTOM_REGISTRY")
	customRepo := os.Getenv("CUSTOM_REPO")
	if customRegistry == "" || customRepo == "" {
		cmd = fmt.Sprintf("helm upgrade px-central px-central-%s.tgz --namespace %s --version %s --set persistentStorage.enabled=true,persistentStorage.storageClassName=\"%s\",pxbackup.enabled=true",
			versionToUpgrade, pxBackupNamespace, versionToUpgrade, *storageClassName)
	} else {
		cmd = fmt.Sprintf("helm upgrade px-central px-central-%s.tgz --namespace %s --version %s --set persistentStorage.enabled=true,persistentStorage.storageClassName=\"%s\",pxbackup.enabled=true",
			versionToUpgrade, pxBackupNamespace, versionToUpgrade, *storageClassName)

		// Additional settings to be appended using template
		tmpl := `,{{range .Images}}images.{{.Name}}.repo="{{$.CustomRepo}}",images.{{.Name}}.registry="{{$.CustomRegistry}}",{{end}}`

		// Define the template
		t, err := template.New("cmd").Parse(tmpl)
		if err != nil {
			return err
		}

		// Data for the template
		data := struct {
			CustomRegistry string
			CustomRepo     string
			Images         []struct{ Name string }
		}{
			CustomRegistry: customRegistry,
			CustomRepo:     customRepo,
			Images: []struct{ Name string }{
				{Name: "pxcentralApiServerImage"},
				{Name: "pxcentralFrontendImage"},
				{Name: "pxcentralBackendImage"},
				{Name: "pxcentralMiddlewareImage"},
				{Name: "postInstallSetupImage"},
				{Name: "keycloakBackendImage"},
				{Name: "keycloakFrontendImage"},
				{Name: "keycloakLoginThemeImage"},
				{Name: "keycloakInitContainerImage"},
				{Name: "mysqlImage"},
				{Name: "pxBackupImage"},
				{Name: "mongodbImage"},
				{Name: "licenseServerImage"},
				{Name: "cortexImage"},
				{Name: "cassandraImage"},
				{Name: "proxyConfigImage"},
				{Name: "consulImage"},
				{Name: "dnsmasqImage"},
				{Name: "grafanaImage"},
				{Name: "prometheusImage"},
				{Name: "prometheusConfigReloadrImage"},
				{Name: "prometheusOperatorImage"},
				{Name: "memcachedMetricsImage"},
				{Name: "memcachedIndexImage"},
				{Name: "memcachedImage"},
				{Name: "pxBackupPrometheusImage"},
				{Name: "pxBackupAlertmanagerImage"},
				{Name: "pxBackupPrometheusOperatorImage"},
				{Name: "pxBackupPrometheusConfigReloaderImage"},
			},
		}

		// Execute the template and append the result to the existing command
		var buf bytes.Buffer
		if err := t.Execute(&buf, data); err != nil {
			return err
		}

		// Append the dynamically generated settings to the initial command
		cmd += buf.String()
	}
	log.Infof("helm command: %v ", cmd)

	pxBackupUpgradeStartTime := time.Now()

	output, _, err = osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("upgrade failed with error: %v", err)
	}
	log.Infof("Terminal output: %s", output)

	// Collect mongoDB logs right after the command
	currentSpecReport := CurrentSpecReport()
	testCaseName := fmt.Sprintf("%s-start", currentSpecReport.FullText())
	CollectMongoDBLogs(testCaseName)

	// Wait for post install hook job to be completed
	postInstallHookJobCompletedCheck := func() (interface{}, bool, error) {
		job, err := batch.Instance().GetJob(pxCentralPostInstallHookJobName, pxBackupNamespace)
		if err != nil {
			return "", true, err
		}
		if job.Status.Succeeded > 0 {
			log.Infof("Status of job %s after completion - "+
				"\nactive count - %d"+
				"\nsucceeded count - %d"+
				"\nfailed count - %d\n", job.Name, job.Status.Active, job.Status.Succeeded, job.Status.Failed)
			return "", false, nil
		}
		return "", true, fmt.Errorf("status of job %s not yet in desired state - "+
			"\nactive count - %d"+
			"\nsucceeded count - %d"+
			"\nfailed count - %d\n", job.Name, job.Status.Active, job.Status.Succeeded, job.Status.Failed)
	}
	_, err = task.DoRetryWithTimeout(postInstallHookJobCompletedCheck, 10*time.Minute, 30*time.Second)
	if err != nil {
		return err
	}

	// Collect mongoDB logs once the postInstallHook is completed
	currentSpecReport = CurrentSpecReport()
	testCaseName = fmt.Sprintf("%s-end", currentSpecReport.FullText())
	CollectMongoDBLogs(testCaseName)

	pxBackupUpgradeEndTime := time.Now()
	pxBackupUpgradeDuration := pxBackupUpgradeEndTime.Sub(pxBackupUpgradeStartTime)
	log.InfoD("Time taken for Px-Backup upgrade to complete: %02d:%02d:%02d hh:mm:ss", int(pxBackupUpgradeDuration.Hours()), int(pxBackupUpgradeDuration.Minutes())%60, int(pxBackupUpgradeDuration.Seconds())%60)

	// Checking if all pods are running
	err = ValidateAllPodsInPxBackupNamespace()
	if err != nil {
		return err
	}

	postUpgradeVersion, err := GetPxBackupVersionSemVer()
	if err != nil {
		return err
	}
	if !strings.EqualFold(postUpgradeVersion, versionToUpgrade) {
		return fmt.Errorf("expected version after upgrade was %s but got %s", versionToUpgrade, postUpgradeVersion)
	}
	log.InfoD("Px-Backup upgrade from %s to %s is complete", currentBackupVersionString, postUpgradeVersion)
	return nil
}

// deleteJobAndWait waits for the provided job to be deleted
func deleteJobAndWait(job batchv1.Job) error {
	t := func() (interface{}, bool, error) {
		err := batch.Instance().DeleteJob(job.Name, job.Namespace)

		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return "", false, nil
			}
			return "", false, err
		}
		return "", true, fmt.Errorf("job %s not deleted", job.Name)
	}

	_, err := task.DoRetryWithTimeout(t, jobDeleteTimeout, jobDeleteRetryTime)
	if err != nil {
		return err
	}
	log.Infof("job %s deleted", job.Name)
	return nil
}

func ValidateAllPodsInPxBackupNamespace() error {
	pxBackupNamespace, err := backup.GetPxBackupNamespace()
	if err != nil {
		return err
	}
	for _, label := range backupPodLabels {
		allPods, err := core.Instance().GetPods(pxBackupNamespace, label)
		if err != nil {
			return err
		}
		for _, pod := range allPods.Items {
			log.Infof("Checking status for pod - %s", pod.GetName())
			err = core.Instance().ValidatePod(&pod, podReadyTimeout, podReadyRetryTime)
			if err != nil {
				// Collect mongoDB logs right after the command
				currentSpecReport := CurrentSpecReport()
				testCaseName := currentSpecReport.FullText()
				matches := regexp.MustCompile(`\{([^}]+)\}`).FindStringSubmatch(testCaseName)
				if len(matches) > 1 {
					testCaseName = fmt.Sprintf("%s-error-%s", matches[1], label)
				}
				CollectLogsFromPods(testCaseName, label, pxBackupNamespace, pod.GetName())
				return err
			}
		}
	}
	err = IsMongoDBReady()
	return err
}

// getStorkImageVersion returns current stork image version.
func getStorkImageVersion() (string, error) {
	storkDeploymentNamespace, err := k8sutils.GetStorkPodNamespace()
	if err != nil {
		return "", err
	}
	storkDeployment, err := apps.Instance().GetDeployment(storkDeploymentName, storkDeploymentNamespace)
	if err != nil {
		return "", err
	}
	storkImage := storkDeployment.Spec.Template.Spec.Containers[0].Image
	storkImageVersion := strings.Split(storkImage, ":")[len(strings.Split(storkImage, ":"))-1]
	return storkImageVersion, nil
}

// upgradeStorkVersion upgrades the stork to the provided version.
func UpgradeStorkVersion(storkImageToUpgrade string) error {
	var finalImageToUpgrade string
	var postUpgradeStorkImageVersionStr string
	storkDeploymentNamespace, err := k8sutils.GetStorkPodNamespace()
	if err != nil {
		return err
	}
	currentStorkImageStr, err := getStorkImageVersion()
	if err != nil {
		return err
	}
	currentStorkVersion, err := version.NewSemver(currentStorkImageStr)
	if err != nil {
		return err
	}
	storkImageVersionToUpgrade, err := version.NewSemver(storkImageToUpgrade)
	if err != nil {
		return err
	}

	log.Infof("Current stork version : %s", currentStorkVersion)
	log.Infof("Upgrading stork version to : %s", storkImageVersionToUpgrade)

	if currentStorkVersion.GreaterThanOrEqual(storkImageVersionToUpgrade) {
		return fmt.Errorf("cannot upgrade stork version from %s to %s as the current version is higher than the provided version", currentStorkVersion, storkImageVersionToUpgrade)
	}
	internalDockerRegistry := os.Getenv("INTERNAL_DOCKER_REGISTRY")
	if internalDockerRegistry != "" {
		finalImageToUpgrade = fmt.Sprintf("%s/portworx/stork:%s", internalDockerRegistry, storkImageToUpgrade)
	} else {
		finalImageToUpgrade = fmt.Sprintf("docker.io/openstorage/stork:%s", storkImageToUpgrade)
	}
	isOpBased, _ := Inst().V.IsOperatorBasedInstall()
	if isOpBased {
		log.Infof("Operator based Portworx deployment, Upgrading stork via StorageCluster")
		storageSpec, err := Inst().V.GetDriver()
		if err != nil {
			return err
		}
		storageSpec.Spec.Stork.Image = finalImageToUpgrade

		// Check to reset customImageRegistry to blank as in case of ibm it'll be icr.io/ext/ and not
		// docker.io/ which causes issues when we try to install stork which is not pushed to icr.io/ext
		if GetClusterProviders()[0] == "ibm" {
			storageSpec.Spec.CustomImageRegistry = ""
		}

		_, err = operator.Instance().UpdateStorageCluster(storageSpec)
		if err != nil {
			return err
		}
	} else {
		log.Infof("Non-Operator based Portworx deployment, Upgrading stork via Deployment")
		storkDeployment, err := apps.Instance().GetDeployment(storkDeploymentName, storkDeploymentNamespace)
		if err != nil {
			return err
		}
		storkDeployment.Spec.Template.Spec.Containers[0].Image = finalImageToUpgrade
		_, err = apps.Instance().UpdateDeployment(storkDeployment)
		if err != nil {
			return err
		}
	}
	// Wait for upgrade request to go through before validating
	t := func() (interface{}, bool, error) {
		postUpgradeStorkImageVersionStr, err = getStorkImageVersion()
		if err != nil {
			return "", true, err
		}
		if !strings.EqualFold(postUpgradeStorkImageVersionStr, storkImageToUpgrade) {
			return "", true, fmt.Errorf("expected version after upgrade was %s but got %s", storkImageToUpgrade, postUpgradeStorkImageVersionStr)
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 30*time.Second)
	if err != nil {
		return err
	}

	// validate stork pods after upgrade
	updatedStorkDeployment, err := apps.Instance().GetDeployment(storkDeploymentName, storkDeploymentNamespace)
	if err != nil {
		return err
	}
	err = apps.Instance().ValidateDeployment(updatedStorkDeployment, storkPodReadyTimeout, podReadyRetryTime)
	if err != nil {
		return err
	}

	log.Infof("Successfully upgraded stork version from %v to %v", currentStorkImageStr, postUpgradeStorkImageVersionStr)
	return nil
}

// CreateBackupWithNamespaceLabel creates a backup with Namespace label and checks for success
func CreateBackupWithNamespaceLabel(backupName string, clusterName string, bkpLocation string, bkpLocationUID string,
	labelSelectors map[string]string, orgID string, uid string, preRuleName string, preRuleUid string, postRuleName string,
	postRuleUid string, namespaceLabel string, ctx context1.Context) error {
	_, err := CreateBackupWithNamespaceLabelWithoutCheck(backupName, clusterName, bkpLocation, bkpLocationUID, labelSelectors, orgID, uid, preRuleName, preRuleUid, postRuleName, postRuleUid, namespaceLabel, ctx)
	if err != nil {
		return err
	}
	err = BackupSuccessCheck(backupName, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Successfully created backup [%s] with namespace label [%s]", backupName, namespaceLabel)
	return nil
}

// CreateBackupWithNamespaceLabelWithValidation creates backup with namespace label, checks for success, and validates the backup.
func CreateBackupWithNamespaceLabelWithValidation(ctx context1.Context, backupName string, clusterName string, bkpLocation string, bkpLocationUID string, scheduledAppContextsExpectedInBackup []*scheduler.Context, labelSelectors map[string]string, orgID string, uid string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, namespaceLabel string) error {
	err := CreateBackupWithNamespaceLabel(backupName, clusterName, bkpLocation, bkpLocationUID, labelSelectors, orgID, uid, preRuleName, preRuleUid, postRuleName, postRuleUid, namespaceLabel, ctx)
	if err != nil {
		return err
	}
	return ValidateBackup(ctx, backupName, orgID, scheduledAppContextsExpectedInBackup, make([]string, 0))
}

// CreateScheduleBackupWithNamespaceLabel creates a schedule backup with namespace label and checks for success
func CreateScheduleBackupWithNamespaceLabel(scheduleName string, clusterName string, bkpLocation string, bkpLocationUID string, labelSelectors map[string]string, orgID string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, namespaceLabel, schPolicyName string, schPolicyUID string, ctx context1.Context) error {
	_, err := CreateScheduleBackupWithNamespaceLabelWithoutCheck(scheduleName, clusterName, bkpLocation, bkpLocationUID, labelSelectors, orgID, preRuleName, preRuleUid, postRuleName, postRuleUid, schPolicyName, schPolicyUID, namespaceLabel, ctx)
	if err != nil {
		return err
	}
	time.Sleep(1 * time.Minute)
	firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
	if err != nil {
		return err
	}
	log.InfoD("first schedule backup for schedule name [%s] is [%s]", scheduleName, firstScheduleBackupName)
	err = BackupSuccessCheck(firstScheduleBackupName, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Successfully created schedule backup [%s] with namespace label [%s]", firstScheduleBackupName, namespaceLabel)
	return nil
}

// CreateBackupWithNamespaceLabelWithoutCheck creates backup with namespace label filter without waiting for success
func CreateBackupWithNamespaceLabelWithoutCheck(backupName string, clusterName string, bkpLocation string, bkpLocationUID string,
	labelSelectors map[string]string, orgID string, uid string, preRuleName string, preRuleUid string, postRuleName string,
	postRuleUid string, namespaceLabel string, ctx context1.Context) (*api.BackupInspectResponse, error) {

	if GlobalRuleFlag {
		preRuleName = GlobalPreRuleName
		if GlobalPreRuleName != "" {
			preRuleUid = GlobalPreRuleUid
		}

		postRuleName = GlobalPostRuleName
		if GlobalPostRuleName != "" {
			postRuleUid = GlobalPostRuleUid
		}
	}

	backupDriver := Inst().Backup
	bkpCreateRequest := &api.BackupCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  backupName,
			OrgId: orgID,
		},
		BackupLocationRef: &api.ObjectRef{
			Name: bkpLocation,
			Uid:  bkpLocationUID,
		},
		Cluster:        clusterName,
		LabelSelectors: labelSelectors,
		ClusterRef: &api.ObjectRef{
			Name: clusterName,
			Uid:  uid,
		},
		PreExecRuleRef: &api.ObjectRef{
			Name: preRuleName,
			Uid:  preRuleUid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: postRuleName,
			Uid:  postRuleUid,
		},
		NsLabelSelectors: namespaceLabel,
	}

	err := AdditionalBackupRequestParams(bkpCreateRequest)
	if err != nil {
		return nil, err
	}
	_, err = backupDriver.CreateBackup(ctx, bkpCreateRequest)
	if err != nil {
		return nil, err
	}
	backupUid, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return nil, err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUid,
		OrgId: orgID,
	}
	resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// CreateScheduleBackupWithNamespaceLabelWithoutCheck creates a schedule backup with namespace label filter without waiting for success
func CreateScheduleBackupWithNamespaceLabelWithoutCheck(scheduleName string, clusterName string, bkpLocation string, bkpLocationUID string, labelSelectors map[string]string, orgID string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, schPolicyName string, schPolicyUID string, namespaceLabel string, ctx context1.Context) (*api.BackupScheduleInspectResponse, error) {

	if GlobalRuleFlag {
		preRuleName = GlobalPreRuleName
		if GlobalPreRuleName != "" {
			preRuleUid = GlobalPreRuleUid
		}

		postRuleName = GlobalPostRuleName
		if GlobalPostRuleName != "" {
			postRuleUid = GlobalPostRuleUid
		}
	}

	backupDriver := Inst().Backup
	bkpSchCreateRequest := &api.BackupScheduleCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  scheduleName,
			OrgId: orgID,
		},
		SchedulePolicyRef: &api.ObjectRef{
			Name: schPolicyName,
			Uid:  schPolicyUID,
		},
		BackupLocationRef: &api.ObjectRef{
			Name: bkpLocation,
			Uid:  bkpLocationUID,
		},
		SchedulePolicy: schPolicyName,
		Cluster:        clusterName,
		LabelSelectors: labelSelectors,
		PreExecRuleRef: &api.ObjectRef{
			Name: preRuleName,
			Uid:  preRuleUid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: postRuleName,
			Uid:  postRuleUid,
		},
		NsLabelSelectors: namespaceLabel,
	}

	err := AdditionalScheduledBackupRequestParams(bkpSchCreateRequest)
	if err != nil {
		return nil, err
	}
	_, err = backupDriver.CreateBackupSchedule(ctx, bkpSchCreateRequest)
	if err != nil {
		return nil, err
	}
	backupScheduleInspectRequest := &api.BackupScheduleInspectRequest{
		OrgId: orgID,
		Name:  scheduleName,
		Uid:   "",
	}
	resp, err := backupDriver.InspectBackupSchedule(ctx, backupScheduleInspectRequest)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// CreateVMScheduleBackupWithNamespaceLabelWithoutCheck creates a schedule backup with namespace label filter without waiting for success
func CreateVMScheduleBackupWithNamespaceLabelWithoutCheck(scheduleName string, vms []kubevirtv1.VirtualMachine, clusterName string, bkpLocation string, bkpLocationUID string, labelSelectors map[string]string, orgID string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, schPolicyName string, schPolicyUID string, namespaceLabel string, skipVmAutoExecRules bool, ctx context1.Context) (*api.BackupScheduleInspectResponse, error) {
	backupDriver := Inst().Backup

	includeResource := GenerateResourceInfo(vms)
	bkpScheduleCreateRequest := &api.BackupScheduleCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  scheduleName,
			OrgId: orgID,
		},
		BackupLocationRef: &api.ObjectRef{
			Name: bkpLocation,
			Uid:  bkpLocationUID,
		},
		SchedulePolicyRef: &api.ObjectRef{
			Name: schPolicyName,
			Uid:  schPolicyUID,
		},
		SchedulePolicy: schPolicyName,
		Cluster:        clusterName,
		LabelSelectors: labelSelectors,
		PreExecRuleRef: &api.ObjectRef{
			Name: preRuleName,
			Uid:  preRuleUid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: postRuleName,
			Uid:  postRuleUid,
		},
		IncludeResources: includeResource,
		BackupObjectType: &api.BackupScheduleCreateRequest_BackupObjectType{
			Type: api.BackupScheduleCreateRequest_BackupObjectType_VirtualMachine,
		},
		SkipVmAutoExecRules: skipVmAutoExecRules,
		NsLabelSelectors:    namespaceLabel,
	}

	err := AdditionalScheduledBackupRequestParams(bkpScheduleCreateRequest)
	if err != nil {
		return nil, err
	}

	_, err = backupDriver.CreateBackupSchedule(ctx, bkpScheduleCreateRequest)
	if err != nil {
		return nil, err
	}
	backupScheduleInspectRequest := &api.BackupScheduleInspectRequest{
		OrgId: orgID,
		Name:  scheduleName,
		Uid:   "",
	}
	resp, err := backupDriver.InspectBackupSchedule(ctx, backupScheduleInspectRequest)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// CreateScheduleBackupWithNamespaceLabelWithValidation creates a schedule backup with namespace label, checks for success of first (immediately triggered) backup, validates that backup and returns the name of that first scheduled backup
func CreateScheduleBackupWithNamespaceLabelWithValidation(ctx context1.Context, scheduleName string, clusterName string, bkpLocation string, bkpLocationUID string, scheduledAppContextsExpectedInBackup []*scheduler.Context, labelSelectors map[string]string, orgID string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, namespaceLabel string, schPolicyName string, schPolicyUID string) (string, error) {
	_, err := CreateScheduleBackupWithNamespaceLabelWithoutCheck(scheduleName, clusterName, bkpLocation, bkpLocationUID, labelSelectors, orgID, preRuleName, preRuleUid, postRuleName, postRuleUid, schPolicyName, schPolicyUID, namespaceLabel, ctx)
	if err != nil {
		return "", err
	}
	time.Sleep(1 * time.Minute)
	firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	log.InfoD("first schedule backup for schedule name [%s] is [%s]", scheduleName, firstScheduleBackupName)

	return firstScheduleBackupName, BackupSuccessCheckWithValidation(ctx, firstScheduleBackupName, scheduledAppContextsExpectedInBackup, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
}

// CreateVMScheduleBackupWithNamespaceLabelWithValidation creates a schedule backup for VM with namespace label, checks for success of first (immediately triggered) backup, validates that backup and returns the name of that first scheduled backup
func CreateVMScheduleBackupWithNamespaceLabelWithValidation(ctx context1.Context, scheduleName string, vms []kubevirtv1.VirtualMachine, clusterName string, bkpLocation string, bkpLocationUID string, scheduledAppContextsExpectedInBackup []*scheduler.Context, labelSelectors map[string]string, orgID string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string, namespaceLabel string, schPolicyName string, schPolicyUID string, skipVmAutoExecRules bool) (string, error) {
	var removeSpecs []interface{}
	for _, scheduledAppContext := range scheduledAppContextsExpectedInBackup {
		// Removing specs which are outside the scope of VM Backup
		for _, spec := range scheduledAppContext.App.SpecList {
			// VM Backup will not consider service object for now
			if appSpec, ok := spec.(*corev1.Service); ok {
				removeSpecs = append(removeSpecs, appSpec)
			}
			// TODO: Add more types of specs to remove depending on the app context
		}
		err := Inst().S.RemoveAppSpecsByName(scheduledAppContext, removeSpecs)
		if err != nil {
			return "", err
		}
	}

	_, err := CreateVMScheduleBackupWithNamespaceLabelWithoutCheck(scheduleName, vms, clusterName, bkpLocation, bkpLocationUID, labelSelectors, orgID, preRuleName, preRuleUid, postRuleName, postRuleUid, schPolicyName, schPolicyUID, namespaceLabel, skipVmAutoExecRules, ctx)
	if err != nil {
		return "", err
	}
	time.Sleep(1 * time.Minute)
	firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	log.InfoD("first schedule backup for schedule name [%s] is [%s]", scheduleName, firstScheduleBackupName)

	return firstScheduleBackupName, BackupSuccessCheckWithValidation(ctx, firstScheduleBackupName, scheduledAppContextsExpectedInBackup, orgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
}

// suspendBackupSchedule will suspend backup schedule
func SuspendBackupSchedule(backupScheduleName, schPolicyName, OrgID string, ctx context1.Context) error {
	backupDriver := Inst().Backup
	backupScheduleUID, err := GetScheduleUID(backupScheduleName, BackupOrgID, ctx)
	if err != nil {
		return err
	}
	schPolicyUID, err := Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, schPolicyName)
	if err != nil {
		return err
	}
	bkpScheduleSuspendRequest := &api.BackupScheduleUpdateRequest{
		CreateMetadata: &api.CreateMetadata{Name: backupScheduleName, OrgId: OrgID, Uid: backupScheduleUID},
		Suspend:        true,
		SchedulePolicyRef: &api.ObjectRef{
			Name: schPolicyName,
			Uid:  schPolicyUID,
		},
	}
	_, err = backupDriver.UpdateBackupSchedule(ctx, bkpScheduleSuspendRequest)
	return err
}

// resumeBackupSchedule will resume backup schedule
func ResumeBackupSchedule(backupScheduleName, schPolicyName, OrgID string, ctx context1.Context) error {
	backupDriver := Inst().Backup
	backupScheduleUID, err := GetScheduleUID(backupScheduleName, BackupOrgID, ctx)
	if err != nil {
		return err
	}
	schPolicyUID, err := Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, schPolicyName)
	if err != nil {
		return err
	}
	bkpScheduleSuspendRequest := &api.BackupScheduleUpdateRequest{
		CreateMetadata: &api.CreateMetadata{Name: backupScheduleName, OrgId: OrgID, Uid: backupScheduleUID},
		Suspend:        false,
		SchedulePolicyRef: &api.ObjectRef{
			Name: schPolicyName,
			Uid:  schPolicyUID,
		},
	}
	_, err = backupDriver.UpdateBackupSchedule(ctx, bkpScheduleSuspendRequest)
	return err
}

// NamespaceLabelBackupSuccessCheck verifies if the labeled namespaces are backed up and checks for labels applied to backups
func NamespaceLabelBackupSuccessCheck(backupName string, ctx context1.Context, listOfLabelledNamespaces []string, namespaceLabel string) error {
	backupDriver := Inst().Backup
	log.Infof("Getting the Uid of backup %v", backupName)
	backupUid, err := backupDriver.GetBackupUID(ctx, backupName, BackupOrgID)
	if err != nil {
		return err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUid,
		OrgId: BackupOrgID,
	}
	resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return err
	}
	namespaceList := resp.GetBackup().GetNamespaces()
	log.Infof("The list of namespaces backed up are %v", namespaceList)
	if !AreStringSlicesEqual(namespaceList, listOfLabelledNamespaces) {
		return fmt.Errorf("list of namespaces backed up are %v which is not same as expected %v", namespaceList, listOfLabelledNamespaces)
	}
	backupLabels := resp.GetBackup().GetNsLabelSelectors()
	log.Infof("The list of labels applied to backup are %v", backupLabels)
	expectedLabels := strings.Split(namespaceLabel, ",")
	actualLabels := strings.Split(backupLabels, ",")
	AreStringSlicesEqual(expectedLabels, actualLabels)
	if !AreStringSlicesEqual(expectedLabels, actualLabels) {
		return fmt.Errorf("labels applied to backup are %v which is not same as expected %v", actualLabels, expectedLabels)
	}
	return nil
}

// AddLabelsToMultipleNamespaces add labels to multiple namespace
func AddLabelsToMultipleNamespaces(labels map[string]string, namespaces []string) error {
	for _, namespace := range namespaces {
		err := Inst().S.AddNamespaceLabel(namespace, labels)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteLabelsFromMultipleNamespaces delete labels from multiple namespace
func DeleteLabelsFromMultipleNamespaces(labels map[string]string, namespaces []string) error {
	for _, namespace := range namespaces {
		err := Inst().S.RemoveNamespaceLabel(namespace, labels)
		if err != nil {
			return err
		}
	}
	return nil
}

// GenerateRandomLabels creates random label
func GenerateRandomLabels(number int) map[string]string {
	labels := make(map[string]string)
	randomString := uuid.New()
	for i := 0; i < number; i++ {
		key := fmt.Sprintf("%v-%v", i, randomString)
		value := randomString
		labels[key] = value
	}
	return labels
}

// MapToKeyValueString converts a map of string keys and value to a comma separated string of "key=value"
func MapToKeyValueString(m map[string]string) string {
	var pairs []string
	for k, v := range m {
		pairs = append(pairs, k+"="+v)
	}
	return strings.Join(pairs, ",")
}

// VerifyLicenseConsumedCount verifies the consumed license count for px-backup
func VerifyLicenseConsumedCount(ctx context1.Context, OrgId string, expectedLicenseConsumedCount int64) error {
	licenseInspectRequestObject := &api.LicenseInspectRequest{
		OrgId: OrgId,
	}
	licenseCountCheck := func() (interface{}, bool, error) {
		licenseInspectResponse, err := Inst().Backup.InspectLicense(ctx, licenseInspectRequestObject)
		if err != nil {
			return "", false, err
		}
		licenseResponseInfoFeatureInfo := licenseInspectResponse.GetLicenseRespInfo().GetFeatureInfo()
		if licenseResponseInfoFeatureInfo[0].Consumed == expectedLicenseConsumedCount {
			return "", false, nil
		}
		return "", true, fmt.Errorf("actual license count:%v, expected license count: %v", licenseInspectResponse.GetLicenseRespInfo().GetFeatureInfo()[0].Consumed, expectedLicenseConsumedCount)
	}
	_, err := task.DoRetryWithTimeout(licenseCountCheck, licenseCountUpdateTimeout, licenseCountUpdateRetryTime)
	if err != nil {
		return err
	}
	return err
}

// DeleteRule deletes backup rule
func DeleteRule(ruleName string, orgId string, ctx context1.Context) error {
	ruleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, ruleName)
	if err != nil {
		return err
	}
	deleteRuleReq := &api.RuleDeleteRequest{
		OrgId: orgId,
		Name:  ruleName,
		Uid:   ruleUid,
	}
	_, err = Inst().Backup.DeleteRule(ctx, deleteRuleReq)
	if err != nil {
		return err
	}
	return nil
}

// SafeAppend appends elements to a given slice in a thread-safe manner using a provided mutex
func SafeAppend(mu *sync.Mutex, slice interface{}, elements ...interface{}) interface{} {
	mu.Lock()
	defer mu.Unlock()
	sliceValue := reflect.ValueOf(slice)
	for _, elem := range elements {
		elemValue := reflect.ValueOf(elem)
		sliceValue = reflect.Append(sliceValue, elemValue)
	}
	return sliceValue.Interface()
}

// TaskHandler executes the given task on each input in the taskInputs collection, either sequentially
// * or in parallel, depending on the specified execution mode. It also returns an error when taskInputs is not
// * of type slice or map.
// *
// * Parameters:
// *
// * taskInputs: The collection of inputs to operate on (either a slice or map).
// * task:       The function to execute on each input. If the function takes one argument,
// *
// *	it will be passed the input value. If it takes two arguments, the first
// *	will be the input key or index, and the second will be the input value.
// *
// * executionMode: The mode to use for executing the task, either "Sequential" or "Parallel".
// *
// * # Example
// *
// * The original code:
// *
// *	for _, value := range taskInputs / slice or map / {
// *	    task(value)
// *	}
// *
// * or
// *
// *	for index, value := range taskInputs / slice / {
// *	    task(index, value)
// *	}
// *
// * or
// *
// *	for key, value := range taskInputs / map / {
// *	    task(key, value)
// *	}
// *
// * The original code uses a common pattern for iterating over a slice or map of inputs and calling the 'task'
// * function for each input. To simplify this pattern and allow for concurrent execution of the 'task'
// * function, you can replace the for loops with a call to TaskHandler(taskInputs, task, executionMode), where
// * 'executionMode' is either 'Parallel' or 'Sequential'.
func TaskHandler(taskInputs interface{}, task interface{}, executionMode ExecutionMode) error {
	v := reflect.ValueOf(taskInputs)
	var keys []reflect.Value
	isMap := false
	if v.Kind() == reflect.Map {
		keys = v.MapKeys()
		isMap = true
	} else if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		keys = make([]reflect.Value, v.Len())
		for i := 0; i < v.Len(); i++ {
			keys[i] = v.Index(i)
		}
	} else {
		return fmt.Errorf("instead of %#v, type of taskInputs should be a slice or map", v.Kind().String())
	}
	length := len(keys)
	if length == 0 {
		return nil
	} else if length == 1 {
		executionMode = Sequential
	}
	fnValue := reflect.ValueOf(task)
	numArgs := fnValue.Type().NumIn()
	callTask := func(key, value reflect.Value) {
		in := make([]reflect.Value, numArgs)
		if numArgs == 1 {
			in[0] = value
		} else {
			in[0] = key
			in[1] = value
		}
		fnValue.Call(in)
	}
	if executionMode == Sequential {
		for i := 0; i < length; i++ {
			if isMap {
				callTask(keys[i], v.MapIndex(keys[i]))
			} else {
				callTask(reflect.ValueOf(i), keys[i])
			}
		}
	} else {
		var wg sync.WaitGroup
		for i := 0; i < length; i++ {
			wg.Add(1)
			go func(i int) {
				defer GinkgoRecover()
				defer wg.Done()
				if isMap {
					callTask(keys[i], v.MapIndex(keys[i]))
				} else {
					callTask(reflect.ValueOf(i), keys[i])
				}
			}(i)
		}
		wg.Wait()
	}
	return nil
}

// FetchNamespacesFromBackup fetches the namespace from backup
func FetchNamespacesFromBackup(ctx context1.Context, backupName string, orgID string) ([]string, error) {
	var backedUpNamespaces []string
	backupUid, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return nil, err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUid,
		OrgId: orgID,
	}
	resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return nil, err
	}
	backedUpNamespaces = resp.GetBackup().GetNamespaces()
	return backedUpNamespaces, err
}

// AreSlicesEqual verifies if two slices are equal or not
func AreSlicesEqual(slice1, slice2 interface{}) bool {
	v1 := reflect.ValueOf(slice1)
	v2 := reflect.ValueOf(slice2)
	if v1.Len() != v2.Len() {
		return false
	}
	m := make(map[interface{}]int)
	for i := 0; i < v2.Len(); i++ {
		m[v2.Index(i).Interface()]++
	}
	for i := 0; i < v1.Len(); i++ {
		if m[v1.Index(i).Interface()] == 0 {
			return false
		}
		m[v1.Index(i).Interface()]--
	}
	return true
}

// AreStringSlicesEqual compares two slices of string
func AreStringSlicesEqual(slice1 []string, slice2 []string) bool {
	if len(slice1) != len(slice2) {
		return false
	}
	sort.Sort(sort.StringSlice(slice1))
	sort.Sort(sort.StringSlice(slice2))
	for i, v := range slice1 {
		if v != slice2[i] {
			return false
		}
	}
	return true
}

// GetNextScheduleBackupName returns the upcoming schedule backup after it has been initiated
func GetNextScheduleBackupName(scheduleName string, scheduleInterval time.Duration, ctx context1.Context) (string, error) {
	var nextScheduleBackupName string
	allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(ctx, scheduleName, BackupOrgID)
	if err != nil {
		return "", err
	}
	currentScheduleBackupCount := len(allScheduleBackupNames)
	nextScheduleBackupOrdinal := currentScheduleBackupCount + 1
	checkOrdinalScheduleBackupCreation := func() (interface{}, bool, error) {
		ordinalScheduleBackupName, err := GetOrdinalScheduleBackupName(ctx, scheduleName, nextScheduleBackupOrdinal, BackupOrgID)
		log.InfoD("schedule name %s, Next schedule backup name: %s", scheduleName, ordinalScheduleBackupName)
		if err != nil {
			return "", true, err
		}
		backupDriver := Inst().Backup
		backupUid, err := backupDriver.GetBackupUID(ctx, ordinalScheduleBackupName, BackupOrgID)
		backupInspectRequest := &api.BackupInspectRequest{
			Name:  ordinalScheduleBackupName,
			Uid:   backupUid,
			OrgId: BackupOrgID,
		}
		resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
		log.InfoD("Inspect obj %s", resp)
		return ordinalScheduleBackupName, false, nil
	}
	log.InfoD("Waiting for [%d] minutes for the next schedule backup to be triggered", scheduleInterval)
	time.Sleep(scheduleInterval * time.Minute)
	nextScheduleBackup, err := task.DoRetryWithTimeout(checkOrdinalScheduleBackupCreation, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
	if err != nil {
		return "", err
	}
	nextScheduleBackupName = nextScheduleBackup.(string)
	return nextScheduleBackupName, nil
}

// GetNextCompletedScheduleBackupName returns the upcoming schedule backup
// after it has been created and checked for success status
func GetNextCompletedScheduleBackupName(ctx context1.Context, scheduleName string, scheduleInterval time.Duration) (string, error) {
	nextScheduleBackupName, err := GetNextScheduleBackupName(scheduleName, scheduleInterval, ctx)
	if err != nil {
		return "", err
	}
	log.InfoD("Next schedule backup name [%s]", nextScheduleBackupName)
	err = BackupSuccessCheck(nextScheduleBackupName, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return "", err
	}
	return nextScheduleBackupName, nil
}

// GetNextCompletedScheduleBackupNameWithValidation returns the upcoming schedule backup
// after it has been created and checked for success status and validated
func GetNextCompletedScheduleBackupNameWithValidation(ctx context1.Context, scheduleName string, scheduledAppContextsToBackup []*scheduler.Context, scheduleInterval time.Duration) (string, error) {
	nextScheduleBackupName, err := GetNextScheduleBackupName(scheduleName, scheduleInterval, ctx)
	if err != nil {
		return "", err
	}
	log.InfoD("Next schedule backup name [%s]", nextScheduleBackupName)
	err = BackupSuccessCheckWithValidation(ctx, nextScheduleBackupName, scheduledAppContextsToBackup, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
	if err != nil {
		return "", err
	}
	return nextScheduleBackupName, nil
}

// GetNextPeriodicScheduleBackupName returns next periodic schedule backup name with the given interval
func GetNextPeriodicScheduleBackupName(scheduleName string, scheduleInterval time.Duration, ctx context1.Context) (string, error) {
	var nextScheduleBackupName string
	allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(ctx, scheduleName, BackupOrgID)
	if err != nil {
		return "", err
	}
	currentScheduleBackupCount := len(allScheduleBackupNames)
	nextScheduleBackupOrdinal := currentScheduleBackupCount + 1
	checkOrdinalScheduleBackupCreation := func() (interface{}, bool, error) {
		ordinalScheduleBackupName, err := GetOrdinalScheduleBackupName(ctx, scheduleName, nextScheduleBackupOrdinal, BackupOrgID)
		if err != nil {
			return "", true, err
		}
		return ordinalScheduleBackupName, false, nil
	}
	log.InfoD("Waiting for %v minutes for the next schedule backup to be triggered", scheduleInterval)
	time.Sleep(scheduleInterval * time.Minute)
	nextScheduleBackup, err := task.DoRetryWithTimeout(checkOrdinalScheduleBackupCreation, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
	if err != nil {
		return "", err
	}
	log.InfoD("Next schedule backup name [%s]", nextScheduleBackup.(string))
	err = BackupSuccessCheck(nextScheduleBackup.(string), BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return "", err
	}
	nextScheduleBackupName = nextScheduleBackup.(string)
	return nextScheduleBackupName, nil
}

// RemoveElementByValue remove the first occurence of the element from the array.Pass a pointer to the array and the element by value.
func RemoveElementByValue(arr interface{}, value interface{}) error {
	v := reflect.ValueOf(arr)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("removeElementByValue: not a pointer")
	}
	v = v.Elem()
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("removeElementByValue: not a slice pointer")
	}
	for i := 0; i < v.Len(); i++ {
		if v.Index(i).Interface() == value {
			v.Set(reflect.AppendSlice(v.Slice(0, i), v.Slice(i+1, v.Len())))
			break
		}
	}
	return nil
}

// IsFullBackup checks if given backup is full backup or not
func IsFullBackup(backupName string, orgID string, ctx context1.Context) error {
	backupUid, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return err
	}
	backupInspectReq := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
		Uid:   backupUid,
	}
	resp, err := Inst().Backup.InspectBackup(ctx, backupInspectReq)
	if err != nil {
		return err
	}
	for _, vol := range resp.GetBackup().GetVolumes() {
		backupId := vol.GetBackupId()
		log.Infof("BackupID of backup [%s]: [%s]", backupName, backupId)
		if strings.HasSuffix(backupId, "-incr") {
			return fmt.Errorf("backup [%s] is an incremental backup", backupName)
		}
	}
	return nil
}

// RemoveLabelFromNodesIfPresent remove the given label from the given node if present
func RemoveLabelFromNodesIfPresent(node node.Node, expectedKey string) error {
	nodeLabels, err := core.Instance().GetLabelsOnNode(node.Name)
	if err != nil {
		return err
	}
	for key := range nodeLabels {
		if key == expectedKey {
			log.InfoD("Removing the applied label with key %s from node %s", expectedKey, node.Name)
			err = Inst().S.RemoveLabelOnNode(node, expectedKey)
			if err != nil {
				return err
			}
			return nil
		}
	}
	return nil
}

// ValidatePodByLabel validates if the pod with specified label is in a running state
func ValidatePodByLabel(label map[string]string, namespace string, timeout time.Duration, retryInterval time.Duration) error {
	log.Infof("Checking if pods with label %v are running in namespace %s", label, namespace)
	pods, err := core.Instance().GetPods(namespace, label)
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		err = core.Instance().ValidatePod(&pod, timeout, retryInterval)
		if err != nil {
			return fmt.Errorf("failed to validate pod [%s] with error - %s", pod.GetName(), err.Error())
		}
	}
	return nil
}

// IsMongoDBReady validates if the mongo db pods in Px-Backup namespace are healthy enough for Px-Backup to function
func IsMongoDBReady() error {
	log.Infof("Verify that at least 2 mongodb pods are in Ready state at the end of the testcase")
	errorString := "mongodb pods are not ready yet"
	pxbNamespace, err := backup.GetPxBackupNamespace()
	if err != nil {
		return err
	}
	mongoDBPodStatus := func() (interface{}, bool, error) {
		statefulSet, err := apps.Instance().GetStatefulSet(MongodbStatefulset, pxbNamespace)
		if err != nil {
			return "", true, err
		}

		// Check if all 3 mongo pods have come up
		if statefulSet.Status.ReadyReplicas < 3 {
			return "", true, fmt.Errorf("%s. expected ready pods - %d, actual ready pods - %d",
				errorString, 3, statefulSet.Status.ReadyReplicas)

		}
		return "", false, nil
	}
	_, err = DoRetryWithTimeoutWithGinkgoRecover(mongoDBPodStatus, 30*time.Minute, 30*time.Second)
	if err != nil {
		if strings.Contains(err.Error(), errorString) {
			statefulSet, err := apps.Instance().GetStatefulSet(MongodbStatefulset, pxbNamespace)

			// Check atleast 2 mongo pods are up if 3 mongo pods have not come up even after waiting for 30 min
			// Ideally we would expect all 3 pods to be ready but because of intermittent issues, we are limiting to 2
			// Px-Backup would function with just 2 mongo DB pods in healthy state.
			// TODO: Remove the limit to check for only 2 out of 3 pods once fixed
			// Tracking JIRAs: https://portworx.atlassian.net/browse/PB-3105, https://portworx.atlassian.net/browse/PB-3481
			log.Infof("Validating atleast 2 mongodb pods are ready")
			if statefulSet.Status.ReadyReplicas < 2 {
				return err
			}
		}
	}
	statefulSet, err := apps.Instance().GetStatefulSet(MongodbStatefulset, pxbNamespace)
	if err != nil {
		return err
	}
	log.Infof("Number of mongodb pods in Ready state are %v", statefulSet.Status.ReadyReplicas)
	return nil
}

// DeleteAppNamespace deletes the given namespace and wait for termination
func DeleteAppNamespace(namespace string) error {
	var ns *corev1.Namespace
	k8sCore := core.Instance()
	err := k8sCore.DeleteNamespace(namespace)
	if err != nil {
		return err
	}
	namespaceDeleteCheck := func() (interface{}, bool, error) {
		nsObj, err := core.Instance().GetNamespace(namespace)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				log.Infof("Namespace - %s is not found and hence deleted", namespace)
				return "", false, nil
			} else {
				return "", false, err
			}
		}
		if nsObj.Status.Phase == "Terminating" {
			return "", true, fmt.Errorf("namespace - %s is in %s phase ", namespace, nsObj.Status.Phase)
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(namespaceDeleteCheck, namespaceDeleteTimeout, jobDeleteRetryTime)
	if err != nil {
		ns, err = k8sCore.GetNamespace(namespace)
		if err != nil {
			return err
		}
		if ns.Status.Phase == "Terminating" {
			log.Infof("Namespace - %s is in %s phase ", namespace, ns.Status.Phase)
			err = DeleteTerminatingNamespace(namespace)
			return err
		}
	}
	return nil
}

// DeleteTerminatingNamespace deletes the given namespace which has been in terminating state
func DeleteTerminatingNamespace(namespace string) error {
	k8sCore := core.Instance()
	ns, err := k8sCore.GetNamespace(namespace)
	if err != nil {
		return err
	}
	if ns.Status.Phase == "Terminating" {
		log.Infof("Namespace - %s is in %s phase ", namespace, ns.Status.Phase)
		log.Infof("Finalizers to be set to nil - %v", ns.Spec.Finalizers)
		ns.Spec.Finalizers = nil
		ns, err = k8sCore.UpdateNamespace(ns)
		if err != nil {
			return err
		}
		namespaceDeleteCheck := func() (interface{}, bool, error) {
			nsObj, err := core.Instance().GetNamespace(namespace)
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					log.Infof("Namespace - %s is not found and hence deleted", namespace)
					return "", false, nil
				} else {
					return "", false, err
				}
			}
			if nsObj.Status.Phase == "Terminating" {
				return "", true, fmt.Errorf("namespace - %s is in %s phase ", namespace, nsObj.Status.Phase)
			}
			return "", false, nil
		}
		_, err = task.DoRetryWithTimeout(namespaceDeleteCheck, namespaceDeleteTimeout, jobDeleteRetryTime)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("Namespace [%s] expected to be in Terminating phase but is in %s phase ", namespace, ns.Status.Phase)
	}
	return nil
}

// RegisterCluster adds the cluster with the given name
func RegisterCluster(clusterName string, cloudCredName string, orgID string, ctx context1.Context) error {
	var kubeconfigPath string
	var err error
	kubeConfigs := os.Getenv("KUBECONFIGS")
	if kubeConfigs == "" {
		return fmt.Errorf("unable to get KUBECONFIGS from Environment variable")
	}
	kubeconfigList := strings.Split(kubeConfigs, ",")
	DumpKubeconfigs(kubeconfigList)
	// Register cluster with backup driver
	log.InfoD("Create cluster [%s] in org [%s]", clusterName, orgID)
	if clusterName == SourceClusterName {
		kubeconfigPath, err = GetSourceClusterConfigPath()
	} else if clusterName == DestinationClusterName {
		kubeconfigPath, err = GetDestinationClusterConfigPath()
	} else {
		return fmt.Errorf("registering %s cluster not implemented", clusterName)
	}
	if err != nil {
		return err
	}
	log.Infof("Save cluster %s kubeconfig to %s", clusterName, kubeconfigPath)
	clusterStatus := func() (interface{}, bool, error) {
		err = CreateCluster(clusterName, kubeconfigPath, orgID, cloudCredName, "", ctx)
		if err != nil && !strings.Contains(err.Error(), "already exists with status: Online") {
			return "", true, err
		}
		createClusterStatus, err := Inst().Backup.GetClusterStatus(orgID, clusterName, ctx)
		if err != nil {
			return "", true, err
		}
		if createClusterStatus == api.ClusterInfo_StatusInfo_Online {
			return "", false, nil
		}
		return "", true, fmt.Errorf("the %s cluster state is not Online yet", clusterName)
	}
	_, err = task.DoRetryWithTimeout(clusterStatus, clusterCreationTimeout, ClusterCreationRetryTime)
	if err != nil {
		return err
	}
	return nil
}

// NamespaceExistsInNamespaceMapping checks if namespace is present in map of namespace mapping
func NamespaceExistsInNamespaceMapping(namespaceMap map[string]string, namespaces []string) bool {
	for _, namespace := range namespaces {
		if _, ok := namespaceMap[namespace]; !ok {
			fmt.Printf("%s is not a present in namespaces %v", namespace, namespaces)
			return false
		}
	}
	return true
}

// RemoveNamespaceLabelForMultipleNamespaces removes labels from multiple namespace
func RemoveNamespaceLabelForMultipleNamespaces(labels map[string]string, namespaces []string) error {
	for _, namespace := range namespaces {
		err := Inst().S.RemoveNamespaceLabel(namespace, labels)
		if err != nil {
			return err
		}
	}
	return nil
}

func AddSourceCluster(ctx context1.Context) error {
	err := RegisterCluster(SourceClusterName, "", BackupOrgID, ctx)
	if err != nil {
		return err
	}
	return nil
}

func AddDestinationCluster(ctx context1.Context) error {
	err := RegisterCluster(DestinationClusterName, "", BackupOrgID, ctx)
	if err != nil {
		return err
	}
	return nil
}

// GenerateRandomLabelsWithMaxChar creates random label with max characters
func GenerateRandomLabelsWithMaxChar(number int, charLimit int) map[string]string {
	labels := make(map[string]string)
	for i := 0; i < number; i++ {
		key := RandomString(charLimit)
		value := uuid.New()
		labels[key] = value
	}
	return labels
}

// GetCustomBucketName creates a custom bucket and returns name
func GetCustomBucketName(provider string, testName string) string {
	var customBucket string
	customBucket = fmt.Sprintf("%s-%s-%s-%v", provider, testName, RandomString(5), time.Now().Unix())
	if provider == drivers.ProviderAws {
		CreateBucket(provider, customBucket)
	}
	return customBucket
}

// ValidateBackupLocation validates the given backup location
func ValidateBackupLocation(ctx context1.Context, orgID string, backupLocationName string, uid string) error {
	backupLocationValidateRequest := &api.BackupLocationValidateRequest{
		OrgId: orgID,
		Name:  backupLocationName,
		Uid:   uid,
	}
	_, err := Inst().Backup.ValidateBackupLocation(ctx, backupLocationValidateRequest)
	return err
}

// GetAppLabelFromSpec gets the label of the pod from the spec
func GetAppLabelFromSpec(AppContextsMapping *scheduler.Context) (map[string]string, error) {
	labelMap := make(map[string]string)
	for _, specObj := range AppContextsMapping.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			labelMap = k8s.MergeMaps(labelMap, obj.Spec.Selector.MatchLabels)
		} else if obj, ok := specObj.(*kubevirtv1.VirtualMachine); ok {
			labelMap = k8s.MergeMaps(labelMap, obj.Spec.Template.ObjectMeta.Labels)
		}
	}
	log.Infof("labelMap - %+v", labelMap)
	if len(labelMap) == 0 {
		return nil, fmt.Errorf("unable to find the label for %s", AppContextsMapping.App.Key)
	}
	return labelMap, nil
}

// GetVolumeMounts gets the volume mounts from the spec
func GetVolumeMounts(AppContextsMapping *scheduler.Context) ([]string, error) {
	var volumeMounts []string
	for _, specObj := range AppContextsMapping.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			mountPoints := obj.Spec.Template.Spec.Containers[0].VolumeMounts
			for index := range mountPoints {
				volumeMounts = append(volumeMounts, mountPoints[index].MountPath)
			}
			return volumeMounts, nil
		}
	}
	return nil, fmt.Errorf("unable to find the mount point for %s", AppContextsMapping.App.Key)
}

type BackupTypeForCSI string

const (
	NativeCSIWithOffloadToS3 BackupTypeForCSI = "csi_offload_s3"
	NativeCSI                BackupTypeForCSI = "native_csi"
	DirectKDMP               BackupTypeForCSI = "direct_kdmp"
	NativeAzure              BackupTypeForCSI = "azure"
)

// AdditionalBackupRequestParams decorates the backupRequest with additional parameters required
// when BACKUP_TYPE is Native CSI, Direct KDMP or CSI snapshot with offload to S3
func AdditionalBackupRequestParams(backupRequest *api.BackupCreateRequest) error {
	switch strings.ToLower(os.Getenv("BACKUP_TYPE")) {
	case string(NativeCSIWithOffloadToS3):
		log.Infof("Detected backup type - %s", NativeCSIWithOffloadToS3)
		backupRequest.BackupType = api.BackupCreateRequest_Generic
		var csiSnapshotClassName string
		var err error
		if csiSnapshotClassName, err = GetCsiSnapshotClassName(); err != nil {
			return err
		}
		backupRequest.CsiSnapshotClassName = csiSnapshotClassName
	case string(NativeCSI):
		log.Infof("Detected backup type - %s", NativeCSI)
		backupRequest.BackupType = api.BackupCreateRequest_Normal
		var csiSnapshotClassName string
		var err error
		if csiSnapshotClassName, err = GetCsiSnapshotClassName(); err != nil {
			return err
		}
		backupRequest.CsiSnapshotClassName = csiSnapshotClassName
	case string(NativeAzure):
		log.Infof("Detected backup type - %s", NativeAzure)
		backupRequest.BackupType = api.BackupCreateRequest_Normal
		var csiSnapshotClassName string
		var err error
		if csiSnapshotClassName, err = GetCsiSnapshotClassName(); err != nil {
			return err
		}
		backupRequest.CsiSnapshotClassName = csiSnapshotClassName
	case string(DirectKDMP):
		log.Infof("Detected backup type - %s", DirectKDMP)
		backupRequest.BackupType = api.BackupCreateRequest_Generic
	default:
		log.Infof("Environment variable BACKUP_TYPE is not provided")
	}
	return nil
}

// AdditionalScheduledBackupRequestParams decorates the backupScheduleRequest with additional parameters required
// when BACKUP_TYPE is Native CSI, Direct KDMP or CSI snapshot with offload to S3
func AdditionalScheduledBackupRequestParams(backupScheduleRequest *api.BackupScheduleCreateRequest) error {
	switch strings.ToLower(os.Getenv("BACKUP_TYPE")) {
	case string(NativeCSIWithOffloadToS3):
		log.Infof("Detected backup type - %s", NativeCSIWithOffloadToS3)
		backupScheduleRequest.BackupType = api.BackupScheduleCreateRequest_Generic
		var csiSnapshotClassName string
		var err error
		if csiSnapshotClassName, err = GetCsiSnapshotClassName(); err != nil {
			return err
		}
		backupScheduleRequest.CsiSnapshotClassName = csiSnapshotClassName
	case string(NativeCSI):
		log.Infof("Detected backup type - %s", NativeCSI)
		backupScheduleRequest.BackupType = api.BackupScheduleCreateRequest_Normal
		var csiSnapshotClassName string
		var err error
		if csiSnapshotClassName, err = GetCsiSnapshotClassName(); err != nil {
			return err
		}
		backupScheduleRequest.CsiSnapshotClassName = csiSnapshotClassName
	case string(NativeAzure):
		log.Infof("Detected backup type - %s", NativeAzure)
		backupScheduleRequest.BackupType = api.BackupScheduleCreateRequest_Normal
		var csiSnapshotClassName string
		var err error
		if csiSnapshotClassName, err = GetCsiSnapshotClassName(); err != nil {
			return err
		}
		backupScheduleRequest.CsiSnapshotClassName = csiSnapshotClassName
	case string(DirectKDMP):
		log.Infof("Detected backup type - %s", DirectKDMP)
		backupScheduleRequest.BackupType = api.BackupScheduleCreateRequest_Generic
	default:
		log.Infof("Environment variable BACKUP_TYPE is not provided")
	}
	return nil
}

// CreateRestoreWithProjectMapping creates restore with project mapping
func CreateRestoreWithProjectMapping(restoreName string, backupName string, namespaceMapping map[string]string, clusterName string,
	orgID string, ctx context1.Context, storageClassMapping map[string]string, rancherProjectMapping map[string]string, rancherProjectNameMapping map[string]string) error {

	var bkp *api.BackupObject
	var bkpUid string
	backupDriver := Inst().Backup
	log.Infof("Getting the UID of the backup %s needed to be restored", backupName)
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID}
	curBackups, err := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	if err != nil {
		return err
	}
	for _, bkp = range curBackups.GetBackups() {
		if bkp.Name == backupName {
			bkpUid = bkp.Uid
			break
		}
	}
	createRestoreReq := &api.RestoreCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  restoreName,
			OrgId: orgID,
		},
		Backup:              backupName,
		Cluster:             clusterName,
		NamespaceMapping:    namespaceMapping,
		StorageClassMapping: storageClassMapping,
		BackupRef: &api.ObjectRef{
			Name: backupName,
			Uid:  bkpUid,
		},
		RancherProjectMapping:     rancherProjectMapping,
		RancherProjectNameMapping: rancherProjectNameMapping,
	}
	_, err = backupDriver.CreateRestore(ctx, createRestoreReq)
	if err != nil {
		return err
	}
	err = RestoreSuccessCheck(restoreName, orgID, MaxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Restore [%s] created successfully", restoreName)

	return nil
}

// CreateRestoreOnRancherWithoutCheck creates restore with project mapping
func CreateRestoreOnRancherWithoutCheck(restoreName string, backupName string, namespaceMapping map[string]string, clusterName string,
	orgID string, ctx context1.Context, storageClassMapping map[string]string, rancherProjectMapping map[string]string, rancherProjectNameMapping map[string]string, replacePolicy ReplacePolicyType) error {

	var bkp *api.BackupObject
	var bkpUid string
	backupDriver := Inst().Backup
	log.Infof("Getting the UID of the backup %s needed to be restored", backupName)
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID}
	curBackups, err := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	if err != nil {
		return err
	}
	for _, bkp = range curBackups.GetBackups() {
		if bkp.Name == backupName {
			bkpUid = bkp.Uid
			break
		}
	}
	createRestoreReq := &api.RestoreCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  restoreName,
			OrgId: orgID,
		},
		Backup:              backupName,
		Cluster:             clusterName,
		NamespaceMapping:    namespaceMapping,
		StorageClassMapping: storageClassMapping,
		BackupRef: &api.ObjectRef{
			Name: backupName,
			Uid:  bkpUid,
		},
		ReplacePolicy:             api.ReplacePolicy_Type(replacePolicy),
		RancherProjectMapping:     rancherProjectMapping,
		RancherProjectNameMapping: rancherProjectNameMapping,
	}
	_, err = backupDriver.CreateRestore(ctx, createRestoreReq)
	if err != nil {
		return err
	}
	return nil
}

// IsClusterPresent checks whether the cluster is present or not
func IsClusterPresent(clusterName string, ctx context1.Context, orgID string) (bool, error) {
	clusterEnumerateRequest := &api.ClusterEnumerateRequest{
		OrgId:          orgID,
		IncludeSecrets: false,
	}
	clusterObjs, err := Inst().Backup.EnumerateCluster(ctx, clusterEnumerateRequest)
	if err != nil {
		return false, err
	}
	for _, clusterObj := range clusterObjs.GetClusters() {
		if clusterObj.GetName() == clusterName {
			log.Infof("Cluster [%s] is present", clusterName)
			return true, nil
		}
	}
	return false, nil
}

// GetConfigObj reads the configuration file and returns a BackupCloudConfig object.
func GetConfigObj() (backup.BackupCloudConfig, error) {
	var config backup.BackupCloudConfig
	var found bool
	cmList, err := core.Instance().ListConfigMap("default", metav1.ListOptions{})
	log.FailOnError(err, fmt.Sprintf("Error listing Configmaps in default namespace"))
	for _, cm := range cmList.Items {
		if cm.Name == cloudCredConfigMap {
			found = true
			break
		}
	}
	if found {
		log.Infof(fmt.Sprintf("Configmap with name %s found in the Configmaps list", cloudCredConfigMap))
		cm, err := core.Instance().GetConfigMap(cloudCredConfigMap, "default")
		if err != nil {
			log.Errorf("Error reading Configmap: %v", err)
			return config, err
		}
		log.Infof("Fetch the cloud-config from the Configmap")
		configData := cm.Data["cloud-json"]
		err = json.Unmarshal([]byte(configData), &config)
		return config, nil
	}
	log.Warnf(fmt.Sprintf("Configmap with name %s not found in the Configmaps list, if you are running on any cloud provider please provide Configmap", cloudCredConfigMap))
	return config, nil
}

// CreateRuleForBackupWithMultipleApplications creates backup rule for multiple application in one rule
func CreateRuleForBackupWithMultipleApplications(orgID string, appList []string, ctx context1.Context, appParameters ...map[string]backup.AppRule) (string, string, error) {
	var (
		preUid             string
		preRuleName        string
		postRuleName       string
		postUid            string
		preActionValue     []string
		preContainer       []string
		postActionValue    []string
		postContainer      []string
		postBackground     []bool
		postRunInSinglePod []bool
		preBackground      []bool
		preRunInSinglePod  []bool
		preRulesInfo       api.RulesInfo
		postRulesInfo      api.RulesInfo
		prePodSelector     []map[string]string
		postPodSelector    []map[string]string
		appParameter       map[string]backup.AppRule
	)
	if len(appParameters) == 0 {
		appParameter = AppRuleMaster
	} else {
		appParameter = appParameters[0]
	}

	for i := 0; i < len(appList); i++ {
		appRule := appParameter[appList[i]]
		if reflect.DeepEqual(appRule.PreRule, backup.PreRule{}) {
			log.Infof("Pre rule not required for application %v", appList[i])
		} else {
			for j := 0; j < len(appRule.PreRule.Rule.PodSelectorList); j++ {
				ps := strings.Split(appRule.PreRule.Rule.PodSelectorList[j], "=")
				psMap := make(map[string]string)
				psMap[ps[0]] = ps[1]
				prePodSelector = append(prePodSelector, psMap)
				preActionValue = append(preActionValue, appRule.PreRule.Rule.ActionList[j])
				backgroundVal, _ := strconv.ParseBool(appRule.PreRule.Rule.Background[j])
				preBackground = append(preBackground, backgroundVal)
				podVal, _ := strconv.ParseBool(appRule.PreRule.Rule.RunInSinglePod[j])
				preRunInSinglePod = append(preRunInSinglePod, podVal)
				containerName := fmt.Sprintf("%s-%s", "container", appList[i])
				preContainer = append(preContainer, os.Getenv(containerName))
			}
		}

		if reflect.DeepEqual(appRule.PostRule, backup.PostRule{}) {
			log.Infof("Post rule not required for application %v", appList[i])
		} else {
			for j := 0; j < len(appRule.PostRule.Rule.PodSelectorList); j++ {
				ps := strings.Split(appRule.PostRule.Rule.PodSelectorList[j], "=")
				psMap := make(map[string]string)
				psMap[ps[0]] = ps[1]
				postPodSelector = append(postPodSelector, psMap)
				postActionValue = append(postActionValue, appRule.PostRule.Rule.ActionList[j])
				backgroundVal, _ := strconv.ParseBool(appRule.PostRule.Rule.Background[j])
				postBackground = append(postBackground, backgroundVal)
				podVal, _ := strconv.ParseBool(appRule.PostRule.Rule.RunInSinglePod[j])
				postRunInSinglePod = append(postRunInSinglePod, podVal)
				containerName := fmt.Sprintf("%s-%s", "container", appList[i])
				postContainer = append(postContainer, os.Getenv(containerName))
			}
		}

	}
	totalPreRules := len(preActionValue)
	totalPostRules := len(postActionValue)

	if totalPreRules != 0 {
		preRuleName = fmt.Sprintf("pre-rule-%v", RandomString(5))
		rulesInfoRuleItem := make([]api.RulesInfo_RuleItem, totalPreRules)
		for i := 0; i < totalPreRules; i++ {
			ruleAction := api.RulesInfo_Action{Background: preBackground[i], RunInSinglePod: preRunInSinglePod[i],
				Value: preActionValue[i]}
			var actions = []*api.RulesInfo_Action{&ruleAction}
			rulesInfoRuleItem[i].PodSelector = prePodSelector[i]
			rulesInfoRuleItem[i].Actions = actions
			rulesInfoRuleItem[i].Container = preContainer[i]
			preRulesInfo.Rules = append(preRulesInfo.Rules, &rulesInfoRuleItem[i])
		}
		PreRuleCreateReq := &api.RuleCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  preRuleName,
				OrgId: orgID,
			},
			RulesInfo: &preRulesInfo,
		}

		_, err := Inst().Backup.CreateRule(ctx, PreRuleCreateReq)
		if err != nil {
			err = fmt.Errorf("failed to create backup pre-rules: [%v]", err)
			return "", "", err
		}
	}

	if totalPostRules != 0 {
		postRuleName = fmt.Sprintf("post-rule-%v", RandomString(5))
		rulesInfoRuleItem := make([]api.RulesInfo_RuleItem, totalPostRules)
		for i := 0; i < totalPostRules; i++ {
			ruleAction := api.RulesInfo_Action{Background: postBackground[i], RunInSinglePod: postRunInSinglePod[i],
				Value: postActionValue[i]}
			var actions = []*api.RulesInfo_Action{&ruleAction}
			rulesInfoRuleItem[i].PodSelector = postPodSelector[i]
			rulesInfoRuleItem[i].Actions = actions
			rulesInfoRuleItem[i].Container = postContainer[i]
			postRulesInfo.Rules = append(postRulesInfo.Rules, &rulesInfoRuleItem[i])
		}
		PostRuleCreateReq := &api.RuleCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  postRuleName,
				OrgId: orgID,
			},
			RulesInfo: &postRulesInfo,
		}

		_, err := Inst().Backup.CreateRule(ctx, PostRuleCreateReq)
		if err != nil {
			err = fmt.Errorf("failed to create backup post-rules: [%v]", err)
			return "", "", err
		}
	}

	RuleEnumerateReq := &api.RuleEnumerateRequest{
		OrgId: orgID,
	}
	ruleList, err := Inst().Backup.EnumerateRule(ctx, RuleEnumerateReq)
	if err != nil {
		err = fmt.Errorf("failed to enumerate rule with Error: [%v]", err)
		return "", "", err
	}
	for i := 0; i < len(ruleList.Rules); i++ {
		if ruleList.Rules[i].Metadata.Name == preRuleName {
			preUid = ruleList.Rules[i].Metadata.Uid

		} else if ruleList.Rules[i].Metadata.Name == postRuleName {
			postUid = ruleList.Rules[i].Metadata.Uid
		}
	}
	if totalPreRules != 0 {
		log.Infof("Validate pre-rules for backup")
		preRuleInspectReq := &api.RuleInspectRequest{
			OrgId: orgID,
			Name:  preRuleName,
			Uid:   preUid,
		}
		_, err = Inst().Backup.InspectRule(ctx, preRuleInspectReq)
		if err != nil {
			err = fmt.Errorf("failed to validate the created pre-rule with Error: [%v]", err)
			return "", "", err
		}
	}
	if totalPostRules != 0 {
		log.Infof("Validate post-rules for backup")
		postRuleInspectReq := &api.RuleInspectRequest{
			OrgId: orgID,
			Name:  postRuleName,
			Uid:   postUid,
		}
		_, err = Inst().Backup.InspectRule(ctx, postRuleInspectReq)
		if err != nil {
			err = fmt.Errorf("failed to validate the created post-rule with Error: [%v]", err)
			return "", "", err
		}
	}
	return preRuleName, postRuleName, nil
}

type VMRuleType string

const (
	Freeze   VMRuleType = "freeze"
	Unfreeze VMRuleType = "unfreeze"
)

// CreateRuleForVMBackup creates freeze/unfreeze rule for VM backup
func CreateRuleForVMBackup(ruleName string, vms []kubevirtv1.VirtualMachine, ruleType VMRuleType, ctx context1.Context) error {
	var rulesInfo api.RulesInfo
	namespaceToVMs := GetNamespacesToVMsMap(vms)
	for namespace, vmList := range namespaceToVMs {
		for _, vm := range vmList {
			freezeAction := fmt.Sprintf("/usr/bin/virt-freezer --%s --name %s --namespace %s", ruleType, vm, namespace)
			ruleAction := api.RulesInfo_Action{Background: false, RunInSinglePod: false,
				Value: freezeAction}
			var actions = []*api.RulesInfo_Action{&ruleAction}

			rulesInfo.Rules = append(rulesInfo.Rules, &api.RulesInfo_RuleItem{
				PodSelector: map[string]string{"vm.kubevirt.io/name": vm},
				Actions:     actions,
				Container:   VirtLauncherContainerName,
			})
		}
	}
	RuleCreateReq := &api.RuleCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  ruleName,
			OrgId: BackupOrgID,
		},
		RulesInfo: &rulesInfo,
	}
	_, err := Inst().Backup.CreateRule(ctx, RuleCreateReq)
	return err
}

// GetAllBackupNamesByOwnerID gets all backup names associated with the given ownerID
func GetAllBackupNamesByOwnerID(ownerID string, orgID string, ctx context1.Context) ([]string, error) {
	isAdminCtx, err := portworx.IsAdminCtx(ctx)
	if err != nil {
		return nil, err
	}
	backupEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID,
		EnumerateOptions: &api.EnumerateOptions{
			Owners: func() []string {
				if isAdminCtx {
					return []string{ownerID}
				}
				return nil
			}(),
		},
	}
	backupEnumerateResp, err := Inst().Backup.EnumerateBackup(ctx, backupEnumerateReq)
	if err != nil {
		return nil, err
	}
	backupNames := make([]string, 0)
	for _, backupObj := range backupEnumerateResp.GetBackups() {
		if isAdminCtx {
			backupNames = append(backupNames, backupObj.GetName())
		} else {
			if backupObj.GetOwnership().GetOwner() == ownerID {
				backupNames = append(backupNames, backupObj.GetName())
			}
		}
	}
	return backupNames, nil
}

// GetAllBackupScheduleNamesByOwnerID gets all backup schedule names associated with the given ownerID
func GetAllBackupScheduleNamesByOwnerID(ownerID string, orgID string, ctx context1.Context) ([]string, error) {
	isAdminCtx, err := portworx.IsAdminCtx(ctx)
	if err != nil {
		return nil, err
	}
	backupScheduleEnumerateReq := &api.BackupScheduleEnumerateRequest{
		OrgId: orgID,
		EnumerateOptions: &api.EnumerateOptions{
			Owners: func() []string {
				//if isAdminCtx { // PB-4199
				//	return []string{ownerID}
				//}
				return nil
			}(),
		},
	}
	backupScheduleEnumerateResp, err := Inst().Backup.EnumerateBackupSchedule(ctx, backupScheduleEnumerateReq)
	if err != nil {
		return nil, err
	}
	backupScheduleNames := make([]string, 0)
	for _, backupScheduleObj := range backupScheduleEnumerateResp.GetBackupSchedules() {
		if isAdminCtx {
			backupScheduleNames = append(backupScheduleNames, backupScheduleObj.GetName())
		} else {
			if backupScheduleObj.GetOwnership().GetOwner() == ownerID {
				backupScheduleNames = append(backupScheduleNames, backupScheduleObj.GetName())
			}
		}
	}
	return backupScheduleNames, nil
}

// GetAllRestoreNamesByOwnerID gets all restore names associated with the given ownerID
func GetAllRestoreNamesByOwnerID(ownerID string, orgID string, ctx context1.Context) ([]string, error) {
	isAdminCtx, err := portworx.IsAdminCtx(ctx)
	if err != nil {
		return nil, err
	}
	restoreEnumerateReq := &api.RestoreEnumerateRequest{
		OrgId: orgID,
		EnumerateOptions: &api.EnumerateOptions{
			Owners: func() []string {
				//if isAdminCtx {  // PB-4199
				//	return []string{ownerID}
				//}
				return nil
			}(),
		},
	}
	restoreEnumerateResp, err := Inst().Backup.EnumerateRestore(ctx, restoreEnumerateReq)
	if err != nil {
		return nil, err
	}
	restoreNames := make([]string, 0)
	for _, restoreObj := range restoreEnumerateResp.GetRestores() {
		if isAdminCtx {
			restoreNames = append(restoreNames, restoreObj.GetName())
		} else {
			if restoreObj.GetOwnership().GetOwner() == ownerID {
				restoreNames = append(restoreNames, restoreObj.GetName())
			}
		}
	}
	return restoreNames, nil
}

// GetAllBackupSchedulesForUser returns all current BackupSchedules for user.
func GetAllBackupSchedulesForUser(username, password string) ([]string, error) {
	scheduleNames := make([]string, 0)
	backupDriver := Inst().Backup
	ctx, err := backup.GetNonAdminCtx(username, password)
	if err != nil {
		return nil, err
	}

	scheduleEnumerateReq := &api.BackupScheduleEnumerateRequest{
		OrgId: BackupOrgID,
	}
	currentSchedules, err := backupDriver.EnumerateBackupSchedule(ctx, scheduleEnumerateReq)
	if err != nil {
		return nil, err
	}
	for _, schedule := range currentSchedules.GetBackupSchedules() {
		scheduleNames = append(scheduleNames, schedule.GetName())
	}
	return scheduleNames, nil
}

// GetAllRestoresForUser returns all the current restores for the user.
func GetAllRestoresForUser(username string, password string) ([]string, error) {
	restoreNames := make([]string, 0)
	backupDriver := Inst().Backup
	ctx, err := backup.GetNonAdminCtx(username, password)
	if err != nil {
		return nil, err
	}

	restoreEnumerateRequest := &api.RestoreEnumerateRequest{
		OrgId: BackupOrgID,
	}
	restoreResponse, err := backupDriver.EnumerateRestore(ctx, restoreEnumerateRequest)
	if err != nil {
		return restoreNames, err
	}
	for _, restore := range restoreResponse.GetRestores() {
		restoreNames = append(restoreNames, restore.Name)
	}
	return restoreNames, nil
}

// CreateBackupScheduleIntervalPolicy create periodic schedule policy with given context.
func CreateBackupScheduleIntervalPolicy(retian int64, intervalMins int64, incrCount uint64, periodicSchedulePolicyName string, periodicSchedulePolicyUid string, OrgID string, ctx context1.Context, ObjectLock bool, AutoDeleteForObjectLock bool) (err error) {
	backupDriver := Inst().Backup
	schedulePolicyCreateRequest := &api.SchedulePolicyCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  periodicSchedulePolicyName,
			Uid:   periodicSchedulePolicyUid,
			OrgId: OrgID,
		},

		SchedulePolicy: &api.SchedulePolicyInfo{
			Interval:      &api.SchedulePolicyInfo_IntervalPolicy{Retain: retian, Minutes: intervalMins, IncrementalCount: &api.SchedulePolicyInfo_IncrementalCount{Count: incrCount}},
			ForObjectLock: ObjectLock,
			AutoDelete:    AutoDeleteForObjectLock,
		},
	}

	_, err = backupDriver.CreateSchedulePolicy(ctx, schedulePolicyCreateRequest)
	if err != nil {
		return
	}
	return
}

// CreateInvalidAWSCloudCredential creates cloud credentials with invalid paramaters.
func CreateInvalidAWSCloudCredential(credName string, uid, orgID string, ctx context1.Context) error {
	log.Infof("Create cloud credential with name [%s] for org [%s] ", credName, orgID)
	var credCreateRequest *api.CloudCredentialCreateRequest
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
					AccessKey: "admin",
					SecretKey: backup.PxCentralAdminPwd + RandomString(10),
				},
			},
		},
	}
	_, err := Inst().Backup.CreateCloudCredential(ctx, credCreateRequest)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		log.Errorf("failed to create invalid cloud credential with name [%s] in org [%s] with [AWS/S3] as provider", credName, orgID)
		return err
	}
	return nil
}

// UpdateCluster updates cluster with given credentials.
func UpdateCluster(clusterName string, clusterUid string, kubeConfigPath string, orgId string, cloudCred string, cloudCredUID string, ctx context1.Context) (*api.ClusterUpdateResponse, error) {
	backupDriver := Inst().Backup
	kubeconfigRaw, err := ioutil.ReadFile(kubeConfigPath)
	if err != nil {
		return nil, err
	}

	clusterUpdateRequest := &api.ClusterUpdateRequest{
		CreateMetadata:        &api.CreateMetadata{Name: clusterName, OrgId: orgId, Uid: clusterUid},
		PxConfig:              &api.PXConfig{},
		Kubeconfig:            base64.StdEncoding.EncodeToString(kubeconfigRaw),
		CloudCredential:       cloudCred,
		CloudCredentialRef:    &api.ObjectRef{Name: cloudCred, Uid: cloudCredUID},
		PlatformCredentialRef: &api.ObjectRef{},
	}
	status, err := backupDriver.UpdateCluster(ctx, clusterUpdateRequest)
	if err != nil {
		return nil, err
	}
	return status, err
}

// DeleteAllBackups deletes all backup from the given context and org
func DeleteAllBackups(ctx context1.Context, orgId string) error {
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgId,
	}
	curBackups, err := Inst().Backup.EnumerateBackup(ctx, bkpEnumerateReq)
	if err != nil {
		return err
	}
	errChan := make(chan error, len(curBackups.GetBackups()))
	semaphore := make(chan int, 4)
	var wg sync.WaitGroup
	for _, bkp := range curBackups.GetBackups() {
		wg.Add(1)
		go func(bkp *api.BackupObject) {
			semaphore <- 0
			defer wg.Done()
			defer func() { <-semaphore }()
			bkpDeleteRequest := &api.BackupDeleteRequest{
				Name:  bkp.GetName(),
				OrgId: bkp.GetOrgId(),
				Uid:   bkp.GetUid(),
			}
			_, err := Inst().Backup.DeleteBackup(ctx, bkpDeleteRequest)
			if err != nil {
				errChan <- err
				return
			}
			err = Inst().Backup.WaitForBackupDeletion(ctx, bkp.GetName(), bkp.GetOrgId(), BackupDeleteTimeout, BackupDeleteRetryTime)
			if err != nil {
				errChan <- err
				return
			}
		}(bkp)
	}
	wg.Wait()
	close(errChan)
	close(semaphore)
	var errList []string
	for err := range errChan {
		errList = append(errList, err.Error())
	}
	if len(errList) > 0 {
		return fmt.Errorf(strings.Join(errList, "; "))
	}
	return nil
}

type RoleServices string

const (
	BackupSchedulePolicy RoleServices = "schedulepolicy"
	Rules                             = "rules"
	Cloudcredential                   = "cloudcredential"
	BackupLocation                    = "backuplocation"
	Role                              = "role"
)

type RoleApis string

const (
	All       RoleApis = "*"
	Create             = "create*"
	Inspect            = "inspect*"
	Update             = "update*"
	Enumerate          = "enumerate*"
	Validate           = "validate*"
	Delete             = "delete*"
)

// CreateRole creates role with given services and apis in px-backup datastore and also add role to keycloak.
func CreateRole(roleName backup.PxBackupRole, svcs []RoleServices, apis []RoleApis, ctx context1.Context) error {
	err := backup.CreateRole(roleName, "custom-role")
	if err != nil {
		return err
	}
	roleId, err := backup.GetRoleID(roleName)
	if err != nil {
		return err
	}
	backupDriver := Inst().Backup

	serviceList := make([]string, len(svcs))
	for i, svc := range svcs {
		serviceList[i] = string(svc)
	}
	apiList := make([]string, len(apis))
	for i, api := range apis {
		apiList[i] = string(api)
	}
	rule := &api.RoleConfig{
		Services: serviceList,
		Apis:     apiList,
	}
	roleCreateRequest := &api.RoleCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  string(roleName),
			OrgId: BackupOrgID,
		},
		Rules:  []*api.RoleConfig{rule},
		RoleId: roleId,
	}
	_, err = backupDriver.CreateRole(ctx, roleCreateRequest)
	if err != nil {
		return err
	}
	return nil
}

// DeleteRole delete role with given services and apis from datastore and also from keycloak.
func DeleteRole(roleName backup.PxBackupRole, orgId string, ctx context1.Context) error {
	backupDriver := Inst().Backup
	roleId, err := backup.GetRoleID(roleName)
	if err != nil {
		return err
	}
	roleDeleteRequest := &api.RoleDeleteRequest{
		OrgId: orgId,
		Name:  string(roleName),
		Uid:   roleId,
	}
	_, err = backupDriver.DeleteRole(ctx, roleDeleteRequest)
	if err != nil {
		return err
	}
	err = backup.DeleteRole(roleName)
	if err != nil {
		return err
	}
	return nil
}

// DeleteBackupSchedulePolicyWithContext delete schedule policy with given context1.
func DeleteBackupSchedulePolicyWithContext(orgID string, policyList []string, ctx context1.Context) error {
	schedPolicyMap := make(map[string]string)
	schedPolicyEnumerateReq := &api.SchedulePolicyEnumerateRequest{
		OrgId: orgID,
	}
	schedulePolicyList, err := Inst().Backup.EnumerateSchedulePolicy(ctx, schedPolicyEnumerateReq)
	if err != nil {
		err = fmt.Errorf("failed to get list of schedule policies with error: [%v]", err)
		return err
	}
	for i := 0; i < len(schedulePolicyList.SchedulePolicies); i++ {
		schedPolicyMap[schedulePolicyList.SchedulePolicies[i].Metadata.Name] = schedulePolicyList.SchedulePolicies[i].Metadata.Uid
	}
	for i := 0; i < len(policyList); i++ {
		schedPolicydeleteReq := &api.SchedulePolicyDeleteRequest{
			OrgId: orgID,
			Name:  policyList[i],
			Uid:   schedPolicyMap[policyList[i]],
		}
		_, err := Inst().Backup.DeleteSchedulePolicy(ctx, schedPolicydeleteReq)
		if err != nil {
			err = fmt.Errorf("failed to delete schedule policy %s with error [%v]", policyList[i], err)
			return err
		}
	}
	return nil
}

// DeletePodWhileBackupInProgress deletes pod with given label and in given namespace when backup is in progress
func DeletePodWhileBackupInProgress(ctx context1.Context, orgId string, backupName string, namespace string, label map[string]string) error {
	log.InfoD("Deleting pod while backup is in progress")
	backupInProgressStatus := api.BackupInfo_StatusInfo_InProgress
	backupPendingStatus := api.BackupInfo_StatusInfo_Pending
	backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, orgId)
	if err != nil {
		return err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUID,
		OrgId: orgId,
	}
	backupProgressCheckFunc := func() (interface{}, bool, error) {
		backupResponse, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
		if err != nil {
			return "", false, err
		}
		actual := backupResponse.GetBackup().GetStatus().Status
		if actual == backupInProgressStatus {
			return "", false, nil
		}
		if actual == backupPendingStatus {
			return "", true, fmt.Errorf("backup status for [%s] expected was [%v] but got [%s]", backupName, backupInProgressStatus, actual)
		} else {
			return "", false, fmt.Errorf("backup status for [%s] expected was [%v] but got [%s]", backupName, backupInProgressStatus, actual)
		}
	}
	_, err = DoRetryWithTimeoutWithGinkgoRecover(backupProgressCheckFunc, MaxWaitPeriodForBackupJobCancellation*time.Minute, BackupJobCancellationRetryTime*time.Second)
	if err != nil {
		return err
	}
	err = DeletePodWithWithoutLabelInNamespace(namespace, label, false)
	if err != nil {
		return err
	}
	return nil
}

// DeletePodWhileRestoreInProgress deletes pod with given label and in given namespace when restore is in progress
func DeletePodWhileRestoreInProgress(ctx context1.Context, orgId string, restoreName string, namespace string, label map[string]string) error {
	log.InfoD("Deleting pod while restore is in progress")
	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgId,
	}
	restoreInProgressStatus := api.RestoreInfo_StatusInfo_InProgress
	restorePendingStatus := api.RestoreInfo_StatusInfo_Pending
	restoreProgressCheckFunc := func() (interface{}, bool, error) {
		resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
		if err != nil {
			return "", false, err
		}
		actual := resp.GetRestore().GetStatus().Status
		if actual == restoreInProgressStatus {
			return "", false, nil
		}
		if actual == restorePendingStatus {
			return "", true, fmt.Errorf("restore status for [%s] expected was [%v] but got [%s]", restoreName, restoreInProgressStatus, actual)
		} else {
			return "", false, fmt.Errorf("restore status for [%s] expected was [%v] but got [%s]", restoreName, restoreInProgressStatus, actual)
		}
	}
	_, err := DoRetryWithTimeoutWithGinkgoRecover(restoreProgressCheckFunc, MaxWaitPeriodForRestoreCompletionInMinute*time.Minute, RestoreJobProgressRetryTime*time.Second)
	if err != nil {
		return err
	}
	err = DeletePodWithWithoutLabelInNamespace(namespace, label, false)
	if err != nil {
		return err
	}
	return nil
}

// AddBackupLocationOwnership adds new ownership to the existing backup location object
func AddBackupLocationOwnership(name string, uid string, userNames []string, groups []string, accessType OwnershipAccessType, publicAccess OwnershipAccessType, ctx context1.Context) error {
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

	userBackupLocationOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)

	for _, userID := range userIDs {
		userBackupLocationOwnershipAccessConfig := &api.Ownership_AccessConfig{
			Id:     userID,
			Access: api.Ownership_AccessType(accessType),
		}
		userBackupLocationOwnershipAccessConfigs = append(userBackupLocationOwnershipAccessConfigs, userBackupLocationOwnershipAccessConfig)
	}

	groupBackupLocationOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)

	for _, groupID := range groupIDs {
		groupBackupLocationOwnershipAccessConfig := &api.Ownership_AccessConfig{
			Id:     groupID,
			Access: api.Ownership_AccessType(accessType),
		}
		groupBackupLocationOwnershipAccessConfigs = append(groupBackupLocationOwnershipAccessConfigs, groupBackupLocationOwnershipAccessConfig)
	}

	backupLocationInspectRequest := &api.BackupLocationInspectRequest{
		OrgId: BackupOrgID,
		Name:  name,
		Uid:   uid,
	}
	backupLocationInspectResp, err := Inst().Backup.InspectBackupLocation(ctx, backupLocationInspectRequest)
	if err != nil {
		return err
	}
	currentGroupsConfigs := backupLocationInspectResp.BackupLocation.GetOwnership().GetGroups()
	groupBackupLocationOwnershipAccessConfigs = append(groupBackupLocationOwnershipAccessConfigs, currentGroupsConfigs...)
	currentUsersConfigs := backupLocationInspectResp.BackupLocation.GetOwnership().GetCollaborators()
	userBackupLocationOwnershipAccessConfigs = append(userBackupLocationOwnershipAccessConfigs, currentUsersConfigs...)

	bLocationOwnershipUpdateReq := &api.BackupLocationOwnershipUpdateRequest{
		OrgId: BackupOrgID,
		Name:  name,
		Ownership: &api.Ownership{
			Groups:        groupBackupLocationOwnershipAccessConfigs,
			Collaborators: userBackupLocationOwnershipAccessConfigs,
			Public: &api.Ownership_PublicAccessControl{
				Type: api.Ownership_AccessType(publicAccess),
			},
		},
		Uid: uid,
	}

	_, err = backupDriver.UpdateOwnershipBackupLocation(ctx, bLocationOwnershipUpdateReq)
	if err != nil {
		return fmt.Errorf("failed to create backup location: %v", err)
	}
	return nil
}

// AddRuleOwnership adds new ownership to the existing rule object
func AddRuleOwnership(ruleName string, ruleUid string, userNames []string, groups []string, accessType OwnershipAccessType, publicAccess OwnershipAccessType, ctx context1.Context) error {
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

	userRuleOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)

	for _, userID := range userIDs {
		userRuleOwnershipAccessConfig := &api.Ownership_AccessConfig{
			Id:     userID,
			Access: api.Ownership_AccessType(accessType),
		}
		userRuleOwnershipAccessConfigs = append(userRuleOwnershipAccessConfigs, userRuleOwnershipAccessConfig)
	}

	groupRuleOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)

	for _, groupID := range groupIDs {
		groupRuleOwnershipAccessConfig := &api.Ownership_AccessConfig{
			Id:     groupID,
			Access: api.Ownership_AccessType(accessType),
		}
		groupRuleOwnershipAccessConfigs = append(groupRuleOwnershipAccessConfigs, groupRuleOwnershipAccessConfig)
	}

	ruleInspectRequest := &api.RuleInspectRequest{
		OrgId: BackupOrgID,
		Name:  ruleName,
		Uid:   ruleUid,
	}
	ruleInspectResp, err := Inst().Backup.InspectRule(ctx, ruleInspectRequest)
	if err != nil {
		return err
	}
	currentGroupsConfigs := ruleInspectResp.Rule.GetOwnership().GetGroups()
	groupRuleOwnershipAccessConfigs = append(groupRuleOwnershipAccessConfigs, currentGroupsConfigs...)
	currentUsersConfigs := ruleInspectResp.Rule.GetOwnership().GetCollaborators()
	userRuleOwnershipAccessConfigs = append(userRuleOwnershipAccessConfigs, currentUsersConfigs...)

	ruleOwnershipUpdateReq := &api.RuleOwnershipUpdateRequest{
		OrgId: BackupOrgID,
		Name:  ruleName,
		Ownership: &api.Ownership{
			Groups:        groupRuleOwnershipAccessConfigs,
			Collaborators: userRuleOwnershipAccessConfigs,
			Public: &api.Ownership_PublicAccessControl{
				Type: api.Ownership_AccessType(publicAccess),
			},
		},
		Uid: ruleUid,
	}

	_, err = backupDriver.UpdateOwnershipRule(ctx, ruleOwnershipUpdateReq)
	if err != nil {
		return fmt.Errorf("failed to update rule ownership: %v", err)
	}
	return nil
}

// AddSchedulePolicyOwnership adds new ownership to the existing schedulePolicy object.
func AddSchedulePolicyOwnership(schedulePolicyName string, schedulePolicyUid string, userNames []string, groups []string, accessType OwnershipAccessType, publicAccess OwnershipAccessType, ctx context1.Context) error {
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

	userSchdeulePolicyOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)

	for _, userID := range userIDs {
		userSchedulePolicyOwnershipAccessConfig := &api.Ownership_AccessConfig{
			Id:     userID,
			Access: api.Ownership_AccessType(accessType),
		}
		userSchdeulePolicyOwnershipAccessConfigs = append(userSchdeulePolicyOwnershipAccessConfigs, userSchedulePolicyOwnershipAccessConfig)
	}

	groupSchedulePolicyOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)

	for _, groupID := range groupIDs {
		groupSchedulePolicyOwnershipAccessConfig := &api.Ownership_AccessConfig{
			Id:     groupID,
			Access: api.Ownership_AccessType(accessType),
		}
		groupSchedulePolicyOwnershipAccessConfigs = append(groupSchedulePolicyOwnershipAccessConfigs, groupSchedulePolicyOwnershipAccessConfig)
	}

	schedulePolicyInspectRequest := &api.SchedulePolicyInspectRequest{
		OrgId: BackupOrgID,
		Name:  schedulePolicyName,
		Uid:   schedulePolicyUid,
	}
	schedulePolicyInspectResp, err := Inst().Backup.InspectSchedulePolicy(ctx, schedulePolicyInspectRequest)
	if err != nil {
		return err
	}
	currentGroups := schedulePolicyInspectResp.SchedulePolicy.GetOwnership().GetGroups()
	groupSchedulePolicyOwnershipAccessConfigs = append(groupSchedulePolicyOwnershipAccessConfigs, currentGroups...)
	currentUsersConfigs := schedulePolicyInspectResp.SchedulePolicy.GetOwnership().GetCollaborators()
	userSchdeulePolicyOwnershipAccessConfigs = append(userSchdeulePolicyOwnershipAccessConfigs, currentUsersConfigs...)

	schedulePolicyOwnershipUpdateReq := &api.SchedulePolicyOwnershipUpdateRequest{
		OrgId: BackupOrgID,
		Name:  schedulePolicyName,
		Ownership: &api.Ownership{
			Groups:        groupSchedulePolicyOwnershipAccessConfigs,
			Collaborators: userSchdeulePolicyOwnershipAccessConfigs,
			Public: &api.Ownership_PublicAccessControl{
				Type: api.Ownership_AccessType(publicAccess),
			},
		},
		Uid: schedulePolicyUid,
	}

	_, err = backupDriver.UpdateOwnershipSchedulePolicy(ctx, schedulePolicyOwnershipUpdateReq)
	if err != nil {
		return fmt.Errorf("failed to update schedule policy: %v", err)
	}
	return nil
}

// RemoveSchedulePolicyOwnership removes ownership from the existing schedulePolicy object.
func RemoveSchedulePolicyOwnership(schedulePolicyName string, schedulePolicyUid string, userNames []string, groups []string, publicAccess OwnershipAccessType, ctx context1.Context) error {
	backupDriver := Inst().Backup
	userIDs := make([]string, 0)
	groupIDs := make([]string, 0)
	groupSchedulePolicyOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)
	userSchdeulePolicyOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)

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

	schedulePolicyInspectRequest := &api.SchedulePolicyInspectRequest{
		OrgId: BackupOrgID,
		Name:  schedulePolicyName,
		Uid:   schedulePolicyUid,
	}
	schedulePolicyInspectResp, err := Inst().Backup.InspectSchedulePolicy(ctx, schedulePolicyInspectRequest)
	if err != nil {
		return err
	}
	currentGroupConfigs := schedulePolicyInspectResp.SchedulePolicy.GetOwnership().GetGroups()
	for _, currentGroupConfig := range currentGroupConfigs {
		if !IsPresent(groupIDs, currentGroupConfig.Id) {
			groupSchedulePolicyOwnershipAccessConfigs = append(groupSchedulePolicyOwnershipAccessConfigs, currentGroupConfig)
		}
	}

	currentUsersConfigs := schedulePolicyInspectResp.SchedulePolicy.GetOwnership().GetCollaborators()
	for _, currentUserConfig := range currentUsersConfigs {
		if !IsPresent(userIDs, currentUserConfig.Id) {
			userSchdeulePolicyOwnershipAccessConfigs = append(userSchdeulePolicyOwnershipAccessConfigs, currentUserConfig)
		}
	}

	schedulePolicyOwnershipUpdateReq := &api.SchedulePolicyOwnershipUpdateRequest{
		OrgId: BackupOrgID,
		Name:  schedulePolicyName,
		Ownership: &api.Ownership{
			Groups:        groupSchedulePolicyOwnershipAccessConfigs,
			Collaborators: userSchdeulePolicyOwnershipAccessConfigs,
			Public: &api.Ownership_PublicAccessControl{
				Type: api.Ownership_AccessType(publicAccess),
			},
		},
		Uid: schedulePolicyUid,
	}

	_, err = backupDriver.UpdateOwnershipSchedulePolicy(ctx, schedulePolicyOwnershipUpdateReq)
	if err != nil {
		return fmt.Errorf("failed to update schedule policy: %v", err)
	}
	return nil
}

// RemoveRuleOwnership removes ownership from to the existing rule object
func RemoveRuleOwnership(ruleName string, ruleUid string, userNames []string, groups []string, publicAccess OwnershipAccessType, ctx context1.Context) error {
	backupDriver := Inst().Backup
	userIDs := make([]string, 0)
	groupIDs := make([]string, 0)
	userRuleOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)
	groupRuleOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)

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

	ruleInspectRequest := &api.RuleInspectRequest{
		OrgId: BackupOrgID,
		Name:  ruleName,
		Uid:   ruleUid,
	}
	ruleInspectResp, err := Inst().Backup.InspectRule(ctx, ruleInspectRequest)
	if err != nil {
		return err
	}
	currentGroupsConfigs := ruleInspectResp.Rule.GetOwnership().GetGroups()
	for _, currentGroupConfig := range currentGroupsConfigs {
		if !IsPresent(groupIDs, currentGroupConfig.Id) {
			groupRuleOwnershipAccessConfigs = append(groupRuleOwnershipAccessConfigs, currentGroupConfig)
		}
	}

	currentUsersConfigs := ruleInspectResp.Rule.GetOwnership().GetCollaborators()
	for _, currentUserConfig := range currentUsersConfigs {
		if !IsPresent(userIDs, currentUserConfig.Id) {
			userRuleOwnershipAccessConfigs = append(userRuleOwnershipAccessConfigs, currentUserConfig)
		}
	}

	ruleOwnershipUpdateReq := &api.RuleOwnershipUpdateRequest{
		OrgId: BackupOrgID,
		Name:  ruleName,
		Ownership: &api.Ownership{
			Groups:        groupRuleOwnershipAccessConfigs,
			Collaborators: userRuleOwnershipAccessConfigs,
			Public: &api.Ownership_PublicAccessControl{
				Type: api.Ownership_AccessType(publicAccess),
			},
		},
		Uid: ruleUid,
	}

	_, err = backupDriver.UpdateOwnershipRule(ctx, ruleOwnershipUpdateReq)
	if err != nil {
		return fmt.Errorf("failed to update rule ownership: %v", err)
	}
	return nil
}

// RemoveBackupLocationOwnership removes ownership from the existing backup location object.
func RemoveBackupLocationOwnership(name string, uid string, userNames []string, groups []string, publicAccess OwnershipAccessType, ctx context1.Context) error {
	backupDriver := Inst().Backup
	userIDs := make([]string, 0)
	groupIDs := make([]string, 0)
	userBackupLocationOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)
	groupBackupLocationOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)

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

	backupLocationInspectRequest := &api.BackupLocationInspectRequest{
		OrgId: BackupOrgID,
		Name:  name,
		Uid:   uid,
	}
	backupLocationInspectResp, err := Inst().Backup.InspectBackupLocation(ctx, backupLocationInspectRequest)
	if err != nil {
		return err
	}
	currentGroupsConfigs := backupLocationInspectResp.BackupLocation.GetOwnership().GetGroups()
	for _, currentGroupConfig := range currentGroupsConfigs {
		if !IsPresent(groupIDs, currentGroupConfig.Id) {
			groupBackupLocationOwnershipAccessConfigs = append(groupBackupLocationOwnershipAccessConfigs, currentGroupConfig)
		}
	}

	currentUsersConfigs := backupLocationInspectResp.BackupLocation.GetOwnership().GetCollaborators()
	for _, currentUserConfig := range currentUsersConfigs {
		if !IsPresent(userIDs, currentUserConfig.Id) {
			userBackupLocationOwnershipAccessConfigs = append(userBackupLocationOwnershipAccessConfigs, currentUserConfig)
		}
	}

	bLocationOwnershipUpdateReq := &api.BackupLocationOwnershipUpdateRequest{
		OrgId: BackupOrgID,
		Name:  name,
		Ownership: &api.Ownership{
			Groups:        groupBackupLocationOwnershipAccessConfigs,
			Collaborators: userBackupLocationOwnershipAccessConfigs,
			Public: &api.Ownership_PublicAccessControl{
				Type: api.Ownership_AccessType(publicAccess),
			},
		},
		Uid: uid,
	}

	_, err = backupDriver.UpdateOwnershipBackupLocation(ctx, bLocationOwnershipUpdateReq)
	if err != nil {
		return fmt.Errorf("failed to create backup location: %v", err)
	}
	return nil
}

// RemoveCloudCredentialOwnership removes ownership from the existing CloudCredential object.
func RemoveCloudCredentialOwnership(cloudCredentialName string, cloudCredentialUid string, userNames []string, groups []string, publicAccess OwnershipAccessType, ctx context1.Context, orgID string) error {
	backupDriver := Inst().Backup
	userIDs := make([]string, 0)
	groupIDs := make([]string, 0)
	userCloudCredentialOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)
	groupCloudCredentialOwnershipAccessConfigs := make([]*api.Ownership_AccessConfig, 0)
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
	for _, currentGroupConfig := range currentGroupsConfigs {
		if !IsPresent(groupIDs, currentGroupConfig.Id) {
			groupCloudCredentialOwnershipAccessConfigs = append(groupCloudCredentialOwnershipAccessConfigs, currentGroupConfig)
		}
	}

	currentUsersConfigs := cloudCredentialInspectResp.CloudCredential.GetOwnership().GetCollaborators()
	for _, currentUserConfig := range currentUsersConfigs {
		if !IsPresent(userIDs, currentUserConfig.Id) {
			userCloudCredentialOwnershipAccessConfigs = append(userCloudCredentialOwnershipAccessConfigs, currentUserConfig)
		}
	}

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

// StartKubevirtVM starts the kubevirt VM and waits till the status is Running
func StartKubevirtVM(name string, namespace string, waitForCompletion bool) error {
	k8sKubevirt := kubevirt.Instance()
	vm, err := k8sKubevirt.GetVirtualMachine(name, namespace)
	if err != nil {
		return err
	}
	err = k8sKubevirt.StartVirtualMachine(vm)
	if err != nil {
		return err
	}
	if waitForCompletion {
		t := func() (interface{}, bool, error) {
			vm, err = k8sKubevirt.GetVirtualMachine(name, namespace)
			if err != nil {
				return "", false, fmt.Errorf("unable to get virtual machine [%s] in namespace [%s]\nerror - %s", name, namespace, err.Error())
			}
			if vm.Status.PrintableStatus != kubevirtv1.VirtualMachineStatusRunning {
				return "", true, fmt.Errorf("virtual machine [%s] in namespace [%s] is in %s state, waiting to be in %s state", name, namespace, vm.Status.PrintableStatus, kubevirtv1.VirtualMachineStatusRunning)
			}
			log.InfoD("virtual machine [%s] in namespace [%s] is in %s state", name, namespace, vm.Status.PrintableStatus)
			return "", false, nil
		}
		_, err = DoRetryWithTimeoutWithGinkgoRecover(t, vmStartStopTimeout, vmStartStopRetryTime)
		return err
	}
	return nil
}

// StopKubevirtVM stops the kubevirt VM and waits till the status is Stopped
func StopKubevirtVM(name string, namespace string, waitForCompletion bool) error {
	k8sKubevirt := kubevirt.Instance()
	vm, err := k8sKubevirt.GetVirtualMachine(name, namespace)
	if err != nil {
		return err
	}
	err = k8sKubevirt.StopVirtualMachine(vm)
	if err != nil {
		return err
	}
	if waitForCompletion {
		t := func() (interface{}, bool, error) {
			vm, err = k8sKubevirt.GetVirtualMachine(name, namespace)
			if err != nil {
				return "", false, fmt.Errorf("unable to get virtual machine [%s] in namespace [%s]\nerror - %s", name, namespace, err.Error())
			}
			if vm.Status.PrintableStatus != kubevirtv1.VirtualMachineStatusStopped {
				return "", true, fmt.Errorf("virtual machine [%s] in namespace [%s] is in %s state, waiting to be in %s state", name, namespace, vm.Status.PrintableStatus, kubevirtv1.VirtualMachineStatusStopped)
			}
			log.InfoD("virtual machine [%s] in namespace [%s] is in %s state", name, namespace, vm.Status.PrintableStatus)
			return "", false, nil
		}
		_, err = DoRetryWithTimeoutWithGinkgoRecover(t, vmStartStopTimeout, vmStartStopRetryTime)
		return err
	}
	return nil
}

// RestartKubevirtVM restarts the kubevirt VM
func RestartKubevirtVM(name string, namespace string, waitForCompletion bool) error {
	k8sKubevirt := kubevirt.Instance()
	vm, err := k8sKubevirt.GetVirtualMachine(name, namespace)
	if err != nil {
		return err
	}
	err = k8sKubevirt.RestartVirtualMachine(vm)
	if err != nil {
		return err
	}
	if waitForCompletion {
		t := func() (interface{}, bool, error) {
			vm, err = k8sKubevirt.GetVirtualMachine(name, namespace)
			if err != nil {
				return "", false, fmt.Errorf("unable to get virtual machine [%s] in namespace [%s]\nerror - %s", name, namespace, err.Error())
			}
			if vm.Status.PrintableStatus != kubevirtv1.VirtualMachineStatusRunning {
				return "", true, fmt.Errorf("virtual machine [%s] in namespace [%s] is in %s state, waiting to be in %s state", name, namespace, vm.Status.PrintableStatus, kubevirtv1.VirtualMachineStatusRunning)
			}
			log.InfoD("virtual machine [%s] in namespace [%s] is in %s state", name, namespace, vm.Status.PrintableStatus)
			return "", false, nil
		}
		_, err = DoRetryWithTimeoutWithGinkgoRecover(t, vmStartStopTimeout, vmStartStopRetryTime)
		if err != nil {
			return err
		}
	}
	return nil
}

// checkBackupObjectForUnexpectedNS checks if namespaces like kube-system, kube-node-lease, kube-public and px namespace
// is backed up or not
func CheckBackupObjectForUnexpectedNS(ctx context1.Context, backupName string) error {

	var namespacesToSkip = []string{"kube-system", "kube-node-lease", "kube-public"}

	// Fetch a list of backups
	backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, BackupOrgID)
	if err != nil {
		return fmt.Errorf("failed to fetch backup UID")
	}

	// Get an inspect of the backup object
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUID,
		OrgId: BackupOrgID,
	}
	backupResponse, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return fmt.Errorf("failed to fetch backup inspect object")
	}
	backupNamespaces := backupResponse.GetBackup().Namespaces

	err = SetDestinationKubeConfig()
	if err != nil {
		return fmt.Errorf("failed to switch destination cluster context")
	}

	// Get a list of all services and get the namespace where px service is running
	k8sCore := core.Instance()
	allServices, err := k8sCore.ListServices("", metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to get list of services")
	}
	for _, svc := range allServices.Items {
		if svc.Name == "portworx-service" {
			namespacesToSkip = append(namespacesToSkip, svc.Namespace)
		}
	}

	// Check if the namespaces to be skipped is present or not return error if not
	for _, namespace := range backupNamespaces {
		for _, namespacetoskip := range namespacesToSkip {
			if namespacetoskip == namespace {
				return fmt.Errorf("expected namespace %s shouldn't be present in backup", namespace)
			}
		}
	}
	err = SetSourceKubeConfig()
	if err != nil {
		return fmt.Errorf("switching context to source cluster failed")
	}
	return nil
}

type nsPodAge map[string]time.Time

// getPodAge gets the pod age of all pods on all the namespaces on the cluster
func GetBackupPodAge() (map[string]nsPodAge, error) {
	var podAge = make(map[string]nsPodAge)
	k8sCore := core.Instance()
	allNamespaces, err := k8sCore.ListNamespaces(make(map[string]string))
	if err != nil {
		return podAge, fmt.Errorf("failed to get namespaces list")
	}
	for _, namespace := range allNamespaces.Items {
		pods, err := k8sCore.GetPods(namespace.ObjectMeta.GetName(), make(map[string]string))
		if err != nil {
			return podAge, fmt.Errorf("failed to get pods for namespace")
		}
		for _, pod := range pods.Items {
			podAge[namespace.ObjectMeta.GetName()] = nsPodAge{pod.ObjectMeta.GetGenerateName(): pod.GetCreationTimestamp().Time}
		}
	}
	return podAge, nil
}

// comparePodAge checks the status of all pods on all namespaces clusters where the restore was done
func ComparePodAge(oldPodAge map[string]nsPodAge) error {
	var namespacesToSkip = []string{"kube-system", "kube-node-lease", "kube-public"}
	podAge, err := GetBackupPodAge()
	k8sCore := core.Instance()
	allServices, err := k8sCore.ListServices("", metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to get list of services")
	}
	for _, svc := range allServices.Items {
		if svc.Name == "portworx-service" {
			namespacesToSkip = append(namespacesToSkip, svc.Namespace)
		}
	}

	allNamespaces, err := k8sCore.ListNamespaces(make(map[string]string))
	for _, namespace := range allNamespaces.Items {
		pods, err := k8sCore.GetPods(namespace.ObjectMeta.GetName(), make(map[string]string))
		if err != nil {
			return fmt.Errorf("failed to get pods for namespace")
		}
		if IsPresent(namespacesToSkip, namespace.ObjectMeta.GetName()) {
			for _, pod := range pods.Items {
				if podAge[namespace.ObjectMeta.GetName()][pod.ObjectMeta.GetGenerateName()] != oldPodAge[namespace.ObjectMeta.GetName()][pod.ObjectMeta.GetGenerateName()] {
					return fmt.Errorf("namespace [%s] was restored but was expected to skipped", namespace.ObjectMeta.GetName())
				}
			}
		} else {
			for _, pod := range pods.Items {
				if !podAge[namespace.ObjectMeta.GetName()][pod.ObjectMeta.GetGenerateName()].After(oldPodAge[namespace.ObjectMeta.GetName()][pod.ObjectMeta.GetGenerateName()]) {
					return fmt.Errorf("namespace[%s] was to be restored but was expected to be restored due to pod [%s] due to old pod age is %v and new pod age is %v ", namespace.GetName(), pod.ObjectMeta.GetName(), oldPodAge[namespace.ObjectMeta.GetName()][pod.ObjectMeta.GetName()], podAge[namespace.ObjectMeta.GetName()][pod.ObjectMeta.GetName()])
				}
			}
		}
	}
	return nil
}

// createBackupUntilIncrementalBackup creates backup until incremental backups is created returns the name of the incremental backup created
func CreateBackupUntilIncrementalBackup(ctx context1.Context, scheduledAppContextToBackup *scheduler.Context, customBackupLocationName string, backupLocationUID string, labelSelectors map[string]string, orgID string, clusterUid string) (string, error) {
	namespace := scheduledAppContextToBackup.ScheduleOptions.Namespace
	incrementalBackupName := fmt.Sprintf("%s-%s-%v", "incremental-backup", namespace, time.Now().Unix())
	err := CreateBackupWithValidation(ctx, incrementalBackupName, SourceClusterName, customBackupLocationName, backupLocationUID, []*scheduler.Context{scheduledAppContextToBackup}, labelSelectors, orgID, clusterUid, "", "", "", "")
	if err != nil {
		return "", fmt.Errorf("creation and validation of incremental backup [%s] creation: error [%v]", incrementalBackupName, err)
	}

	log.InfoD("Check if backups are incremental backups or not")
	backupDriver := Inst().Backup
	bkpUid, err := backupDriver.GetBackupUID(ctx, incrementalBackupName, orgID)
	if err != nil {
		return "", fmt.Errorf("unable to fetch backup UID - %s : error [%v]", incrementalBackupName, err)
	}

	bkpInspectReq := &api.BackupInspectRequest{
		Name:  incrementalBackupName,
		OrgId: orgID,
		Uid:   bkpUid,
	}
	bkpInspectResponse, err := backupDriver.InspectBackup(ctx, bkpInspectReq)
	if err != nil {
		return "", fmt.Errorf("unable to fetch backup - %s : error [%v]", incrementalBackupName, err)
	}

	for _, vol := range bkpInspectResponse.GetBackup().GetVolumes() {
		backupId := vol.GetBackupId()
		log.InfoD(fmt.Sprintf("Backup Name: %s; BackupID: %s", incrementalBackupName, backupId))
		if strings.Contains(backupId, "incr") {
			return incrementalBackupName, nil
		} else {
			// Attempting to take backups and checking if they are incremental or not
			// as the original incremental backup which we took has taken a full backup this is mostly
			// because CloudSnap is taking full backup instead of incremental backup as it's hitting one of
			// the if else condition in CloudSnap which forces it to take full instead of incremental backup
			log.InfoD("New backup wasn't an incremental backup hence recreating new backup")
			listOfVolumes := make(map[string]bool)
			noFailures := true
			for maxBackupsBeforeIncremental := 0; maxBackupsBeforeIncremental < 8; maxBackupsBeforeIncremental++ {
				log.InfoD(fmt.Sprintf("Recreate incremental backup iteration: %d", maxBackupsBeforeIncremental))
				// Create a new incremental backups
				incrementalBackupName = fmt.Sprintf("%s-%v-%s-%v", "incremental-backup", maxBackupsBeforeIncremental, namespace, time.Now().Unix())
				err := CreateBackupWithValidation(ctx, incrementalBackupName, SourceClusterName, customBackupLocationName, backupLocationUID, []*scheduler.Context{scheduledAppContextToBackup}, labelSelectors, orgID, clusterUid, "", "", "", "")
				if err != nil {
					return "", fmt.Errorf("verifying incremental backup [%s] creation : error [%v]", incrementalBackupName, err)
				}

				// Check if they are incremental or not
				bkpUid, err = backupDriver.GetBackupUID(ctx, incrementalBackupName, orgID)
				if err != nil {
					return "", fmt.Errorf("unable to fetch backup - %s : error [%v]", incrementalBackupName, err)
				}
				bkpInspectReq := &api.BackupInspectRequest{
					Name:  incrementalBackupName,
					OrgId: orgID,
					Uid:   bkpUid,
				}
				bkpInspectResponse, err = backupDriver.InspectBackup(ctx, bkpInspectReq)
				if err != nil {
					return "", fmt.Errorf("unable to fetch backup - %s : error [%v]", incrementalBackupName, err)
				}
				for _, vol := range bkpInspectResponse.GetBackup().GetVolumes() {
					backupId := vol.GetBackupId()
					log.InfoD(fmt.Sprintf("Backup Name: %s; BackupID: %s ", incrementalBackupName, backupId))
					if !strings.Contains(backupId, "incr") {
						listOfVolumes[backupId] = false
					} else {
						listOfVolumes[backupId] = true
					}
				}
				for id, isIncremental := range listOfVolumes {
					if !isIncremental {
						log.InfoD(fmt.Sprintf("Backup %s wasn't a incremental backup", id))
						noFailures = false
					}
				}
				if noFailures {
					break
				}
			}
		}
	}
	return incrementalBackupName, nil
}

// StartAllVMsInNamespace starts all the Kubevirt VMs in the given namespace
func StartAllVMsInNamespace(namespace string, waitForCompletion bool) error {
	k8sKubevirt := kubevirt.Instance()
	var wg sync.WaitGroup
	errors := make([]string, 0)
	var mutex sync.Mutex

	vms, err := k8sKubevirt.ListVirtualMachines(namespace)
	if err != nil {
		return err
	}
	for _, vm := range vms.Items {
		wg.Add(1)
		go func(vm kubevirtv1.VirtualMachine) {
			defer GinkgoRecover()
			defer wg.Done()
			err := StartKubevirtVM(vm.Name, namespace, waitForCompletion)
			if err != nil {
				mutex.Lock()
				defer mutex.Unlock()
				errors = append(errors, err.Error())
			}
		}(vm)
	}
	wg.Wait()
	if len(errors) > 0 {
		return fmt.Errorf("Errors generated while starting VMs in namespace [%s] -\n%s", namespace, strings.Join(errors, "}\n{"))
	}
	return nil
}

// StopAllVMsInNamespace stops all the Kubevirt VMs in the given namespace
func StopAllVMsInNamespace(namespace string, waitForCompletion bool) error {
	k8sKubevirt := kubevirt.Instance()
	var wg sync.WaitGroup
	errors := make([]string, 0)
	var mutex sync.Mutex

	vms, err := k8sKubevirt.ListVirtualMachines(namespace)
	if err != nil {
		return err
	}
	for _, vm := range vms.Items {
		wg.Add(1)
		go func(vm kubevirtv1.VirtualMachine) {
			defer GinkgoRecover()
			defer wg.Done()
			err := StopKubevirtVM(vm.Name, namespace, waitForCompletion)
			if err != nil {
				mutex.Lock()
				defer mutex.Unlock()
				errors = append(errors, err.Error())
			}
		}(vm)
	}
	wg.Wait()
	if len(errors) > 0 {
		return fmt.Errorf("Errors generated while stopping VMs in namespace [%s] -\n%s", namespace, strings.Join(errors, "}\n{"))
	}
	return nil
}

// RestartAllVMsInNamespace restarts all the Kubevirt VMs in the given namespace
func RestartAllVMsInNamespace(namespace string, waitForCompletion bool) error {
	k8sKubevirt := kubevirt.Instance()
	var wg sync.WaitGroup
	errors := make([]string, 0)
	var mutex sync.Mutex

	vms, err := k8sKubevirt.ListVirtualMachines(namespace)
	if err != nil {
		return err
	}
	for _, vm := range vms.Items {
		wg.Add(1)
		go func(vm kubevirtv1.VirtualMachine) {
			defer GinkgoRecover()
			defer wg.Done()
			err := RestartKubevirtVM(vm.Name, namespace, waitForCompletion)
			if err != nil {
				mutex.Lock()
				defer mutex.Unlock()
				errors = append(errors, err.Error())
			}
		}(vm)
	}
	wg.Wait()
	if len(errors) > 0 {
		return fmt.Errorf("Errors generated while restarting VMs in namespace [%s] -\n%s", namespace, strings.Join(errors, "}\n{"))
	}
	return nil
}

// ValidateVMMigration validate VM is migrated to a different node after any migration operation.
func ValidateVMMigration(vm kubevirtv1.VirtualMachine, nodeName string) error {
	k8sKubevirt := kubevirt.Instance()
	t := func() (interface{}, bool, error) {
		vmObj, err := k8sKubevirt.GetVirtualMachine(vm.Name, vm.Namespace)
		if err != nil {
			return "", false, fmt.Errorf("unable to get virtual machine [%s] in namespace [%s]\nerror - %s", vm.Name, vm.Namespace, err.Error())
		}
		if vmObj.Status.PrintableStatus != kubevirtv1.VirtualMachineStatusRunning {
			return "", true, fmt.Errorf("virtual machine [%s] in namespace [%s] is in %s state, waiting to be in %s state", vm.Name, vm.Namespace, vmObj.Status.PrintableStatus, kubevirtv1.VirtualMachineStatusRunning)
		}

		vmPod, err := GetVirtLauncherPodObject(vm)
		if err != nil {
			return "", false, err
		}

		// Get the node where the VM is scheduled after the migration
		nodeNameAfterMigration := vmPod.Spec.NodeName

		if nodeName == nodeNameAfterMigration {
			return "", true, fmt.Errorf("VM pod live migrated [%s] in namespace [%s] but is still on the same node [%s]", vmPod.Name, vmPod.Namespace, nodeName)
		}

		if nodeName != nodeNameAfterMigration {
			log.InfoD("VM pod live migrated to node: [%s]", nodeNameAfterMigration)
			return "", false, nil
		}
		return "", false, nil
	}

	// Retry with timeout until nodeName != nodeNameAfterMigration
	_, err := task.DoRetryWithTimeout(t, defaultVmMountCheckTimeout, defaultVmMountCheckRetryInterval)
	if err != nil {
		return err
	}
	return nil
}

// GetAllVMsInNamespace returns all the Kubevirt VMs in the given namespace
func GetAllVMsInNamespace(namespace string) ([]kubevirtv1.VirtualMachine, error) {
	k8sKubevirt := kubevirt.Instance()
	vms, err := k8sKubevirt.ListVirtualMachines(namespace)
	if err != nil {
		return nil, err
	}
	return vms.Items, nil

}

// GetAllVMsInNamespacesWithLabel returns all the Kubevirt VMs in the namespaces filtered by the namespace label provided
func GetAllVMsInNamespacesWithLabel(namespaceLabel map[string]string) ([]kubevirtv1.VirtualMachine, error) {
	var vms []kubevirtv1.VirtualMachine
	nsList, err := k8sCore.ListNamespaces(namespaceLabel)
	if err != nil {
		return nil, err
	}
	for _, ns := range nsList.Items {
		vmList, err := GetAllVMsInNamespace(ns.Name)
		if err != nil {
			return nil, err
		}
		vms = append(vms, vmList...)
	}
	return vms, nil
}

// GetAllVMsFromScheduledContexts returns all the Kubevirt VMs in the scheduled contexts
func GetAllVMsFromScheduledContexts(scheduledContexts []*scheduler.Context) ([]kubevirtv1.VirtualMachine, error) {
	var vms []kubevirtv1.VirtualMachine

	// Get only unique namespaces
	uniqueNamespaces := make(map[string]bool)
	for _, scheduledContext := range scheduledContexts {
		uniqueNamespaces[scheduledContext.ScheduleOptions.Namespace] = true
	}
	namespaces := make([]string, 0, len(uniqueNamespaces))
	for namespace := range uniqueNamespaces {
		namespaces = append(namespaces, namespace)
	}

	// Get VMs from the unique namespaces
	for _, n := range namespaces {
		vmList, err := GetAllVMsInNamespace(n)
		if err != nil {
			return nil, err
		}
		vms = append(vms, vmList...)
	}
	return vms, nil
}

// GetVirtLauncherPodName returns the name of the virt-launcher pod in the given namespace
func GetVirtLauncherPodName(vm kubevirtv1.VirtualMachine) (string, error) {
	k8sCore := core.Instance()
	pods, err := k8sCore.GetPods(vm.Namespace, make(map[string]string))
	if err != nil {
		return "", err
	}
	for _, pod := range pods.Items {
		if strings.Contains(pod.Name, fmt.Sprintf("%s-%s", "virt-launcher", vm.Name)) {
			log.InfoD("virt-launcher pod found for vm [%s] is [%s]", vm.Name, pod.Name)
			return pod.Name, nil
		}
	}
	return "", fmt.Errorf("virt-launcher pod not found")
}

// GetVirtLauncherPodObject returns the PodObject of the virt-launcher pod in the given namespace
func GetVirtLauncherPodObject(vm kubevirtv1.VirtualMachine) (*corev1.Pod, error) {
	k8sCore := core.Instance()
	pods, err := k8sCore.GetPods(vm.Namespace, make(map[string]string))
	if err != nil {
		return nil, err
	}
	for _, pod := range pods.Items {
		if strings.Contains(pod.Name, fmt.Sprintf("%s-%s", "virt-launcher", vm.Name)) {
			log.InfoD("virt-launcher pod found for vm [%s] is [%s]", vm.Name, pod.Name)
			return &pod, nil
		}
	}
	return nil, fmt.Errorf("virt-launcher pod not found")
}

// GetNumberOfDisksInVM returns the number of disks in the VM
func GetNumberOfDisksInVM(vm kubevirtv1.VirtualMachine) (int, error) {
	cmdForDiskCount := "lsblk -d | grep disk | wc -l"
	diskCountOutput, err := RunCmdInVM(vm, cmdForDiskCount, context1.TODO())
	if err != nil {
		return 0, fmt.Errorf("failed to get disk count in VM: %v", err)
	}
	// trim diskCountOutput and convert to int
	diskCount, err := strconv.Atoi(strings.TrimSpace(diskCountOutput))
	if err != nil {
		return 0, fmt.Errorf("failed to convert disk count to int: %v", err)
	}
	return diskCount, nil
}

// GetVMsInBackup returns the list of VMs in the backup
func GetVMsInBackup(ctx context1.Context, backupName string) ([]string, error) {
	backupDriver := Inst().Backup
	backupUID, err := backupDriver.GetBackupUID(ctx, backupName, BackupOrgID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch backup UID")
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUID,
		OrgId: BackupOrgID,
	}
	backupResponse, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch backup inspect object")
	}
	var vmNames []string
	for _, r := range backupResponse.GetBackup().IncludeResources {
		if r.Kind == "VirtualMachine" && r.Group == "kubevirt.io" {
			vmNames = append(vmNames, r.Name)
		}
		vmNames = append(vmNames, r.Name)
	}
	return vmNames, nil
}

// RunCmdInVM runs a command in the VM by SSHing into it
func RunCmdInVM(vm kubevirtv1.VirtualMachine, cmd string, ctx context1.Context) (string, error) {
	var username string
	var password string
	// Kubevirt client
	k8sKubevirt := kubevirt.Instance()

	// Getting IP of the VM
	t := func() (interface{}, bool, error) {
		vmInstance, err := k8sKubevirt.GetVirtualMachineInstance(ctx, vm.Name, vm.Namespace)
		if err != nil {
			return "", false, err
		}
		if len(vmInstance.Status.Interfaces) == 0 {
			return "", true, fmt.Errorf("no interfaces found in the VM [%s] in namespace [%s]", vm.Name, vm.Namespace)
		}
		return "", false, nil

	}
	// Need to retry because in case of restarts that is a window where the Interface is not available
	_, err := task.DoRetryWithTimeout(t, 5*time.Minute, 30*time.Second)
	vmInstance, err := k8sKubevirt.GetVirtualMachineInstance(ctx, vm.Name, vm.Namespace)
	if err != nil {
		return "", err
	}
	ipAddress := vmInstance.Status.Interfaces[0].IP
	log.Infof("VM Name - %s", vm.Name)
	log.Infof("IP Address - %s", ipAddress)

	// Getting username of the VM
	// Username has to be added as a label to the VMI Spec
	if u, ok := vmInstance.Labels["username"]; !ok {
		return "", fmt.Errorf("username not found in the labels of the vmi spec")
	} else {
		log.Infof("Username - %s", u)
		username = u
	}

	// Get password of the VM
	// Password has to be added as a value in the ConfigMap named kubevirt-creds whose key the name of the VM
	cm, err := core.Instance().GetConfigMap("kubevirt-creds", "default")
	if err != nil {
		return "", err
	}
	if p, ok := cm.Data[vm.Name]; !ok {
		return "", fmt.Errorf("password not found in the configmap [%s/%s] for vm - [%s]", "default", "kubevirt-creds", vm.Name)
	} else {
		log.Infof("Password - %s", p)
		password = p

	}

	// SSH command to be executed
	sshCmd := fmt.Sprintf("sshpass -p '%s' ssh -o StrictHostKeyChecking=no %s@%s %s", password, username, ipAddress, cmd)
	log.Infof("SSH Command - %s", sshCmd)

	// If the cluster provider is openshift then we are creating ssh pod and running the command in it
	if os.Getenv("CLUSTER_PROVIDER") == "openshift" {
		log.Infof("Cluster is openshift hence creating the SSH Pod")
		err = initSSHPod(sshPodNamespace)
		if err != nil {
			return "", err
		}

		testCmdArgs := getSSHCommandArgs(username, password, ipAddress, "hostname")
		cmdArgs := getSSHCommandArgs(username, password, ipAddress, cmd)

		// To check if the ssh server is up and running
		t := func() (interface{}, bool, error) {
			output, err := k8sCore.RunCommandInPod(testCmdArgs, sshPodName, "ssh-container", sshPodNamespace)
			if err != nil {
				log.Infof("Error encountered")
				if isConnectionError(err.Error()) {
					log.Infof("Test connection output - \n%s", output)
					return "", true, err
				} else {
					return "", false, err
				}
			}
			log.Infof("Test connection success output - \n%s", output)
			return "", false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 10*time.Minute, 30*time.Second)
		if err != nil {
			return "", err
		}

		// Executing the actual command
		output, err := k8sCore.RunCommandInPod(cmdArgs, sshPodName, "ssh-container", sshPodNamespace)
		if err != nil {
			return output, err
		}
		log.Infof("Output of cmd %s - \n%s", cmd, output)
		return output, nil
	} else {
		workerNode := node.GetWorkerNodes()[0]
		t := func() (interface{}, bool, error) {
			output, err := runCmdGetOutput(sshCmd, workerNode)
			if err != nil {
				log.Infof("Error encountered")
				if isConnectionError(err.Error()) {
					log.Infof("Output of cmd %s - \n%s", cmd, output)
					return "", true, err
				} else {
					return output, false, err
				}
			}
			log.Infof("Output of cmd %s - \n%s", cmd, output)
			return output, false, nil
		}
		commandOutput, err := task.DoRetryWithTimeout(t, 10*time.Minute, 30*time.Second)
		return commandOutput.(string), err
	}
}

// initSSHPod creates a pod with ssh server installed and running along with sshpass utility
func initSSHPod(namespace string) error {
	var p *corev1.Pod
	var err error
	// Check if namespace exists
	_, err = k8sCore.GetNamespace(namespace)
	if err != nil {
		// Namespace doesn't exist, create it
		log.Infof("Namespace %s does not exist. Creating...", namespace)
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			},
		}
		if _, err = k8sCore.CreateNamespace(ns); err != nil {
			log.Errorf("Failed to create namespace %s: %v", namespace, err)
			return err
		}
		log.Infof("Namespace %s created successfully", namespace)
	}
	if p, err = k8sCore.GetPodByName(sshPodName, namespace); p == nil {
		sshPodSpec := &corev1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      sshPodName,
				Namespace: namespace,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "ssh-container",
						Image: "ubuntu:latest",
						Command: []string{
							"/bin/bash",
							"-c",
						},
						Args: []string{
							"apt-get update && apt-get install -y openssh-server sshpass && service ssh start && echo 'root:toor' | chpasswd && sleep infinity",
						},
					},
				},
			},
		}
		log.Infof("Creating ssh pod")
		_, err = k8sCore.CreatePod(sshPodSpec)
		if err != nil {
			log.Errorf("An Error Occured while creating %v", err)
			return err
		}
		t := func() (interface{}, bool, error) {
			pod, err := k8sCore.GetPodByName(sshPodName, namespace)
			if err != nil {
				return "", false, err
			}
			if !k8sCore.IsPodRunning(*pod) {
				return "", true, fmt.Errorf("waiting for pod %s to be in running state", sshPodName)
			}
			// Adding static sleep to let the ssh server start
			time.Sleep(30 * time.Second)
			log.Infof("ssh pod creation complete")
			return "", false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 30*time.Second)
		return err
	}
	return err
}

// isConnectionError checks if the error message is a connection error
func isConnectionError(errorMessage string) bool {
	return strings.Contains(errorMessage, "Connection refused") || strings.Contains(errorMessage, "Host is unreachable") ||
		strings.Contains(errorMessage, "No route to host")
}

func getSSHCommandArgs(username, password, ipAddress, cmd string) []string {
	return []string{"sshpass", "-p", password, "ssh", "-o", "StrictHostKeyChecking=no", fmt.Sprintf("%s@%s", username, ipAddress), cmd}
}

// UpgradeKubevirt upgrades the kubevirt control plane to the given version
// If workloadUpgrade is set to true, kubevirt workloads are upgraded to the given version
func UpgradeKubevirt(versionToUpgrade string, workloadUpgrade bool) error {
	k8sKubevirt := kubevirt.Instance()
	k8sCore := core.Instance()

	// Checking current version
	current, err := k8sKubevirt.GetVersion()
	if err != nil {
		return err
	}
	log.Infof("Current version is - %s. Trying to upgrade to - %s", current, versionToUpgrade)

	// Compare and validate the upgrade path
	currentKubevirtVersionSemVer, err := version.NewSemver(strings.TrimSpace(strings.ReplaceAll(current, "v", "")))
	if err != nil {
		return err
	}
	versionToUpgradeSemVer, err := version.NewSemver(strings.TrimSpace(strings.ReplaceAll(versionToUpgrade, "v", "")))
	if err != nil {
		return err
	}
	if currentKubevirtVersionSemVer.GreaterThanOrEqual(versionToUpgradeSemVer) {
		return fmt.Errorf("kubevirt cannot be upgraded from [%s] to [%s]", currentKubevirtVersionSemVer.String(), versionToUpgradeSemVer.String())
	} else {
		log.InfoD("Upgrade path chosen (%s) ---> (%s)", current, versionToUpgrade)
	}

	// Generating the manifest URL and applying it to begin upgrade
	manifestYamlURL := fmt.Sprintf("https://github.com/kubevirt/kubevirt/releases/download/%s/kubevirt-operator.yaml", versionToUpgrade)
	_, err = kubectlExec([]string{fmt.Sprintf("--kubeconfig=%v", CurrentClusterConfigPath), "apply", "-f", manifestYamlURL})
	if err != nil {
		return err
	}

	// Waiting till the upgrade is complete
	t := func() (interface{}, bool, error) {
		upgradedVersion, err := k8sKubevirt.GetVersion()
		if err != nil {
			return "", false, err
		}
		if upgradedVersion != versionToUpgrade {
			return "", true, fmt.Errorf("waiting for kubevirt control plane to be upgraded to [%s] but got [%s]", versionToUpgrade, upgradedVersion)
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(t, 10*time.Minute, 30*time.Second)
	if err != nil {
		return err
	}

	// Waiting for all pods in kubevirt namespace to be Ready
	kubevirtCheck := func() (interface{}, bool, error) {
		pods, err := k8sCore.GetPods(KubevirtNamespace, make(map[string]string))
		if err != nil {
			return "", false, err
		}
		allReady := true
		for _, p := range pods.Items {
			log.Infof("Checking status for pod - %s", p.GetName())
			for _, condition := range p.Status.Conditions {
				if condition.Type == corev1.PodReady {
					log.Infof("Pod [%s] in [%s] namespace is in [%s] state", p.GetName(), KubevirtNamespace, condition.Type)
					allReady = true
					break
				} else {
					log.Infof("Pod [%s] in [%s] namespace expected state - [%s], but got [%s]", p.GetName(), KubevirtNamespace, corev1.PodReady, condition.Type)
					allReady = false
				}
			}
			if !allReady {
				return "", true, fmt.Errorf("waiting for all the pods in %s namespace to be ready", KubevirtNamespace)
			}
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(kubevirtCheck, 10*time.Minute, 30*time.Second)
	if err != nil {
		return err
	}

	log.Infof("Kubevirt control plane upgraded from [%s] to [%s]", current, versionToUpgrade)

	// Workload upgrade
	if workloadUpgrade {
		patchString := `[
  		{"op": "replace", "path": "/spec/imagePullPolicy", "value": "IfNotPresent"},
  		{"op": "replace", "path": "/spec/workloadUpdateStrategy", "value": {"workloadUpdateMethods": ["Evict"], "batchEvictionSize": 10, "batchEvictionInterval": "1m"}}
	]`

		_, err = kubectlExec([]string{fmt.Sprintf("--kubeconfig=%v", CurrentClusterConfigPath), "patch", "kubevirt", "kubevirt", "-n", "kubevirt", "--type=json", fmt.Sprintf("-p=%s", patchString)})
		if err != nil {
			return err
		}

		// Adding sleep here to account for some for the kubectl patch to go through
		time.Sleep(10 * time.Second)
		namespaces, err := core.Instance().ListNamespaces(make(map[string]string))
		if err != nil {
			return err
		}

		// Validating if all the pods in namespace where kubevirt VMs are deployed are in running state
		for _, n := range namespaces.Items {
			vms, err := k8sKubevirt.ListVirtualMachines(n.GetName())
			if err != nil {
				return err
			}
			if len(vms.Items) == 0 {
				continue
			}
			pods, err := k8sCore.GetPods(n.GetName(), make(map[string]string))
			for _, p := range pods.Items {
				log.Infof("Checking status for pod - %s", p.GetName())
				err = core.Instance().ValidatePod(&p, podReadyTimeout, podReadyRetryTime)
				if err != nil {
					return err
				}
			}
		}
		log.Infof("Kubevirt workload upgrade completed from [%s] to [%s]", current, versionToUpgrade)
	}
	return nil
}

// ChangeStorkAdminNamespace changes admin namespace for Storage Cluster
func ChangeStorkAdminNamespace(namespace string) (*v1.StorageCluster, error) {
	// Get current storage cluster configuration
	isOpBased, err := Inst().V.IsOperatorBasedInstall()
	if err != nil {
		return nil, err
	}
	storkDeploymentNamespace, err := k8sutils.GetStorkPodNamespace()
	storkOldPods, err := core.Instance().GetPods(storkDeploymentNamespace, map[string]string{"name": "stork"})
	if err != nil {
		return nil, err
	}
	stc, err := Inst().V.GetDriver()
	if err != nil {
		return nil, err
	}
	log.InfoD("Is op based deployment %v", isOpBased)

	if isOpBased {
		if adminNamespace, ok := stc.Spec.Stork.Args["admin-namespace"]; ok {
			log.InfoD("Current admin namespace - [%s]", adminNamespace)
		}
		// Setting the new admin namespace
		if namespace != "" {
			stc.Spec.Stork.Args["admin-namespace"] = namespace
		} else {
			delete(stc.Spec.Stork.Args, "admin-namespace")
		}
		stc, err = operator.Instance().UpdateStorageCluster(stc)
		if err != nil {
			return nil, err
		}
		if namespace != "" {
			log.InfoD("Configured admin namespace to %s", namespace)
		} else {
			log.InfoD("Removed admin namespace") // Removing admin namespace is not supported - https://docs.portworx.com/portworx-backup-on-prem/configure/admin-namespace.html
		}
	} else {
		log.InfoD("Updating stork deployment as it's pxe is not present")
		storkDeployment, err := apps.Instance().GetDeployment(storkDeploymentName, storkDeploymentNamespace)
		if err != nil {
			return nil, err
		}
		storkDeployment.Spec.Template.Spec.Containers[0].Command = append(storkDeployment.Spec.Template.Spec.Containers[0].Command, fmt.Sprintf("--admin-namespace=%s", namespace))

		_, err = apps.Instance().UpdateDeployment(storkDeployment)
		if err != nil {
			return nil, err
		}
	}

	// Explicit wait for the deployment to be updated
	time.Sleep(30 * time.Second)

	checkCurrentAdminNamespace := func() (interface{}, bool, error) {
		currentAdminNamespace, err := getCurrentAdminNamespace()
		if err != nil {
			return nil, true, fmt.Errorf("Error occurred while checking admin namespace - [%s]", err.Error())
		}

		if currentAdminNamespace == namespace {
			return nil, false, nil
		} else if namespace == "" && currentAdminNamespace == defaultStorkDeploymentNamespace {
			return nil, false, nil
		} else {
			return nil, true, fmt.Errorf("Admin namespace not updated")
		}
	}

	// Waiting for all pods admin namespace to be updated
	_, err = task.DoRetryWithTimeout(checkCurrentAdminNamespace, 10*time.Minute, 30*time.Second)
	if err != nil {
		return nil, err
	}

	updatedStorkDeployment, err := apps.Instance().GetDeployment(storkDeploymentName, storkDeploymentNamespace)
	if err != nil {
		return nil, err
	}
	err = apps.Instance().ValidateDeployment(updatedStorkDeployment, storkPodReadyTimeout, podReadyRetryTime)
	if err != nil {
		return nil, err
	}

	// Removing all old stork pods
	for _, pod := range storkOldPods.Items {
		err = core.Instance().DeletePod(pod.Name, pod.Namespace, false)
		if err != nil {
			log.Warnf("Unable to delete [%v]. Error - [%s]", pod.Name, err.Error())
		} else {
			log.Infof("Old stork pod deleted [%v]", pod.Name)
		}
	}

	err = apps.Instance().ValidateDeployment(updatedStorkDeployment, storkPodReadyTimeout, podReadyRetryTime)
	if err != nil {
		return nil, err
	}
	log.InfoD("Verifying if stork controller configmap is updated with new admin namespace")
	storkControllerCmUpdate := func() (interface{}, bool, error) {
		storkControllerConfigMapObject, err := core.Instance().GetConfigMap(storkControllerConfigMap, defaultStorkDeploymentNamespace)
		if err != nil {
			return "", false, fmt.Errorf("error getting stork controller configmap: %v", err)
		}
		log.Infof("Value of admin-ns in stork controller configmap is %s", storkControllerConfigMapObject.Data["admin-ns"])
		if storkControllerConfigMapObject.Data["admin-ns"] == namespace {
			fmt.Sprintf("Stork controller configmap is updated with new admin ns %s", namespace)
			return "", false, nil
		}
		return "", true, fmt.Errorf("stork controller configmap is yet not updated with new admin ns %s", namespace)
	}
	_, err = task.DoRetryWithTimeout(storkControllerCmUpdate, storkControllerConfigMapUpdateTimeout, storkControllerConfigMapRetry)
	if err != nil {
		return nil, err
	}
	return stc, nil
}

// getCurrentAdminNamespace returns the value of current admin namespace set
func getCurrentAdminNamespace() (string, error) {
	isOpBased, _ := Inst().V.IsOperatorBasedInstall()
	if isOpBased {
		stc, err := Inst().V.GetDriver()
		if err != nil {
			return "", err
		}
		if adminNamespace, ok := stc.Spec.Stork.Args["admin-namespace"]; ok {
			log.InfoD("Current admin namespace - [%s]", adminNamespace)
			return adminNamespace, nil
		} else {
			adminNamespace = defaultStorkDeploymentNamespace
			return adminNamespace, nil
		}

	} else {
		adminNamespace, err := k8sutils.GetStorkPodNamespace()
		if err != nil {
			return "", err
		}
		log.InfoD("Current admin namespace - [%s]", adminNamespace)
		return adminNamespace, nil
	}
}

// Validates Backup CRs created
func validateBackupCRs(backupName string, clusterName string, orgID string, clusterUID string,
	backupNameSpaces []string, ctx context1.Context) error {

	currentAdminNamespace, _ := getCurrentAdminNamespace()
	if len(backupNameSpaces) == 1 {
		currentAdminNamespace = backupNameSpaces[0]
	}
	log.Infof("Current CR Namespace: [%s]", currentAdminNamespace)

	backupDriver := Inst().Backup
	clusterReq := &api.ClusterInspectRequest{OrgId: orgID, Name: clusterName, IncludeSecrets: true, Uid: clusterUID}
	clusterResp, err := backupDriver.InspectCluster(ctx, clusterReq)
	if err != nil {
		return err
	}
	clusterObj := clusterResp.GetCluster()

	validateBackupCRInNamespace := func() (interface{}, bool, error) {
		allCRsCurrently := GetAllBackupCRObjects(clusterObj)
		log.Infof("All Current CRs -\n%v\n\n", allCRsCurrently)
		allBackupCrs, err := GetBackupCRs(currentAdminNamespace, clusterObj)
		if err != nil {
			return nil, true, err
		}
		log.Infof("All backup CRs in [%s] are [%v]", currentAdminNamespace, allBackupCrs)

		for _, eachCR := range allBackupCrs {
			if strings.Contains(eachCR, backupName) {
				log.Infof("Backup CR found for [%s] under [%s] namespace", backupName, currentAdminNamespace)
				return nil, false, nil
			}
		}
		return nil, true, fmt.Errorf("Unable to find CR for [%s] under [%s] namespace", backupName, currentAdminNamespace)
	}

	_, err = task.DoRetryWithTimeout(validateBackupCRInNamespace, 5*time.Minute, 500*time.Millisecond)

	return err
}

// Validates Restore CRs created
func ValidateRestoreCRs(restoreName string, clusterName string, orgID string, clusterUID string,
	restoreNameSpaces map[string]string, ctx context1.Context) error {
	currentAdminNamespace, _ := getCurrentAdminNamespace()
	if len(restoreNameSpaces) == 1 {
		for _, val := range restoreNameSpaces {
			currentAdminNamespace = val
		}
	}
	log.Infof("Current CR Namespace: [%s]", currentAdminNamespace)

	backupDriver := Inst().Backup
	clusterReq := &api.ClusterInspectRequest{OrgId: orgID, Name: clusterName, IncludeSecrets: true, Uid: clusterUID}
	clusterResp, err := backupDriver.InspectCluster(ctx, clusterReq)
	if err != nil {
		return err
	}
	clusterObj := clusterResp.GetCluster()

	validateRestoreCRInNamespace := func() (interface{}, bool, error) {

		allCRsCurrently := GetAllBackupCRObjects(clusterObj)
		log.Infof("All Current CRs -\n%v\n\n", allCRsCurrently)

		allRestoreCrs, err := GetRestoreCRs(currentAdminNamespace, clusterObj)
		if err != nil {
			return nil, true, err
		}
		log.Infof("All restore CRs in [%s] are [%v]", currentAdminNamespace, allRestoreCrs)

		for _, eachCR := range allRestoreCrs {
			if strings.Contains(eachCR, restoreName) {
				log.Infof("Restore CR found for [%s] under [%s] namespace", restoreName, currentAdminNamespace)
				return nil, false, nil
			}
		}
		return nil, true, fmt.Errorf("Unable to find CR for [%s] under [%s] namespace", restoreName, currentAdminNamespace)
	}

	_, err = task.DoRetryWithTimeout(validateRestoreCRInNamespace, 5*time.Minute, 500*time.Millisecond)

	return err
}

// GetBackupCRs lists all the Backup CRs present under given namespace
func GetBackupCRs(
	namespace string,
	clusterObj *api.ClusterObject) ([]string, error) {

	allBackupCRNames := make([]string, 0)
	_, storkClient, err := portworx.GetKubernetesInstance(clusterObj)
	if err != nil {
		return nil, err
	}

	storkApplicationBackupCR, err := storkClient.ListApplicationBackups(namespace, metav1.ListOptions{})
	if err != nil {
		log.Warnf("failed to get application backup CR from [%s]. Error [%v]", namespace, err)
		return nil, err
	}

	for _, backup := range storkApplicationBackupCR.Items {
		allBackupCRNames = append(allBackupCRNames, backup.Name)
	}
	return allBackupCRNames, nil
}

// GetRestoreCRs lists all the Restore CRs present under given namespace
func GetRestoreCRs(
	namespace string,
	clusterObj *api.ClusterObject) ([]string, error) {

	allRestoreCRNames := make([]string, 0)
	_, storkClient, err := portworx.GetKubernetesInstance(clusterObj)
	if err != nil {
		return nil, err
	}

	storkApplicationRestoreCR, err := storkClient.ListApplicationRestores(namespace, metav1.ListOptions{})
	if err != nil {
		log.Warnf("failed to get application restore CR from [%s]. Error [%v]", namespace, err)
		return nil, err
	}

	for _, restore := range storkApplicationRestoreCR.Items {
		allRestoreCRNames = append(allRestoreCRNames, restore.Name)
	}
	return allRestoreCRNames, nil
}

// UpdateKDMPConfigMap updates the KDMP configMap with the given key and value.
func UpdateKDMPConfigMap(dataKey string, dataValue string) error {
	KDMPconfigMapName := "kdmp-config"
	KDMPconfigMapNamespace := "kube-system"
	KDMPconfigMap, err := core.Instance().GetConfigMap(KDMPconfigMapName, KDMPconfigMapNamespace)
	if err != nil {
		return err
	}
	intialSize := KDMPconfigMap.Size()
	KDMPconfigMap.Data[dataKey] = dataValue
	_, err = core.Instance().UpdateConfigMap(KDMPconfigMap)
	if err != nil {
		return err
	}
	log.Infof("updated the KDMP configMap data with key [%s]: value [%s]", dataKey, dataValue)

	KDMPconfigMap, err = core.Instance().GetConfigMap(KDMPconfigMapName, KDMPconfigMapNamespace)
	if err != nil {
		return err
	}
	finalSize := KDMPconfigMap.Size()
	log.Infof(fmt.Sprintf("The intial size of configMap [%s] : [%v] and final size [%v]", KDMPconfigMapName, intialSize, finalSize))
	return nil
}

type PodDirectoryConfig struct {
	BasePath           string
	Depth              int
	Levels             int
	FilesPerDirectory  int
	FileSizeInMB       int
	FileName           string
	DirName            string
	CreateSymbolicLink bool
	CreateHardLink     bool
}

// CreateNestedDirectoriesWithFilesInPod creates a nested directory structure with files within a specified Pod.
func CreateNestedDirectoriesWithFilesInPod(pod corev1.Pod, containerName string, directoryConfig PodDirectoryConfig) error {
	var wg sync.WaitGroup
	var errChan = make(chan error)
	fileConfig := PodDirectoryConfig{
		BasePath:          directoryConfig.BasePath,
		FilesPerDirectory: directoryConfig.FilesPerDirectory,
	}
	_, err := CreateFilesInPodDirectory(pod, containerName, fileConfig)
	if err != nil {
		return fmt.Errorf("error creating files in %s: %s", directoryConfig.BasePath, err.Error())
	}

	if directoryConfig.Levels > 0 && directoryConfig.Depth > 0 {
		for i := 0; i < directoryConfig.Depth; i++ {
			dirPath := filepath.Join(directoryConfig.BasePath, fmt.Sprintf("level_%d_depth_%d_%s", directoryConfig.Levels, i, RandomString(8)))
			podConfig := PodDirectoryConfig{
				BasePath: dirPath,
			}
			err := CreateDirectoryInPod(pod, containerName, podConfig)
			if err != nil {
				log.Errorf(fmt.Sprintf("Error creating directory %s: %s\n", dirPath, err.Error()))
				continue
			}
			fileConfig := PodDirectoryConfig{
				BasePath:          dirPath,
				FilesPerDirectory: directoryConfig.FilesPerDirectory,
			}

			_, err = CreateFilesInPodDirectory(pod, containerName, fileConfig)
			if err != nil {
				return fmt.Errorf("error creating files in %s: %s", dirPath, err.Error())
			}

			if directoryConfig.Levels > 1 {
				wg.Add(1)
				go func(dirPath string, depth int) {
					defer wg.Done()
					DirectoryConfig := PodDirectoryConfig{
						BasePath:          dirPath,
						Depth:             directoryConfig.Depth,
						Levels:            directoryConfig.Levels - 1,
						FilesPerDirectory: directoryConfig.FilesPerDirectory,
					}
					errChan <- CreateDirectoryStructureInPod(pod, containerName, DirectoryConfig)
				}(dirPath, directoryConfig.Depth)
			}
		}
	}
	go func() {
		wg.Wait()
		close(errChan)
	}()

	var errs []error
	for err := range errChan {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("error creating nested directories: %v", errs)
	}

	return nil
}

// CreateDirectoryStructureInPod creates a directory structure with multiple levels and files per directory within a specified Pod.
func CreateDirectoryStructureInPod(pod corev1.Pod, containerName string, directoryConfig PodDirectoryConfig) error {
	for i := 0; i < directoryConfig.Depth; i++ {
		dirPath := filepath.Join(directoryConfig.BasePath, fmt.Sprintf("level_%d_depth_%d_%s", directoryConfig.Levels, i, RandomString(8)))
		podConfig := PodDirectoryConfig{
			BasePath: dirPath,
		}
		err := CreateDirectoryInPod(pod, containerName, podConfig)
		if err != nil {
			log.Errorf(fmt.Sprintf("Error creating directory %s: %s\n", dirPath, err.Error()))
			continue
		}
		fileConfig := PodDirectoryConfig{
			BasePath:          dirPath,
			FilesPerDirectory: directoryConfig.FilesPerDirectory,
		}

		_, err = CreateFilesInPodDirectory(pod, containerName, fileConfig)
		if err != nil {
			return fmt.Errorf("error creating files in %s: %s", dirPath, err.Error())
		}

		if directoryConfig.Levels > 1 {
			DirectoryConfig := PodDirectoryConfig{
				BasePath:          dirPath,
				Depth:             directoryConfig.Depth,
				Levels:            directoryConfig.Levels - 1,
				FilesPerDirectory: directoryConfig.FilesPerDirectory,
			}
			err := CreateDirectoryStructureInPod(pod, containerName, DirectoryConfig)
			if err != nil {
				return fmt.Errorf("error creating nested directories: %s", err.Error())
			}
		}
	}
	return nil
}

// CreateDirectoryInPod creates a directory within a specified Pod.
func CreateDirectoryInPod(pod corev1.Pod, containerName string, directoryConfig PodDirectoryConfig) error {
	cmd := fmt.Sprintf("mkdir -p %s ", directoryConfig.BasePath)
	cmdArgs := []string{"/bin/sh", "-c", fmt.Sprintf("%s", cmd)}
	_, err := core.Instance().RunCommandInPod(cmdArgs, pod.Name, containerName, pod.Namespace)
	if err != nil {
		return err
	}
	return nil
}

// CreateFilesInPodDirectory creates a specified number of files per directory level in a Pod.
func CreateFilesInPodDirectory(pod corev1.Pod, containerName string, fileConfig PodDirectoryConfig) ([]string, error) {
	var cmd string
	var fileName string
	var filesCreated []string
	for i := 1; i <= fileConfig.FilesPerDirectory; i++ {
		if len(fileConfig.FileName) > 0 {
			fileName = fileConfig.FileName
		} else {
			fileName = fmt.Sprintf("file%d.txt", i)
		}
		filePath := filepath.Join(fileConfig.BasePath, fmt.Sprintf("%s", fileName))
		if fileConfig.FileSizeInMB != 0 {
			cmd = fmt.Sprintf("dd if=/dev/urandom of=%s bs=%dM count=1;", filePath, fileConfig.FileSizeInMB)
		} else {
			content := fmt.Sprintf("This is file %d in directory %s", i, fileConfig.BasePath)
			cmd = fmt.Sprintf("echo \"%s\" > %s", content, filePath)
		}
		cmdArgs := []string{"/bin/sh", "-c", fmt.Sprintf("%s", cmd)}
		_, err := core.Instance().RunCommandInPod(cmdArgs, pod.Name, containerName, pod.Namespace)
		if err != nil {
			return nil, fmt.Errorf("error creating file %s: %s", filePath, err.Error())
		}
		filesCreated = append(filesCreated, strings.TrimPrefix(fileName, fileConfig.BasePath))
		// Check if symbolic link needs to be created inside the pod using ln command
		if fileConfig.CreateSymbolicLink {
			symlinkPath := filePath + ".symlink"
			lnCmd := fmt.Sprintf("ln -s %s %s", filePath, symlinkPath)
			lnCmdArgs := []string{"/bin/sh", "-c", fmt.Sprintf("%s", lnCmd)}
			_, err := core.Instance().RunCommandInPod(lnCmdArgs, pod.Name, containerName, pod.Namespace)
			if err != nil {
				return nil, fmt.Errorf("error creating symbolic link %s: %s", symlinkPath, err.Error())
			}
			filesCreated = append(filesCreated, strings.TrimPrefix(symlinkPath, fileConfig.BasePath+"/"))
		}

		// Check if hard link needs to be created inside the pod using ln command
		if fileConfig.CreateHardLink {
			hardlinkPath := filePath + ".hardlink"
			lnCmd := fmt.Sprintf("ln %s %s", filePath, hardlinkPath)
			lnCmdArgs := []string{"/bin/sh", "-c", fmt.Sprintf("%s", lnCmd)}
			_, err := core.Instance().RunCommandInPod(lnCmdArgs, pod.Name, containerName, pod.Namespace)
			if err != nil {
				return nil, fmt.Errorf("error creating hard link %s: %s", hardlinkPath, err.Error())
			}
			filesCreated = append(filesCreated, strings.TrimPrefix(hardlinkPath, fileConfig.BasePath+"/"))
		}
	}
	return filesCreated, nil
}

// GetExcludeFileListValue generates a formatted string representation
// of storage classes and their associated files and directories which need to be excluded.
// it return string in below formatted structure for updating value in ConfigMap.
// usage eg:
// KDMP_EXCLUDE_FILE_LIST: |-
//
//	mysql-sc-seq=level_3,level_2,file4.txt,file5.txt
//	mysql-sc=level_3,level_2,file5.txt,file1.txt
//	mysql-sc-aggr=level_2,level_3,file3.txt,file1.txt
func GetExcludeFileListValue(storageClassesMap map[*storagev1.StorageClass][]string) string {
	var excludeLists []string
	for storageClass, paths := range storageClassesMap {
		// check if fbda volume and and add .snapshot to exclude by default
		if isFBDAVolume(storageClass) {
			paths = append(paths, ".snapshot")
		}
		excludeList := fmt.Sprintf("%s=%s", storageClass.Name, strings.Join(paths, ","))
		excludeLists = append(excludeLists, excludeList)
	}
	return strings.Join(excludeLists, "\n")
}

// FetchFilesAndDirectoriesFromPod retrieves files and directories from a specified path within a Pod,
// excluding any specified file or directory entries. It executes a 'find' command within the Pod to gather the files
// and directories of specified file types ('f' for files and 'd' for directories).
func FetchFilesAndDirectoriesFromPod(pod corev1.Pod, containerName string, path string, excludeFileDirectoryList []string) ([]string, []string, error) {
	fileList := make(map[string][]string)
	var fileTypes = [2]string{"f,l", "d"}
	//Fetch the user ID associated with the command execution.
	cmdArgs := []string{"/bin/sh", "-c", "whoami"}
	user, err := core.Instance().RunCommandInPod(cmdArgs, pod.Name, containerName, pod.Namespace)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch user-id , error : %v", err)
	}
	for _, fileType := range fileTypes {
		cmdArgs := []string{"/bin/sh", "-c", fmt.Sprintf("find %s/ -type %s -user %s", path, fileType, user)}
		output, err := core.Instance().RunCommandInPod(cmdArgs, pod.Name, containerName, pod.Namespace)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to fetch file and directories from pod: %s. Output: %s", err, output)
		}
		filesWithAbsolutePaths := strings.Split(output, "\n")
		// Unwanted error strings or patterns to filter out
		unwantedPatterns := []string{"Unable to use a TTY - input is not a terminal or the right kind of file"}

		for _, filePath := range filesWithAbsolutePaths {
			if filePath != "" {
				includePath := true
				for _, pattern := range unwantedPatterns {
					if strings.Contains(filePath, pattern) {
						includePath = false
						break
					}
				}
				if includePath {
					relativePath := strings.TrimPrefix(filePath, path)
					relativePath = strings.TrimLeft(relativePath, "/")
					if !IsPresent(excludeFileDirectoryList, relativePath) {
						fileList[fileType] = append(fileList[fileType], relativePath)
					}
				}
			}
		}
	}
	return fileList["f,l"], fileList["d"], nil
}

// isFBDAVolume check if storageClass is of FBDA volume.
func isFBDAVolume(storageClass *storagev1.StorageClass) bool {
	backendFBDAStorageClassKey := "backend"
	backendFBDAStorageClassVal := "pure_file"
	if val, ok := storageClass.Parameters[backendFBDAStorageClassKey]; ok && val == backendFBDAStorageClassVal {
		return true
	}
	return false
}

// isStorageClassPresent checks whether the storage class already present in the cluster.
func IsStorageClassPresent(storageClassName string) (bool, error) {
	k8sStorage := storage.Instance()
	storageClasses, err := k8sStorage.GetAllStorageClasses()
	if err != nil {
		return false, err
	}
	for _, storageClass := range storageClasses.Items {
		if storageClass.GetName() == storageClassName {
			return true, nil
		}
	}
	return false, nil
}

// dumpMongodbCollectionOnConsole Dumps the collection mentioned on the console for debugging
func dumpMongodbCollectionOnConsole(kubeConfigFile string, collectionName string, mongodbusername string, password string) error {
	// Getting Px Backup Namespace
	pxBackupNamespace, err := backup.GetPxBackupNamespace()
	if err != nil {
		return err
	}
	// Getting the mongodb collection objects
	output, err := kubectlExec([]string{fmt.Sprintf("--kubeconfig=%v", kubeConfigFile), "exec", "-it", "pxc-backup-mongodb-0", "-n", pxBackupNamespace, "--", "mongo", "--host", "localhost", "--port", "27017", "--username", mongodbusername, "--password", password, "--authenticationDatabase", "admin", "px-backup", "--eval", fmt.Sprintf("\"db.%s.find()\"", collectionName)})
	if err != nil {
		return err
	}

	// Dumping the collection
	log.InfoD(fmt.Sprintf(
		"Collection dump for %s collection",
		collectionName,
	))
	log.InfoD(output)

	return nil
}

// validateCRCleanup validates CR cleanup created during backup or restore
func validateCRCleanup(resourceInterface interface{},
	ctx context1.Context) error {

	log.InfoD("Validating CR cleanup")

	// TODO : This needs to be removed in future once stork client is integrated for GKE in automation
	if GetClusterProviders()[0] == "gke" {
		log.Infof("Skipping CR cleanup validation in case of GKE")
		return nil
	}

	var allCRs []string
	var err error
	var getCRMethod func(string, *api.ClusterObject) ([]string, error)
	var clusterName string
	var resourceNamespaces []string
	var resourceName string
	var orgID string
	var isValidCluster = false

	if currentObject, ok := resourceInterface.(*api.BackupObject); ok {
		// Creating object and variables from backup object
		getCRMethod = GetBackupCRs
		clusterName = currentObject.Cluster
		orgID = currentObject.OrgId
		resourceNamespaces = currentObject.Namespaces
		resourceName = currentObject.Name
	} else if currentObject, ok := resourceInterface.(*api.RestoreObject); ok {
		// Creating object and variables from Restore object
		getCRMethod = GetRestoreCRs
		clusterName = currentObject.Cluster
		for _, value := range currentObject.RestoreInfo.NamespaceMapping {
			resourceNamespaces = append(resourceNamespaces, value)
		}
		orgID = currentObject.OrgId
		resourceName = currentObject.Name
	}

	// Below code is added to skip the CR cleanup validation in case of synced backup
	// For synced backup the cluster name has uuid suffix which is not supported/handled
	// While creating the cluster object

	// Fetching all clusters
	enumerateClusterRequest := &api.ClusterEnumerateRequest{
		OrgId: orgID,
	}
	enumerateClusterResponse, err := Inst().Backup.EnumerateAllCluster(ctx, enumerateClusterRequest)

	// Comparing cluster names to the name from backup inspect response
	for _, clusterObj := range enumerateClusterResponse.GetClusters() {
		if clusterObj.Name == clusterName {
			isValidCluster = true
			break
		}
	}

	if !isValidCluster {
		log.Infof("%s looks to be a synced backup, skipping CR cleanup validation", clusterName)
		return nil
	}

	backupDriver := Inst().Backup
	clusterUID, err := backupDriver.GetClusterUID(ctx, orgID, clusterName)
	if err != nil {
		return err
	}

	currentAdminNamespace, _ := getCurrentAdminNamespace()
	if len(resourceNamespaces) == 1 {
		currentAdminNamespace = resourceNamespaces[0]
	}

	driveName := Inst().Backup
	clusterReq := &api.ClusterInspectRequest{OrgId: orgID, Name: clusterName, IncludeSecrets: true, Uid: clusterUID}
	clusterResp, err := driveName.InspectCluster(ctx, clusterReq)
	if err != nil {
		return err
	}
	clusterObj := clusterResp.GetCluster()
	validateCRCleanupInNamespace := func() (interface{}, bool, error) {
		allCRs, err = getCRMethod(currentAdminNamespace, clusterObj)
		if err != nil {
			return nil, true, err
		}

		log.Infof("All CRs in [%s] are [%v]", currentAdminNamespace, allCRs)
		for _, eachCR := range allCRs {
			if strings.Contains(eachCR, resourceName) {
				log.Infof("CR found for [%s] under [%s] namespace", allCRs, currentAdminNamespace)
				return nil, true, fmt.Errorf("CR cleanup validation failed for - [%s]", resourceName)
			}
		}

		return nil, false, nil
	}

	_, err = task.DoRetryWithTimeout(validateCRCleanupInNamespace, 5*time.Minute, 5*time.Second)

	return err

}

// SuspendAndDeleteAllSchedulesForUsers suspends and delete the backup schedule for a give list of users
func SuspendAndDeleteAllSchedulesForUsers(userNames []string, clusterName string, orgID string, deleteBackupFlag bool) error {

	for _, user := range userNames {
		log.InfoD("Getting context for non admin user %s", user)
		ctx, err := backup.GetNonAdminCtx(user, CommonPassword)
		if err != nil {
			return err
		}

		SchedulePolices, err := Inst().Backup.GetAllSchedulePolicies(ctx, orgID)
		if err != nil {
			return err
		}
		log.InfoD("Getting list of all schedule polices %s", SchedulePolices)

		for _, schedulePolicyName := range SchedulePolices {
			clusterUID, err := Inst().Backup.GetClusterUID(ctx, orgID, clusterName)
			if err != nil {
				return err
			}
			backupEnumreateRequest := &api.BackupScheduleEnumerateRequest{
				OrgId: orgID,
				EnumerateOptions: &api.EnumerateOptions{
					ClusterNameFilter: clusterName,
					ClusterUidFilter:  clusterUID,
				},
			}
			listOfBackupSchedules, err := Inst().Backup.EnumerateBackupScheduleByUser(ctx, backupEnumreateRequest)
			if err != nil {
				return err
			}
			for _, backupScheduleName := range listOfBackupSchedules.GetBackupSchedules() {
				log.InfoD("Suspend and delete backup schedule [%s] for schedule policy [%s]", backupScheduleName, schedulePolicyName)
				err := SuspendAndDeleteSchedule(backupScheduleName.GetName(), schedulePolicyName, clusterName, orgID, ctx, deleteBackupFlag)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// SuspendAndDeleteSchedule suspends and deletes the backup schedule
func SuspendAndDeleteSchedule(backupScheduleName string, schedulePolicyName string, clusterName string, orgID string, ctx context1.Context, deleteBackupFlag bool) error {
	backupDriver := Inst().Backup
	backupScheduleUID, err := GetScheduleUID(backupScheduleName, orgID, ctx)
	if err != nil {
		return err
	}
	schPolicyUID, err := Inst().Backup.GetSchedulePolicyUid(orgID, ctx, schedulePolicyName)
	if err != nil {
		return err
	}
	bkpScheduleSuspendRequest := &api.BackupScheduleUpdateRequest{
		CreateMetadata: &api.CreateMetadata{Name: backupScheduleName, OrgId: orgID, Uid: backupScheduleUID},
		Suspend:        true,
		SchedulePolicyRef: &api.ObjectRef{
			Name: schedulePolicyName,
			Uid:  schPolicyUID,
		},
	}
	log.InfoD("Suspending backup schedule %s", backupScheduleName)
	_, err = backupDriver.UpdateBackupSchedule(ctx, bkpScheduleSuspendRequest)
	if err != nil {
		return err
	}
	log.Infof("Verifying if the schedule is suspended by getting the suspended state of the schedule by inspecting")
	backupScheduleInspectRequest := &api.BackupScheduleInspectRequest{
		OrgId: orgID,
		Name:  backupScheduleName,
		Uid:   "",
	}
	validateScheduleStatus := func() (interface{}, bool, error) {
		resp, err := backupDriver.InspectBackupSchedule(ctx, backupScheduleInspectRequest)
		if err != nil {
			return nil, false, err
		}
		if resp.GetBackupSchedule().BackupScheduleInfo.GetSuspend() != true {
			return nil, true, fmt.Errorf("backup Schedule status after suspending is %v: ", resp.GetBackupSchedule().BackupScheduleInfo.GetSuspend())
		}
		return nil, false, nil
	}
	_, err = task.DoRetryWithTimeout(validateScheduleStatus, 2*time.Minute, 5*time.Second)
	if err != nil {
		return err
	}
	log.InfoD("Deleting backup schedule %s", backupScheduleName)
	bkpScheduleDeleteRequest := &api.BackupScheduleDeleteRequest{
		OrgId: orgID,
		Name:  backupScheduleName,
		// DeleteBackups indicates whether the cloud backup files need to
		// be deleted or retained.
		DeleteBackups: deleteBackupFlag,
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
		backupScheduleDeleteTimeout,
		backupScheduleDeleteRetryTime)
	if err != nil {
		return err
	}
	return nil
}

// ValidateCustomResourceRestores validates restore taken of custom resource
func ValidateCustomResourceRestores(ctx context1.Context, orgID string, resourceList []string, restoreContextMap map[string][]*scheduler.Context, clusterName string) error {
	if clusterName == "source-cluster" {
		err := SetSourceKubeConfig()
		if err != nil {
			return err
		}
	} else if clusterName == "destination-cluster" {
		err := SetDestinationKubeConfig()
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("cluster name provided is not correct %s", clusterName)
	}
	errChan := make(chan error, 100)
	var errList []error
	var wg sync.WaitGroup
	for restoreName, contexts := range restoreContextMap {
		contexts := contexts
		restoreInspectRequest := &api.RestoreInspectRequest{
			Name:  restoreName,
			OrgId: orgID,
		}
		response, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
		if err != nil {
			return err
		}
		restoreObj := response.GetRestore()
		nsMapping := restoreObj.NamespaceMapping
		scMapping := restoreObj.StorageClassMapping
		log.Infof("Namespace mapping is %s, storage class mapping is %s for restore %s", nsMapping, scMapping, restoreName)
		wg.Add(1)
		go func(restoreName string, nsMapping map[string]string, scMapping map[string]string) {
			defer GinkgoRecover()
			defer wg.Done()
			log.InfoD("Validating restore [%s] with custom resources %v", restoreName, resourceList)
			var expectedRestoredAppContextList []*scheduler.Context
			for _, context := range contexts {
				expectedRestoredAppContext, err := CloneAppContextAndTransformWithMappings(context, nsMapping, scMapping, true)
				if err != nil {
					errChan <- err
					return
				}
				expectedRestoredAppContextList = append(expectedRestoredAppContextList, expectedRestoredAppContext)
			}
			err := ValidateRestore(ctx, restoreName, orgID, expectedRestoredAppContextList, resourceList)
			if err != nil {
				errChan <- err
				return
			}
		}(restoreName, nsMapping, scMapping)
	}
	wg.Wait()
	close(errChan)
	for err := range errChan {
		errList = append(errList, err)
	}
	errStrings := make([]string, 0)
	for _, err := range errList {
		if err != nil {
			errStrings = append(errStrings, err.Error())
		}
	}
	if len(errStrings) > 0 {
		return fmt.Errorf("ValidateRestore Errors: {%s}", strings.Join(errStrings, "}\n{"))
	}
	return nil
}

// ScaleApplicationToDesiredReplicas scales Application to desired replicas for migrated application namespace.
func ScaleApplicationToDesiredReplicas(namespace string) error {
	var options metav1.ListOptions
	var parsedReplicas int
	deploymentList, err := apps.Instance().ListDeployments(namespace, options)
	if err != nil {
		return err
	}
	statefulSetList, err := apps.Instance().ListStatefulSets(namespace, options)
	if err != nil {
		return err
	}
	if len(deploymentList.Items) != 0 {
		deployments := deploymentList.Items
		for _, deployment := range deployments {
			if replicas, present := deployment.Annotations[migration.StorkMigrationReplicasAnnotation]; present {
				parsedReplicas, _ = strconv.Atoi(replicas)

			}
			deploymentObj, err := apps.Instance().GetDeployment(deployment.Name, namespace)
			if err != nil {
				return err
			}
			*deploymentObj.Spec.Replicas = int32(parsedReplicas)
			updatedBackupDeploymentobj, err := apps.Instance().UpdateDeployment(deploymentObj)
			if err != nil {
				return err
			}
			log.Infof("Deployment [%s] replica count after scaling to %d  is %v", deployment.Name, int32(parsedReplicas), *updatedBackupDeploymentobj.Spec.Replicas)
		}
	}

	if len(statefulSetList.Items) != 0 {
		statefulSets := statefulSetList.Items
		for _, statefulSet := range statefulSets {
			if replicas, present := statefulSet.Annotations[migration.StorkMigrationReplicasAnnotation]; present {
				parsedReplicas, _ = strconv.Atoi(replicas)

			}
			statefulSetObj, err := apps.Instance().GetStatefulSet(statefulSet.Name, namespace)
			if err != nil {
				return err
			}
			*statefulSetObj.Spec.Replicas = int32(parsedReplicas)
			updatedBackupstatefulSetobj, err := apps.Instance().UpdateStatefulSet(statefulSetObj)
			if err != nil {
				return err
			}
			log.Infof("statefulSet [%s] replica count after scaling to %d  is %v", statefulSet.Name, int32(parsedReplicas), *updatedBackupstatefulSetobj.Spec.Replicas)
		}

	}

	return nil
}

// AddNodeToVirtualMachine applies node selector to the virtual machine
func AddNodeToVirtualMachine(vm kubevirtv1.VirtualMachine, nodeSelector map[string]string) error {
	k8sKubevirt := kubevirt.Instance()

	vm.Spec.Template.Spec.NodeSelector = nodeSelector

	vmUpdate, err := k8sKubevirt.UpdateVirtualMachine(&vm)
	if err != nil {
		return err
	}

	log.Infof("Node selector for [%s] is updated successfully to [%v]", vmUpdate.Name, vmUpdate.Spec.Template.Spec.NodeSelector)

	return nil
}

// AddPVCsToVirtualMachine adds PVCs to virtual machine
func AddPVCsToVirtualMachine(vm kubevirtv1.VirtualMachine, pvcs []*corev1.PersistentVolumeClaim) error {
	k8sKubevirt := kubevirt.Instance()
	var volumes []kubevirtv1.Volume
	var disks []kubevirtv1.Disk
	for i, pvc := range pvcs {
		log.Infof("Adding PVC [%s] to VM [%s]", pvc.Name, vm.Name)
		volumes = append(volumes, kubevirtv1.Volume{
			Name: fmt.Sprintf("%s-%d", "datavolume-additional", i),
			VolumeSource: kubevirtv1.VolumeSource{
				PersistentVolumeClaim: &kubevirtv1.PersistentVolumeClaimVolumeSource{
					PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvc.Name,
					},
					Hotpluggable: false,
				},
			},
		})
		disks = append(disks, kubevirtv1.Disk{
			Name:       fmt.Sprintf("%s-%d", "datavolume-additional", i),
			DiskDevice: kubevirtv1.DiskDevice{Disk: &kubevirtv1.DiskTarget{Bus: kubevirtv1.DiskBusVirtio}},
		})

	}
	vm.Spec.Template.Spec.Volumes = append(vm.Spec.Template.Spec.Volumes, volumes...)

	vm.Spec.Template.Spec.Domain.Devices.Disks = append(vm.Spec.Template.Spec.Domain.Devices.Disks, disks...)

	vmUpdate, err := k8sKubevirt.UpdateVirtualMachine(&vm)
	if err != nil {
		return err
	}
	log.Infof("[%d] volumes added to VM [%s]", len(pvcs), vmUpdate.Name)

	return nil
}

// CreatePVCsForVM creates PVCs for the VM
func CreatePVCsForVM(vm kubevirtv1.VirtualMachine, numberOfPVCs int, storageClassName, resourceStorage string) ([]*corev1.PersistentVolumeClaim, error) {
	pvcs := make([]*corev1.PersistentVolumeClaim, 0)
	for i := 0; i < numberOfPVCs; i++ {
		pvcName := fmt.Sprintf("%s-%s-%d", "pvc-new", vm.Name, i)
		pvc, err := core.Instance().CreatePersistentVolumeClaim(&corev1.PersistentVolumeClaim{
			TypeMeta: metav1.TypeMeta{
				Kind: "PersistentVolumeClaim",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: vm.Namespace,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
				StorageClassName: &storageClassName,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse(resourceStorage),
					},
				},
			},
		})
		if err != nil {
			return nil, err
		}
		// adding kind to pvc object
		pvc.Kind = "PersistentVolumeClaim"
		pvcs = append(pvcs, pvc)
	}
	return pvcs, nil
}

// CompareNodeAndStatusOfVMInNamespace compares the status and Nodes of the VMI from a particular namespace
func CompareNodeAndStatusOfVMInNamespace(namespace string, expectedNode node.Node, expectedState string, ctx context1.Context) error {
	k8sKubevirt := kubevirt.Instance()

	allVmsInNamespaces, err := GetAllVMsInNamespace(namespace)
	if err != nil {
		return err
	}

	for _, eachVM := range allVmsInNamespaces {
		vmi, err := k8sKubevirt.GetVirtualMachineInstance(ctx, eachVM.Name, eachVM.Namespace)
		if err != nil {
			return err
		}
		log.Infof("Current state of [%s] is [%s]", vmi.Name, vmi.Status.Phase)
		log.Infof("Node of [%s] is [%s]", vmi.Name, vmi.Status.NodeName)
		if string(vmi.Status.Phase) == expectedState {
			if expectedState == "Scheduling" {
				return nil
			}
		} else {
			return fmt.Errorf("VMI state Validation failed for [%s]. Expected State - [%s], State Found [%s]", vmi.Name, expectedState, vmi.Status.Phase)
		}
		if vmi.Status.NodeName != expectedNode.Name {
			return fmt.Errorf("Node Validation failed for [%s]. Expected Node - [%s], Node Found [%s]", vmi.Name, expectedNode.Name, vmi.Status.NodeName)
		}
	}

	return nil
}

// DeleteAllVMsInNamespace delete all the Kubevirt VMs in the given namespace
func DeleteAllVMsInNamespace(namespace string) error {
	k8sKubevirt := kubevirt.Instance()
	var wg sync.WaitGroup
	var mutex sync.Mutex
	errors := make([]string, 0)

	vms, err := k8sKubevirt.ListVirtualMachines(namespace)
	if err != nil {
		return err
	}
	for _, vm := range vms.Items {
		wg.Add(1)
		go func(vm kubevirtv1.VirtualMachine) {
			defer GinkgoRecover()
			defer wg.Done()
			err := k8sKubevirt.DeleteVirtualMachine(vm.Name, namespace)
			if err != nil {
				mutex.Lock()
				errors = append(errors, fmt.Sprintf("Failed to delete [%s]. Error - [%s]", vm.Name, err.Error()))
				mutex.Unlock()
			} else {
				log.Infof("Deleted vm - %s", vm.Name)
			}
		}(vm)
	}
	wg.Wait()

	if len(errors) > 0 {
		return fmt.Errorf("Errors occured while deleting VMs. Errors:\n\n %s", strings.Join(errors, "\n"))
	}

	return nil
}

// GetCRObject queries and returns any CRD defined
func getCRObject(clusterObj *api.ClusterObject, namespace string, customResourceObjectDetails customResourceObjectDetails) (*unstructured.UnstructuredList, error) {

	ctx, err := backup.GetAdminCtxFromSecret()
	config, err := portworx.GetKubernetesRestConfig(clusterObj)
	if err != nil {
		return nil, err
	}

	dynamicClient := dynamic.NewForConfigOrDie(config)

	// Get the GVR of the CRD.
	gvr := metav1.GroupVersionResource{
		Group:    customResourceObjectDetails.Group,
		Version:  customResourceObjectDetails.Version,
		Resource: customResourceObjectDetails.Resource,
	}
	objects, err := dynamicClient.Resource(schema.GroupVersionResource(gvr)).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return objects, nil
}

// GetAllBackupCRObjects returns names of all backup CR object found in the cluster
func GetAllBackupCRObjects(clusterObj *api.ClusterObject) []string {

	var allBackupCrs = make([]string, 0)
	for crName, definition := range crListMap {
		allCurrentCrs, err := getCRObject(clusterObj, "", definition)
		if err != nil {
			log.Infof("Some error occurred while checking for [%s], Error - [%s]", crName, err.Error())
		} else {
			if len(allCurrentCrs.Items) > 0 {
				log.Infof("Found [%s] object in the cluster", crName)
				for _, item := range allCurrentCrs.Items {
					// Skip the CR if present in SkipResources
					if !slices.Contains(definition.SkipResources, item.GetName()) {
						allBackupCrs = append(allBackupCrs, fmt.Sprintf("%s-%s", item.GetName(), item.GetNamespace()))
					}
				}
			}
		}
	}

	return allBackupCrs
}

// GetUpdatedKubeVirtVMSpecForBackup will update the AppContext by removing out of scope backup object for kubevirt VM.
func GetUpdatedKubeVirtVMSpecForBackup(scheduledAppContextsToBackup []*scheduler.Context) error {

	var removeSpecs []interface{}
	for _, scheduledAppContext := range scheduledAppContextsToBackup {
		// Removing specs which are outside the scope of VM Backup
		for _, spec := range scheduledAppContext.App.SpecList {
			// VM Backup will not consider service object for now
			if appSpec, ok := spec.(*corev1.Service); ok {
				removeSpecs = append(removeSpecs, appSpec)
			}
			// TODO: Add more types of specs to remove depending on the app context
		}
		err := Inst().S.RemoveAppSpecsByName(scheduledAppContext, removeSpecs)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetPVCListForNamespace retrieves the list of PVCs in the specified namespace.
func GetPVCListForNamespace(namespace string) ([]string, error) {
	k8sCore := core.Instance()
	pvcList, err := k8sCore.GetPersistentVolumeClaims(namespace, make(map[string]string))
	if err != nil {
		return nil, fmt.Errorf("failed to get PVCs in namespace %s: %w", namespace, err)
	}
	// Extract PVC names from the list
	var pvcNameList []string
	for _, pvc := range pvcList.Items {
		pvcNameList = append(pvcNameList, pvc.Name)
	}
	return pvcNameList, nil
}

// ValidatePVCCleanup checks if there is a mismatch between the original PVC list and the current one.
func ValidatePVCCleanup(pvcBefore, pvcAfter []string) error {
	pvcBeforeSet := make(map[string]bool)
	pvcAfterSet := make(map[string]bool)

	// Populate sets for PVC names before and after the run
	for _, pvc := range pvcBefore {
		pvcBeforeSet[pvc] = true
	}

	for _, pvc := range pvcAfter {
		pvcAfterSet[pvc] = true
	}

	// Check for missing PVCs after the run
	for pvc := range pvcBeforeSet {
		if !pvcAfterSet[pvc] {
			fmt.Printf("PVC '%s' is present before the run but not after the run\n", pvc)
			return fmt.Errorf("mismatch in PVC list before and after the run")
		}
	}

	// Check for extra PVCs after the run
	for pvc := range pvcAfterSet {
		if !pvcBeforeSet[pvc] {
			fmt.Printf("PVC '%s' is present after the run but not before the run\n", pvc)
			return fmt.Errorf("mismatch in PVC list before and after the run")
		}
	}

	return nil
}
