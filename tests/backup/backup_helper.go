package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/torpedo/pkg/osutils"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/hashicorp/go-version"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	. "github.com/onsi/ginkgo"
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
	. "github.com/portworx/torpedo/tests"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	cloudAccountDeleteTimeout                 = 30 * time.Minute
	cloudAccountDeleteRetryTime               = 30 * time.Second
	storkDeploymentName                       = "stork"
	defaultStorkDeploymentNamespace           = "kube-system"
	upgradeStorkImage                         = "UPGRADE_STORK_IMAGE"
	latestStorkImage                          = "openstorage/stork:23.2.0"
	restoreNamePrefix                         = "tp-restore"
	destinationClusterName                    = "destination-cluster"
	appReadinessTimeout                       = 10 * time.Minute
	taskNamePrefix                            = "pxbackuptask"
	orgID                                     = "default"
	usersToBeCreated                          = "USERS_TO_CREATE"
	groupsToBeCreated                         = "GROUPS_TO_CREATE"
	maxUsersInGroup                           = "MAX_USERS_IN_GROUP"
	maxBackupsToBeCreated                     = "MAX_BACKUPS"
	maxWaitPeriodForBackupCompletionInMinutes = 40
	maxWaitPeriodForRestoreCompletionInMinute = 40
	maxWaitPeriodForBackupJobCancellation     = 20
	maxWaitPeriodForRestoreJobCancellation    = 20
	restoreJobCancellationRetryTime           = 30
	restoreJobProgressRetryTime               = 1
	backupJobCancellationRetryTime            = 5
	K8sNodeReadyTimeout                       = 10
	K8sNodeRetryInterval                      = 30
	globalAWSBucketPrefix                     = "global-aws"
	globalAzureBucketPrefix                   = "global-azure"
	globalGCPBucketPrefix                     = "global-gcp"
	globalAWSLockedBucketPrefix               = "global-aws-locked"
	globalAzureLockedBucketPrefix             = "global-azure-locked"
	globalGCPLockedBucketPrefix               = "global-gcp-locked"
	mongodbStatefulset                        = "pxc-backup-mongodb"
	pxBackupDeployment                        = "px-backup"
	backupDeleteTimeout                       = 20 * time.Minute
	backupDeleteRetryTime                     = 30 * time.Second
	backupLocationDeleteTimeout               = 30 * time.Minute
	backupLocationDeleteRetryTime             = 30 * time.Second
	rebootNodeTimeout                         = 1 * time.Minute
	rebootNodeTimeBeforeRetry                 = 5 * time.Second
	latestPxBackupVersion                     = "2.4.0"
	latestPxBackupHelmBranch                  = "master"
	pxCentralPostInstallHookJobName           = "pxcentral-post-install-hook"
	quickMaintenancePod                       = "quick-maintenance-repo"
	fullMaintenancePod                        = "full-maintenance-repo"
	jobDeleteTimeout                          = 5 * time.Minute
	jobDeleteRetryTime                        = 10 * time.Second
	podStatusTimeOut                          = 20 * time.Minute
	podStatusRetryTime                        = 30 * time.Second
	licenseCountUpdateTimeout                 = 15 * time.Minute
	licenseCountUpdateRetryTime               = 1 * time.Minute
	podReadyTimeout                           = 30 * time.Minute
	podReadyRetryTime                         = 30 * time.Second
)

var (
	// User should keep updating preRuleApp, postRuleApp
	preRuleApp                  = []string{"cassandra", "postgres"}
	postRuleApp                 = []string{"cassandra"}
	globalAWSBucketName         string
	globalAzureBucketName       string
	globalGCPBucketName         string
	globalAWSLockedBucketName   string
	globalAzureLockedBucketName string
	globalGCPLockedBucketName   string
	cloudProviders              = []string{"aws"}
	commonPassword              string
)

type userRoleAccess struct {
	user     string
	roles    backup.PxBackupRole
	accesses BackupAccess
	context  context.Context
}

type userAccessContext struct {
	user     string
	accesses BackupAccess
	context  context.Context
}

var backupAccessKeyValue = map[BackupAccess]string{
	1: "ViewOnlyAccess",
	2: "RestoreAccess",
	3: "FullAccess",
}

var storkLabel = map[string]string{
	"name": "stork",
}

type BackupAccess int32

type ReplacePolicy_Type int32

const (
	ReplacePolicy_Invalid ReplacePolicy_Type = 0
	ReplacePolicy_Retain  ReplacePolicy_Type = 1
	ReplacePolicy_Delete  ReplacePolicy_Type = 2
)

const (
	ViewOnlyAccess BackupAccess = 1
	RestoreAccess               = 2
	FullAccess                  = 3
)

// Set default provider as aws
func getProviders() []string {
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

// CreateBackup creates backup
func CreateBackup(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, ctx context.Context) error {

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
	_, err := backupDriver.CreateBackup(ctx, bkpCreateRequest)
	if err != nil {
		return err
	}
	err = backupSuccessCheck(backupName, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Backup [%s] created successfully", backupName)
	return nil
}

func UpdateBackup(backupName string, backupUid string, orgId string, cloudCred string, cloudCredUID string, ctx context.Context) (*api.BackupUpdateResponse, error) {
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

// CreateBackupWithCustomResourceType creates backup with custom resources
func CreateBackupWithCustomResourceType(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, resourceType []string, ctx context.Context) error {

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
		ResourceTypes: resourceType,
	}
	_, err := backupDriver.CreateBackup(ctx, bkpCreateRequest)
	if err != nil {
		return err
	}
	err = backupSuccessCheck(backupName, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Backup [%s] created successfully", backupName)
	return nil
}

// CreateScheduleBackup creates a schedule backup
func CreateScheduleBackup(scheduleName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, schPolicyName string, schPolicyUID string, ctx context.Context) error {
	var firstScheduleBackupName string
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
	}
	_, err := backupDriver.CreateBackupSchedule(ctx, bkpSchCreateRequest)
	if err != nil {
		return err
	}
	time.Sleep(1 * time.Minute)
	firstScheduleBackupName, err = GetFirstScheduleBackupName(ctx, scheduleName, orgID)
	if err != nil {
		return err
	}
	err = backupSuccessCheck(firstScheduleBackupName, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Schedule backup [%s] created successfully", firstScheduleBackupName)
	return nil
}

// CreateBackupWithoutCheck creates backup without waiting for success
func CreateBackupWithoutCheck(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, ctx context.Context) (*api.BackupInspectResponse, error) {

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
	_, err := backupDriver.CreateBackup(ctx, bkpCreateRequest)
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

// CreateScheduleBackupWithoutCheck creates a schedule backup without waiting for success
func CreateScheduleBackupWithoutCheck(scheduleName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, schPolicyName string, schPolicyUID string, ctx context.Context) (*api.BackupScheduleInspectResponse, error) {
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
	}
	_, err := backupDriver.CreateBackupSchedule(ctx, bkpSchCreateRequest)
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
func ShareBackup(backupName string, groupNames []string, userNames []string, accessLevel BackupAccess, ctx context.Context) error {
	var bkpUid string
	backupDriver := Inst().Backup
	groupIDs := make([]string, 0)
	userIDs := make([]string, 0)

	bkpUid, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
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
		OrgId: orgID,
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
func ClusterUpdateBackupShare(clusterName string, groupNames []string, userNames []string, accessLevel BackupAccess, addUsersOrGroups bool, ctx context.Context) error {
	backupDriver := Inst().Backup
	groupIDs := make([]string, 0)
	userIDs := make([]string, 0)
	clusterUID, err := backupDriver.GetClusterUID(ctx, orgID, clusterName)
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
			OrgId:          orgID,
			Name:           clusterName,
			AddBackupShare: backupShare,
			DelBackupShare: nil,
			Uid:            clusterUID,
		}
	} else {
		clusterBackupShareUpdateRequest = &api.ClusterBackupShareUpdateRequest{
			OrgId:          orgID,
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
		clusterReq := &api.ClusterInspectRequest{OrgId: orgID, Name: clusterName, IncludeSecrets: true}
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
		OrgId: orgID,
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
	orgID string, ctx context.Context, storageClassMapping map[string]string) error {

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
	}
	_, err = backupDriver.CreateRestore(ctx, createRestoreReq)
	if err != nil {
		return err
	}
	err = restoreSuccessCheck(restoreName, orgID, maxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Restore [%s] created successfully", restoreName)
	return nil
}

// CreateRestoreWithReplacePolicy Creates in-place restore and waits for it to complete
func CreateRestoreWithReplacePolicy(restoreName string, backupName string, namespaceMapping map[string]string, clusterName string,
	orgID string, ctx context.Context, storageClassMapping map[string]string, replacePolicy ReplacePolicy_Type) error {

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
	err = restoreSuccessWithReplacePolicy(restoreName, orgID, maxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx, replacePolicy)
	if err != nil {
		return err
	}
	log.Infof("Restore [%s] created successfully", restoreName)
	return nil
}

// CreateRestoreWithUID creates restore with UID
func CreateRestoreWithUID(restoreName string, backupName string, namespaceMapping map[string]string, clusterName string,
	orgID string, ctx context.Context, storageClassMapping map[string]string, backupUID string) error {

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
	err = restoreSuccessCheck(restoreName, orgID, maxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Restore [%s] created successfully", restoreName)
	return nil
}

// CreateRestoreWithoutCheck creates restore without waiting for completion
func CreateRestoreWithoutCheck(restoreName string, backupName string,
	namespaceMapping map[string]string, clusterName string, orgID string, ctx context.Context) (*api.RestoreInspectResponse, error) {

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

func getSizeOfMountPoint(podName string, namespace string, kubeConfigFile string) (int, error) {
	var number int
	ret, err := kubectlExec([]string{fmt.Sprintf("--kubeconfig=%v", kubeConfigFile), "exec", "-it", podName, "-n", namespace, "--", "/bin/df"})
	if err != nil {
		return 0, err
	}
	for _, line := range strings.SplitAfter(ret, "\n") {
		if strings.Contains(line, "pxd") {
			ret = strings.Fields(line)[3]
		}
	}
	number, err = strconv.Atoi(ret)
	if err != nil {
		return 0, err
	}
	return number, nil
}

func kubectlExec(arguments []string) (string, error) {
	if len(arguments) == 0 {
		return "", fmt.Errorf("no arguments supplied for kubectl command")
	}
	cmd := exec.Command("kubectl", arguments...)
	output, err := cmd.Output()
	log.InfoD("Command '%s'", cmd.String())
	log.Infof("Command output for '%s': %s", cmd.String(), string(output))
	if err != nil {
		return "", fmt.Errorf("error on executing kubectl command, Err: %+v", err)
	}
	return string(output), err
}

func createUsers(numberOfUsers int) []string {
	users := make([]string, 0)
	log.InfoD("Creating %d users", numberOfUsers)
	var wg sync.WaitGroup
	for i := 1; i <= numberOfUsers; i++ {
		userName := fmt.Sprintf("testuser%v-%v", i, time.Now().Unix())
		firstName := fmt.Sprintf("FirstName%v", i)
		lastName := fmt.Sprintf("LastName%v", i)
		email := fmt.Sprintf("%v@cnbu.com", userName)
		wg.Add(1)
		go func(userName, firstName, lastName, email string) {
			defer GinkgoRecover()
			defer wg.Done()
			err := backup.AddUser(userName, firstName, lastName, email, commonPassword)
			Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Creating user - %s", userName))
			users = append(users, userName)
		}(userName, firstName, lastName, email)
	}
	wg.Wait()
	return users
}

// CleanupCloudSettingsAndClusters removes the backup location(s), cloud accounts and source/destination clusters for the given context
func CleanupCloudSettingsAndClusters(backupLocationMap map[string]string, credName string, cloudCredUID string, ctx context.Context) {
	log.InfoD("Cleaning backup locations in map [%v], cloud credential [%s], source [%s] and destination [%s] cluster", backupLocationMap, credName, SourceClusterName, destinationClusterName)
	if len(backupLocationMap) != 0 {
		for backupLocationUID, bkpLocationName := range backupLocationMap {
			err := DeleteBackupLocation(bkpLocationName, backupLocationUID, orgID, true)
			Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of backup location [%s]", bkpLocationName))
			backupLocationDeleteStatusCheck := func() (interface{}, bool, error) {
				status, err := IsBackupLocationPresent(bkpLocationName, ctx, orgID)
				if err != nil {
					return "", true, fmt.Errorf("backup location %s still present with error %v", bkpLocationName, err)
				}
				if status {
					return "", true, fmt.Errorf("backup location %s is not deleted yet", bkpLocationName)
				}
				return "", false, nil
			}
			_, err = task.DoRetryWithTimeout(backupLocationDeleteStatusCheck, cloudAccountDeleteTimeout, cloudAccountDeleteRetryTime)
			Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup location deletion status %s", bkpLocationName))
		}
		err := DeleteCloudCredential(credName, orgID, cloudCredUID)
		Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of cloud cred [%s]", credName))
		cloudCredDeleteStatus := func() (interface{}, bool, error) {
			status, err := IsCloudCredPresent(credName, ctx, orgID)
			if err != nil {
				return "", true, fmt.Errorf("cloud cred %s still present with error %v", credName, err)
			}
			if status {
				return "", true, fmt.Errorf("cloud cred %s is not deleted yet", credName)
			}
			return "", false, nil
		}
		_, err = task.DoRetryWithTimeout(cloudCredDeleteStatus, cloudAccountDeleteTimeout, cloudAccountDeleteRetryTime)
		Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cloud cred %s", credName))
	}
	err := DeleteCluster(SourceClusterName, orgID, ctx)
	Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", SourceClusterName))
	err = DeleteCluster(destinationClusterName, orgID, ctx)
	Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", destinationClusterName))
}

// AddRoleAndAccessToUsers assigns role and access level to the users
// AddRoleAndAccessToUsers return map whose key is userRoleAccess and value is backup for each user
func AddRoleAndAccessToUsers(users []string, backupNames []string) (map[userRoleAccess]string, error) {
	var access BackupAccess
	var role backup.PxBackupRole
	roleAccessUserBackupContext := make(map[userRoleAccess]string)
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
		ctxNonAdmin, err := backup.GetNonAdminCtx(users[i], commonPassword)
		if err != nil {
			return nil, err
		}
		userRoleAccessContext := userRoleAccess{users[i], role, access, ctxNonAdmin}
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
	userCtx, err := backup.GetNonAdminCtx(user, commonPassword)
	Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching %s user ctx", user))
	log.InfoD("Registering Source and Destination clusters from user context")
	err = CreateSourceAndDestClusters(orgID, "", "", userCtx)
	Inst().Dash.VerifyFatal(err, nil, "Creating source and destination cluster")
	log.InfoD("Validating if user [%s] with access [%v] can restore and delete backup %s or not", user, backupAccessKeyValue[access], backupName)
	backupDriver := Inst().Backup
	switch access {
	case ViewOnlyAccess:
		// Try restore with user having ViewOnlyAccess and it should fail
		err := CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, userCtx, make(map[string]string))
		log.Infof("The expected error returned is %v", err)
		Inst().Dash.VerifyFatal(strings.Contains(err.Error(), "failed to retrieve backup location"), true, "Verifying backup restore is not possible")
		// Try to delete the backup with user having ViewOnlyAccess, and it should not pass
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
		Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Getting backup UID for- %s", backupName))
		// Delete backup to confirm that the user has ViewOnlyAccess and cannot delete backup
		_, err = DeleteBackup(backupName, backupUID, orgID, userCtx)
		log.Infof("The expected error returned is %v", err)
		Inst().Dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")

	case RestoreAccess:
		// Try restore with user having RestoreAccess and it should pass
		err := CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, userCtx, make(map[string]string))
		Inst().Dash.VerifyFatal(err, nil, "Verifying that restore is possible")
		// Try to delete the backup with user having RestoreAccess, and it should not pass
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
		Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Getting backup UID for- %s", backupName))
		// Delete backup to confirm that the user has Restore Access and delete backup should fail
		_, err = DeleteBackup(backupName, backupUID, orgID, userCtx)
		log.Infof("The expected error returned is %v", err)
		Inst().Dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")

	case FullAccess:
		// Try restore with user having FullAccess, and it should pass
		err := CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, userCtx, make(map[string]string))
		Inst().Dash.VerifyFatal(err, nil, "Verifying that restore is possible")
		// Try to delete the backup with user having FullAccess, and it should pass
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
		Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Getting backup UID for- %s", backupName))
		// Delete backup to confirm that the user has Full Access
		_, err = DeleteBackup(backupName, backupUID, orgID, userCtx)
		Inst().Dash.VerifyFatal(err, nil, "Verifying that delete backup is possible")
	}
}

func getEnv(environmentVariable string, defaultValue string) string {
	value, present := os.LookupEnv(environmentVariable)
	if !present {
		value = defaultValue
	}
	return value
}

// ShareBackupWithUsersAndAccessAssignment shares backup with multiple users with different access levels
// It returns a map with key as userAccessContext and value as backup shared
func ShareBackupWithUsersAndAccessAssignment(backupNames []string, users []string, ctx context.Context) (map[userAccessContext]string, error) {
	log.InfoD("Sharing backups with users with different access level")
	accessUserBackupContext := make(map[userAccessContext]string)
	var err error
	var ctxNonAdmin context.Context
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
		ctxNonAdmin, err = backup.GetNonAdminCtx(users[i], commonPassword)
		if err != nil {
			return accessUserBackupContext, fmt.Errorf("unable to get user context: %v", err)
		}
		accessContextUser := userAccessContext{users[i], access, ctxNonAdmin}
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
		OrgId: orgID}
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
		OrgId: orgID,
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

func generateEncryptionKey() string {
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

func GetScheduleUID(scheduleName string, orgID string, ctx context.Context) (string, error) {
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

func removeStringItemFromSlice(itemList []string, item []string) []string {
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

func getAllBackupLocations(ctx context.Context) (map[string]string, error) {
	backupLocationMap := make(map[string]string, 0)
	backupDriver := Inst().Backup
	backupLocationEnumerateRequest := &api.BackupLocationEnumerateRequest{
		OrgId: orgID,
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

func getAllCloudCredentials(ctx context.Context) (map[string]string, error) {
	cloudCredentialMap := make(map[string]string, 0)
	backupDriver := Inst().Backup
	cloudCredentialEnumerateRequest := &api.CloudCredentialEnumerateRequest{
		OrgId: orgID,
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

func GetAllRestoresNonAdminCtx(ctx context.Context) ([]string, error) {
	restoreNames := make([]string, 0)
	backupDriver := Inst().Backup
	restoreEnumerateRequest := &api.RestoreEnumerateRequest{
		OrgId: orgID,
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

// DeletePodWithLabelInNamespace kills pod with the given label in the given namespace
func DeletePodWithLabelInNamespace(namespace string, label map[string]string) error {
	pods, err := core.Instance().GetPods(namespace, label)
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		err := core.Instance().DeletePod(pod.GetName(), namespace, false)
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
func backupSuccessCheck(backupName string, orgID string, retryDuration time.Duration, retryInterval time.Duration, ctx context.Context) error {
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
	if err != nil {
		return err
	}
	return nil
}

// ValidateBackup validates a backup and returns a clone of the provided scheduledCtxs (and each of their specs) *after* filtering the specs to only include the resources that are in the backup.
// * An error can be returned *with* backedupAppContexts, so do check if the first variable returned is actually nil
// *
// * # Parameters
// *
// * requireAllScheduledCtxAreInBackup: This requires that Backup objects are a superset of objects in ScheduledCtx: If set to true, error is returned if something in scheduledCtx is not present in BackupCtx (set to false if not everything in scheduledCtx has been backed up, on purpose)
// * requireAllBackupAreInSomeScheduledCtx: This requires that ScheduledCtx objects are a superset of objects in Backup: If set to true, error is returned if something in Backup is not present in scheduledCtx. (set to false if deploying CRs)
// * NOTES: Setting both to true will check for strict equality of both sets. Set both to false if you just want to filter and get whatever is possible.
// *
// * NOTE: Ensure scheduledCtxs which correspond to the namespaces backed up in the backup are provided
// * NOTE: This function must be called after the backup is completed (with status Success/PartialSuccess)
func ValidateBackup(ctx context.Context, backupName string, orgID string, scheduledAppContexts []*scheduler.Context, requireAllScheduledCtxAreInBackup, requireAllBackupAreInSomeScheduledCtx bool, backupClusterConfigPath string) ([]*scheduler.Context, error) {
	log.InfoD("Validating backup [%s] in org [%s]", backupName, orgID)

	log.InfoD("Obtaining backup info for backup [%s]", backupName)
	backupDriver := Inst().Backup
	backupUid, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return nil, fmt.Errorf("GetBackupUID Err: %v", err)
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUid,
		OrgId: orgID,
	}
	backupInspectResponse, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return nil, fmt.Errorf("InspectBackup Err: %v", err)
	}

	backupStatus := backupInspectResponse.GetBackup().GetStatus().Status
	if backupStatus != api.BackupInfo_StatusInfo_Success &&
		backupStatus != api.BackupInfo_StatusInfo_PartialSuccess {
		return nil, fmt.Errorf("ValidateBackup requires backup [%s] to have a status of Success or PartialSuccess", backupName)
	}

	backedupAppContexts, ScheduledCtxNotFoundInBackupErrors, BackupNotFoundInAnyScheduledCtxErrors, otherErrors := GetBackupCtxsFromScheduledCtxs(backupInspectResponse, scheduledAppContexts)
	errArr := make([]error, 0)
	if requireAllScheduledCtxAreInBackup {
		errArr = append(errArr, ScheduledCtxNotFoundInBackupErrors...)
	}
	if requireAllBackupAreInSomeScheduledCtx {
		errArr = append(errArr, BackupNotFoundInAnyScheduledCtxErrors...)
	}
	errArr = append(errArr, otherErrors...)
	errGetBackupCtxsFromScheduledCtxs := ProcessMultipleErrors("GetBackupCtxsFromScheduledCtxs", errArr, true)

	errors := ValidateBackedUpVolumes(backupInspectResponse, scheduledAppContexts)
	errValidateBackedUpVolumes := ProcessMultipleErrors("ValidateBackedUpVolumes", errors, true)

	if errGetBackupCtxsFromScheduledCtxs != nil || errValidateBackedUpVolumes != nil {
		err = fmt.Errorf("GetBackupCtxsFromScheduledCtxs: %v ;; ValidateBackedUpVolumes: %v",
			errGetBackupCtxsFromScheduledCtxs, errValidateBackedUpVolumes)
	} else {
		err = nil
	}

	return backedupAppContexts, err
}

// ValidateBackedUpVolumes checks if the volumes have been backed up and done so rightly
// *
// * Returns:
// * otherErrors: all the other kind of error; Don't ignore these are they're serious errors
// *
// * NOTES:
// * - make sure to include Contexts of *all* namespaces which are supposed to contain the volumes
func ValidateBackedUpVolumes(backupInspectResponse *api.BackupInspectResponse,
	scheduledAppContexts []*scheduler.Context) (errors []error) {

	backupName := backupInspectResponse.Backup.Name
	backedupVolumes := backupInspectResponse.GetBackup().GetVolumes()
	backupNamespaces := backupInspectResponse.GetBackup().GetNamespaces()

	log.InfoD("ValidateBackedUpVolumes: Validating backed up volumes for backup [%s]", backupName)

	backedUpVolumesFound := make(map[*api.BackupInfo_Volume]bool)
	for _, volume := range backedupVolumes {
		backedUpVolumesFound[volume] = false
	}

	for _, clusterAppsContext := range scheduledAppContexts {

		// Checking if this cluster (namespace) is actually present in the backup
		clusterAppsContextNamespace := clusterAppsContext.ScheduleOptions.Namespace
		if !Contains(backupNamespaces, clusterAppsContextNamespace) {
			err := fmt.Errorf("the namespace (appCtx) [%s] provided to the ValidateBackup, is not present in the backup [%s]", clusterAppsContextNamespace, backupName)
			errors = append(errors, err)
		}

		namespacedBackedUpVolumesFound := make(map[*api.BackupInfo_Volume]bool)
		// collect the backup resources whose VOLUMES should be present in this context (namespace)
		namespacedBackedUpVolumes := make([]*api.BackupInfo_Volume, 0)
		for _, vol := range backedupVolumes {
			if vol.GetNamespace() == clusterAppsContextNamespace {
				if vol.Status.Status != api.BackupInfo_StatusInfo_Success /*Can this also be partialsuccess?*/ {
					backedUpVolumesFound[vol] = true
					err := fmt.Errorf("the status of the backedup volume [%s] was not Success. It was [%s] with reason [%s]", vol.Name, vol.Status.Status, vol.Status.Reason)
					errors = append(errors, err)
				}
				namespacedBackedUpVolumes = append(namespacedBackedUpVolumes, vol)
				namespacedBackedUpVolumesFound[vol] = false
			}
		}

		// Collect all volumes belonging to a context
		log.InfoD("getting the volumes bounded to the PVCs in the context [%s]", clusterAppsContextNamespace)
		volumeMap := make(map[string]*volume.Volume)
		scheduledVolumes, err := Inst().S.GetVolumes(clusterAppsContext)
		if err != nil {
			err := fmt.Errorf("error in Inst().S.GetVolumes: [%s] in namespace (appCtx) [%s]", err, clusterAppsContextNamespace)
			errors = append(errors, err)
			continue
		}
		for _, scheduledVol := range scheduledVolumes {
			volumeMap[scheduledVol.ID] = scheduledVol
		}
		log.Infof("volumes bounded to the PVCs in the context [%s] are [%+v]", clusterAppsContextNamespace, scheduledVolumes)

		// Verify if volumes are present
	volloop:
		for _, spec := range clusterAppsContext.App.SpecList {
			// Obtaining the volume from the PVC
			pvcSpecObj, ok := spec.(*corev1.PersistentVolumeClaim)
			if !ok {
				continue volloop
			}

			sched, ok := Inst().S.(*k8s.K8s)
			if !ok {
				continue volloop
			}

			updatedSpec, err := sched.GetUpdatedSpec(pvcSpecObj)
			if err != nil {
				err := fmt.Errorf("unable to fetch updated version of PVC(name: [%s], namespace: [%s]) present in the context [%s]. Error: %v", pvcSpecObj.GetName(), pvcSpecObj.GetNamespace(), clusterAppsContextNamespace, err)
				errors = append(errors, err)
				continue volloop
			}

			pvcObj, ok := updatedSpec.(*corev1.PersistentVolumeClaim)
			if !ok {
				err := fmt.Errorf("unable to fetch updated version of PVC(name: [%s], namespace: [%s]) present in the context [%s]. Error: %v", pvcSpecObj.GetName(), pvcSpecObj.GetNamespace(), clusterAppsContextNamespace, err)
				errors = append(errors, err)
				continue volloop
			}

			scheduledVol, ok := volumeMap[pvcObj.Spec.VolumeName]
			if !ok {
				err := fmt.Errorf("unable to find the volume corresponding to PVC(name: [%s], namespace: [%s]) in the cluster corresponding to the PVC's context, which is [%s]", pvcSpecObj.GetName(), pvcSpecObj.GetNamespace(), clusterAppsContextNamespace)
				errors = append(errors, err)
				continue volloop
			}

			// Finding the volume in the backup
			for _, backedupVol := range namespacedBackedUpVolumes {
				if backedupVol.GetName() == scheduledVol.ID {
					namespacedBackedUpVolumesFound[backedupVol] = true
					backedUpVolumesFound[backedupVol] = true

					if backedupVol.Pvc != pvcObj.Name {
						err := fmt.Errorf("the PVC of the volume as per the backup [%s] is [%s], but the one found in the scheduled namesapce is [%s]", backedupVol.GetName(), backedupVol.Pvc, pvcObj.Name)
						errors = append(errors, err)
					}

					if backedupVol.DriverName != Inst().V.String() {
						err := fmt.Errorf("the Driver Name of the volume as per the backup [%s] is [%s], but the one found in the scheduled namesapce is [%s]", backedupVol.GetName(), backedupVol.DriverName, clusterAppsContext.ScheduleOptions.StorageProvisioner)
						errors = append(errors, err)
					}

					if backedupVol.StorageClass != *pvcObj.Spec.StorageClassName {
						err := fmt.Errorf("the Storage Class of the volume as per the backup [%s] is [%s], but the one found in the scheduled namesapce is [%s]", backedupVol.GetName(), backedupVol.StorageClass, *pvcObj.Spec.StorageClassName)
						errors = append(errors, err)
					}

					continue volloop
				}
			}

			// The following error means that something WAS not backed up, OR it wasn't supposed to be backed up, and we forgot to exclude the check.
			err = fmt.Errorf("the volume [%s] corresponding to PVC(name: [%s], namespace: [%s]) was present in the cluster corresponding to the PVC's context, but not in the backup [%s]", pvcObj.Spec.VolumeName, pvcObj.GetName(), pvcObj.GetNamespace(), backupName)
			errors = append(errors, err)
		}

		for vol, found := range namespacedBackedUpVolumesFound {
			if !found {
				err := fmt.Errorf("volume (name: [%s], namespace: [%s]) in backup [%s], doesn't have a corresponding PVC spec in the context [%v]", vol.GetName(), vol.GetNamespace(), backupName, clusterAppsContextNamespace)
				errors = append(errors, err)
			} else {
				log.Infof("volume (name: [%s], namespace: [%s]) in backup [%s] has a PVC spec in the context [%s]", vol.GetName(), vol.GetNamespace(), backupName, clusterAppsContextNamespace)
			}
		}
	}

	for vol, found := range backedUpVolumesFound {
		if !found {
			err := fmt.Errorf("volume (name: [%s], namespace: [%s]) in backup [%s], doesn't have a corresponding PVC spec in ANY provided context", vol.GetName(), vol.GetNamespace(), backupName)
			errors = append(errors, err)
		}
	}

	return
}

// GetBackupCtxsFromScheduledCtxs clones and returns the scheduled contexts
// * after filtering its `spec`s to only include the resources that are in the backup.
// *
// * Returns:
// * backupclusterAppsContexts: the filtered context (backup)
// * ScheduledCtxNotFoundInBackupErrors: these are errors that are generated when namespaces and/or specObjs in the scheduled context are not found in the backup
// * BackupNotFoundInAnyScheduledCtxErrors: these are errors that are generated when namespaces and/or resources in the backup context are not found in the scheduled contextx
// * otherErrors: all the other kinds of errors; Don't ignore these are they're serious errors
// *
// * NOTES:
// * - make sure to include Contexts of *all* namespaces which are supposed to contain the backup objects
// * - The errors returned are all tolerable errors that have been caught until an intolerable error was encountered
func GetBackupCtxsFromScheduledCtxs(backupInspectResponse *api.BackupInspectResponse, scheduledAppContexts []*scheduler.Context) (backupclusterAppsContexts []*scheduler.Context, ScheduledCtxNotFoundInBackupErrors, BackupNotFoundInAnyScheduledCtxErrors, otherErrors []error) {
	backupName := backupInspectResponse.Backup.Name
	resourceInfos := backupInspectResponse.Backup.Resources
	backupNamesspaces := backupInspectResponse.GetBackup().GetNamespaces()

	log.InfoD("GetBackupCtxsFromScheduledCtxs: Getting the backup objects (specs) from contexts, for backup [%s]", backupName)

	// Verifying if appCtxs for all namespaces in backup
	log.InfoD("Verifying if scheduledAppContexts provided to ValidateBackup correspond to all namespaces in backup [%s]", backupName)
	availableNamespaces := make([]string, 0)
	unavailableNamespaces := make([]string, 0)
	for _, clusterAppsContext := range scheduledAppContexts {
		availableNamespaces = append(availableNamespaces, clusterAppsContext.ScheduleOptions.Namespace)
	}

	namespacesAvailable := true
	for _, namespace := range backupNamesspaces {
		if !Contains(availableNamespaces, namespace) {
			namespacesAvailable = false
			unavailableNamespaces = append(unavailableNamespaces, namespace)
		}
	}
	if !namespacesAvailable {
		err := fmt.Errorf("the namespaces (appCtxs) [%v] provided to the GetBackupCtxsFromScheduledCtxs, do not contain the namespaces [%v] present in the backup [%s]. They are required for Validation", availableNamespaces, unavailableNamespaces, backupName)
		BackupNotFoundInAnyScheduledCtxErrors = append(BackupNotFoundInAnyScheduledCtxErrors, err)
	}

	nonNSResourceInfoBackupObjsFound := make(map[*api.ResourceInfo]bool)

	nonNSResourceInfoBackupObjs := make([]*api.ResourceInfo, 0)
	for _, resource := range resourceInfos {
		if resource.GetNamespace() == "" && resource.GetKind() != "PersistentVolume" /*we don't have specs of PVs*/ {
			nonNSResourceInfoBackupObjs = append(nonNSResourceInfoBackupObjs, resource)
			nonNSResourceInfoBackupObjsFound[resource] = false
		}
	}

	// filter stage: for each clusterAppsContext (namespace), we create the corresponding BackupSpecObject
	for _, clusterAppsContext := range scheduledAppContexts {

		clusterAppsContextNamespace := clusterAppsContext.ScheduleOptions.Namespace
		if !Contains(backupNamesspaces, clusterAppsContextNamespace) {
			err := fmt.Errorf("the namespace (appCtx) [%s] provided to the ValidateBackup, is not present in the backup [%s]", clusterAppsContextNamespace, backupName)
			ScheduledCtxNotFoundInBackupErrors = append(ScheduledCtxNotFoundInBackupErrors, err)
		}

		resourceInfoBackupObjsFound := make(map[*api.ResourceInfo]bool)
		// collect the backup resources whose specs should be present in this context (namespace)
		resourceInfoBackupObjs := make([]*api.ResourceInfo, 0)
		for _, resource := range resourceInfos {
			if resource.GetNamespace() == clusterAppsContextNamespace {
				resourceInfoBackupObjs = append(resourceInfoBackupObjs, resource)
				resourceInfoBackupObjsFound[resource] = false
			}
		}

		// filter the specs to only keep the backup resources' specs, in this namespace
		var specObjects []interface{} = make([]interface{}, 0)
	specloop:
		for _, spec := range clusterAppsContext.App.SpecList {
			name, kind, ns, err := GetSpecNameKindNamepace(spec)
			if err != nil {
				err := fmt.Errorf("error in GetSpecNameKindNamepace: [%s] in namespace (appCtx) [%s], spec: [%+v]", err, clusterAppsContextNamespace, spec)
				otherErrors = append(otherErrors, err)
				continue specloop
			}

			if name != "" && kind != "" {

				if kind == "StorageClass" || kind == "VolumeSnapshot" {
					// we don't backup "StorageClass"s and "VolumeSnapshot"s
					continue specloop
				}

				// this is a non-namespaced resource
				if ns == "" {
					for _, nonNSBackupObj := range nonNSResourceInfoBackupObjs {
						if name == nonNSBackupObj.GetName() && kind == nonNSBackupObj.GetKind() {
							clone := spec
							specObjects = append(specObjects, clone)
							nonNSResourceInfoBackupObjsFound[nonNSBackupObj] = true

							continue specloop
						}
					}

					// The following error means that something WAS not backed up, OR it wasn't supposed to be backed up, and we forgot to exclude the check.
					err := fmt.Errorf("the non-namespaced spec (name: [%s], kind: [%s]) found in the clusterAppsContext [%s], is not in the backup [%s]", name, kind, clusterAppsContextNamespace, backupName)
					ScheduledCtxNotFoundInBackupErrors = append(ScheduledCtxNotFoundInBackupErrors, err)
					continue specloop
				} else {
					for _, backupObj := range resourceInfoBackupObjs {
						if name == backupObj.GetName() && kind == backupObj.GetKind() {
							clone := spec
							specObjects = append(specObjects, clone)
							resourceInfoBackupObjsFound[backupObj] = true

							continue specloop
						}
					}

					// The following error means that something WAS not backed up, OR it wasn't supposed to be backed up, and we forgot to exclude the check.
					err := fmt.Errorf("the spec (name: [%s], kind: [%s], namespace: [%s]) found in the clusterAppsContext [%s], is not in the backup [%s]", name, kind, ns, clusterAppsContextNamespace, backupName)
					ScheduledCtxNotFoundInBackupErrors = append(ScheduledCtxNotFoundInBackupErrors, err)

					continue specloop
				}

			} else {
				err := fmt.Errorf("error: GetSpecNameKindNamepace returned values with Spec Name: [%s], Kind: [%s], Namespace: [%s], in local Context (NS): [%s], where some of the values are empty, so this spec will be ignored", name, kind, ns, clusterAppsContextNamespace)
				otherErrors = append(otherErrors, err)

				continue specloop
			}
		}

		// Duplicate the object
		backupAppContext := *clusterAppsContext
		// Duplicate the object
		app := *clusterAppsContext.App

		app.SpecList = specObjects
		backupAppContext.App = &app
		backupclusterAppsContexts = append(backupclusterAppsContexts, &backupAppContext)

		for res, found := range resourceInfoBackupObjsFound {
			if !found {
				err := fmt.Errorf("resource(name: [%s], kind: [%s], namespace: [%s]) in backup [%s], doesn't have a corresponding spec in the context [%v]", res.GetName(), res.GetKind(), res.GetNamespace(), backupName, clusterAppsContextNamespace)
				BackupNotFoundInAnyScheduledCtxErrors = append(BackupNotFoundInAnyScheduledCtxErrors, err)
			} else {
				log.Infof("resource(name: [%s], kind: [%s], namespace: [%s]) in backup [%s] has a spec in the context [%s]", res.GetName(), res.GetKind(), res.GetNamespace(), backupName, clusterAppsContextNamespace)
			}
		}
	}

	for res, found := range nonNSResourceInfoBackupObjsFound {
		if !found {
			err := fmt.Errorf("non NS resource(name: [%s], kind: [%s]) in backup [%s], doesn't have a corresponding spec any of the contexts [%v]", res.GetName(), res.GetKind(), backupName, availableNamespaces)
			BackupNotFoundInAnyScheduledCtxErrors = append(BackupNotFoundInAnyScheduledCtxErrors, err)
		} else {
			// TODO: make Infof after testing with elastic-search-CRD-webhook
			log.Errorf("non NS resource(name: [%s], kind: [%s]) in backup [%s] has a spec in scheduledAppContexts", res.GetName(), res.GetKind(), backupName)
		}
	}

	return
}

// restoreSuccessCheck inspects restore task to check for status being "success". NOTE: If the status is different, it retries every `retryInterval` for `retryDuration` before returning `err`
func restoreSuccessCheck(restoreName string, orgID string, retryDuration time.Duration, retryInterval time.Duration, ctx context.Context) error {
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
	if err != nil {
		return err
	}
	return nil
}

// restoreSuccessWithReplacePolicy inspects restore task status as per ReplacePolicy_Type
func restoreSuccessWithReplacePolicy(restoreName string, orgID string, retryDuration time.Duration, retryInterval time.Duration, ctx context.Context, replacePolicy ReplacePolicy_Type) error {
	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}
	var statusesExpected api.RestoreInfo_StatusInfo_Status
	if replacePolicy == ReplacePolicy_Delete {
		statusesExpected = api.RestoreInfo_StatusInfo_Success
	} else if replacePolicy == ReplacePolicy_Retain {
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

// ValidateRestore returns a clone of the contexts (with backup objects) *after* converting the contexts to point to restored objects (and after validating those objects)
func ValidateRestore(ctx context.Context, restoreName string, orgID string, scheduledAppContexts []*scheduler.Context, namespaceMapping map[string]string, storageClassMapping map[string]string) error {
	backupDriver := Inst().Backup
	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}

	restoreInspectResponse, err := backupDriver.InspectRestore(ctx, restoreInspectRequest)
	if err != nil {
		return err
	}

	restoreStatus := restoreInspectResponse.GetRestore().GetStatus().Status
	if restoreStatus != api.RestoreInfo_StatusInfo_Success &&
		restoreStatus != api.RestoreInfo_StatusInfo_PartialSuccess {
		restoreStatusReason := restoreInspectResponse.GetRestore().GetStatus().Reason
		return fmt.Errorf("ValidateRestore requires restore [%s] to have a status of Success or PartialSuccess, but found [%s] with reason [%s]", restoreName, restoreStatus, restoreStatusReason)
	}

	restoredVolumesMap := make(map[string]*api.RestoreInfo_Volume, 0)
	for _, vol := range restoreInspectResponse.Restore.Volumes {
		restoredVolumesMap[vol.Pvc] = vol
	}

	backupName := restoreInspectResponse.Restore.Backup
	log.InfoD("Obtaining backup info for backup [%s] corresponding to restore [%s]", backupName, restoreName)
	backupUid, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		err := fmt.Errorf("GetBackupUID Err: %v", err)
		log.Error(err)
	} else {
		backupInspectRequest := &api.BackupInspectRequest{
			Name:  backupName,
			Uid:   backupUid,
			OrgId: orgID,
		}
		backupInspectResponse, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
		if err != nil {
			err := fmt.Errorf("InspectBackup Err: %v", err)
			log.Error(err)
		} else {
			log.Info("verifying if all backed up resources have been restored (via API)")

			if restoreInspectResponse.Restore.ResourceCount != backupInspectResponse.Backup.ResourceCount {
				err := fmt.Errorf("the resource count in backup [%s] and corresponding restore [%s] are not the same", backupName, restoreName)
				log.Error(err)
			}

			restoredResources := make([]*api.RestoreInfo_RestoredResource, 0)
			for _, res := range restoreInspectResponse.Restore.Resources {
				if res.Kind != "PersistentVolume" {
					restoredResources = append(restoredResources, res)
				}
			}

			// check if all backed up stuff has been restored (as per what px-backup says it restored)
		backupResloop:
			for _, backedupResource := range backupInspectResponse.Backup.Resources {
				if backedupResource.Kind != "PersistentVolume" {
					for _, restoredResource := range restoredResources {
						if backedupResource.Name == restoredResource.Name &&
							backedupResource.Kind == restoredResource.Name &&
							backedupResource.Namespace == restoredResource.Namespace &&
							backedupResource.Group == restoredResource.Group &&
							backedupResource.Version == restoredResource.Version {
							//TODO: infof
							log.InfoD("found the resource (name: %s, GVK: %s,%s,%s) in backup [%s] corresponding to the restore [%s]", backedupResource.Name, backedupResource.Group, backedupResource.Version, backedupResource.Kind, backupName, restoreName)
							continue backupResloop
						}
					}
					err := fmt.Errorf("the resource (name: %s, GVK: %s,%s,%s) in backup [%s] is missing from the corresponding restore [%s]", backedupResource.Name, backedupResource.Group, backedupResource.Version, backedupResource.Kind, backupName, restoreName)
					log.Error(err)
				}
			}

			// check if all backed up volumes (PVC) has been restored (as per what px-backup says it restored)
			for _, backedupVolume := range backupInspectResponse.Backup.Volumes {
				if _, ok := restoredVolumesMap[backedupVolume.Pvc]; !ok {
					err := fmt.Errorf("the volume having PVC [%s] is missing in restore [%s] corresponding to backup [%s]", backedupVolume.Pvc, restoreName, backupName)
					log.Error(err)
				}
			}
		}
	}

	// namespace matching

	// check for success/retai status, and if yes "Get" then and verify presence in restored namesapce. OR use below specs to directly verify presence.

	restoredAppContexts := make([]*scheduler.Context, 0)
	for _, backedupAppContext := range scheduledAppContexts {
		restoredAppContext, err := GetRestoreCtxFromBackupCtx(backedupAppContext, namespaceMapping, storageClassMapping)
		if err != nil {
			return fmt.Errorf("GetRestoreCtxsFromBackupCtxs Err: %v", err)
		}
		restoredAppContexts = append(restoredAppContexts, restoredAppContext)
	}

	// verifying of existence of
	for _, restoredAppContext := range restoredAppContexts {
		log.InfoD("getting the volumes bounded to the PVCs in the restored context [%s]", restoredAppContext)
		volumeMap := make(map[string]*volume.Volume)
		restoredAppsContextNamespace := restoredAppContext.ScheduleOptions.Namespace
		scheduledVolumes, err := Inst().S.GetVolumes(restoredAppContext)
		if err != nil {
			err := fmt.Errorf("error in Inst().S.GetVolumes: [%s] in namespace (appCtx) [%s]", err, restoredAppsContextNamespace)
			log.Error(err)
			continue
		}
		for _, scheduledVol := range scheduledVolumes {
			volumeMap[scheduledVol.ID] = scheduledVol
		}
		log.Infof("volumes bounded to the PVCs in the context [%s] are [%+v]", clusterAppsContextNamespace, scheduledVolumes)
	}

	// check if volume status are all success
	for pvc, vol := range restoredVolumesMap {

	}

	// this validates volumes too
	ValidateApplications(restoredAppContexts)

	return nil
}

// GetRestoreCtxFromBackupCtx uses the contexts to create and return clones which refer to restored specs (along with conversion of their specs)
// NOTE: To be used after switching context to the required destination cluster where restore was performed
func GetRestoreCtxFromBackupCtx(backedupAppContext *scheduler.Context, namespaceMapping map[string]string, storageClassMapping map[string]string) (*scheduler.Context, error) {
	log.InfoD("Getting Restore Context from Backup Context")

	restoreAppContext := *backedupAppContext

	// TODO: remove workaround in future.
	allStorageClassMappingsPresent := true

	specObjects := make([]interface{}, 0)
	for _, appSpecOrig := range backedupAppContext.App.SpecList {
		appSpec, err := CloneSpec(appSpecOrig) //clone spec to create "restore" specs
		if err != nil {
			log.Errorf("Failed to clone spec: '%v'. Err: %v", appSpecOrig, err)
			continue
		}
		err = TransformToRestoredSpec(appSpec, storageClassMapping)
		if err != nil {
			log.Errorf("Failed to TransformToRestoredSpec for %v, with sc map %s. Err: %v", appSpec, storageClassMapping, err)
			continue
		}
		err = UpdateNamespace(appSpec, namespaceMapping)
		if err != nil {
			log.Errorf("Failed to Update the namespace for %v, with ns map %s. Err: %v", appSpec, namespaceMapping, err)
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

	app := *backedupAppContext.App
	app.SpecList = specObjects
	restoreAppContext.App = &app

	// we're having to do this as we're under the assumption that `ScheduleOptions.Namespace` will always contain the namespace of the scheduled app
	options := CreateScheduleOptions()
	if namespace, ok := namespaceMapping[backedupAppContext.ScheduleOptions.Namespace]; ok {
		options.Namespace = namespace
	} else {
		options.Namespace = backedupAppContext.ScheduleOptions.Namespace
	}
	restoreAppContext.ScheduleOptions = options

	// TODO: remove workaround in future.
	if !allStorageClassMappingsPresent {
		restoreAppContext.SkipVolumeValidation = true
	}

	return &restoreAppContext, nil
}

// CloneSpec clones a given spec and returns it. It returns an error if the object (spec) provided is not supported by this function
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
func IsBackupLocationPresent(bkpLocation string, ctx context.Context, orgID string) (bool, error) {
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

// IsCloudCredPresent checks whether the Cloud Cred is present or not
func IsCloudCredPresent(cloudCredName string, ctx context.Context, orgID string) (bool, error) {
	cloudCredEnumerateRequest := &api.CloudCredentialEnumerateRequest{
		OrgId:          orgID,
		IncludeSecrets: false,
	}
	cloudCredObjs, err := Inst().Backup.EnumerateCloudCredential(ctx, cloudCredEnumerateRequest)
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

// CreateCustomRestoreWithPVCs function can be used to deploy custom deployment with it's PVCs. It cannot be used for any other resource type.
func CreateCustomRestoreWithPVCs(restoreName string, backupName string, namespaceMapping map[string]string, clusterName string,
	orgID string, ctx context.Context, storageClassMapping map[string]string, namespace string) (deploymentName string, err error) {

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
	err = restoreSuccessCheck(restoreName, orgID, maxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return "", fmt.Errorf("fail to create restore %v with error %v", restoreName, err)
	}
	return deployment.Name, nil
}

// GetOrdinalScheduleBackupName returns the name of the schedule backup at the specified ordinal position for the given schedule
func GetOrdinalScheduleBackupName(ctx context.Context, scheduleName string, ordinal int, orgID string) (string, error) {
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
func GetFirstScheduleBackupName(ctx context.Context, scheduleName string, orgID string) (string, error) {
	allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	if len(allScheduleBackupNames) == 0 {
		return "", fmt.Errorf("no backups found for schedule %s", scheduleName)
	}
	return allScheduleBackupNames[0], nil
}

// GetLatestScheduleBackupName returns the name of the latest schedule backup for the given schedule
func GetLatestScheduleBackupName(ctx context.Context, scheduleName string, orgID string) (string, error) {
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
func GetOrdinalScheduleBackupUID(ctx context.Context, scheduleName string, ordinal int, orgID string) (string, error) {
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
func GetFirstScheduleBackupUID(ctx context.Context, scheduleName string, orgID string) (string, error) {
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
func GetLatestScheduleBackupUID(ctx context.Context, scheduleName string, orgID string) (string, error) {
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

func DeleteBackupAndWait(backupName string, ctx context.Context) error {
	backupDriver := Inst().Backup
	backupEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID,
	}

	backupDeletionSuccessCheck := func() (interface{}, bool, error) {
		currentBackups, err := backupDriver.EnumerateBackup(ctx, backupEnumerateReq)
		if err != nil {
			return "", true, err
		}
		for _, backupObject := range currentBackups.GetBackups() {
			if backupObject.Name == backupName {
				return "", true, fmt.Errorf("backupObject [%s] is not yet deleted", backupObject.Name)
			}
		}
		return "", false, nil
	}
	_, err := task.DoRetryWithTimeout(backupDeletionSuccessCheck, backupDeleteTimeout, backupDeleteRetryTime)
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

// UpgradePxBackup will perform the upgrade tasks for Px Backup to the version passed as string
// Eg: versionToUpgrade := "2.4.0"
func UpgradePxBackup(versionToUpgrade string) error {
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
	statefulSet, err := apps.Instance().GetStatefulSet(mongodbStatefulset, pxBackupNamespace)
	if err != nil {
		return err
	}
	pvcs, err := apps.Instance().GetPVCsForStatefulSet(statefulSet)
	if err != nil {
		return err
	}
	storageClassName := pvcs.Items[0].Spec.StorageClassName

	// Get the tarball required for helm upgrade
	cmd = fmt.Sprintf("curl -O  https://raw.githubusercontent.com/portworx/helm/%s/stable/px-central-%s.tgz", latestPxBackupHelmBranch, versionToUpgrade)
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
	cmd = fmt.Sprintf("helm upgrade px-central px-central-%s.tgz --namespace %s --version %s --set persistentStorage.enabled=true,persistentStorage.storageClassName=\"%s\",pxbackup.enabled=true",
		versionToUpgrade, pxBackupNamespace, versionToUpgrade, *storageClassName)
	log.Infof("helm command: %v ", cmd)
	output, _, err = osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("upgrade failed with error: %v", err)
	}
	log.Infof("Terminal output: %s", output)

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
	allPods, err := core.Instance().GetPods(pxBackupNamespace, nil)
	for _, pod := range allPods.Items {
		if strings.Contains(pod.Name, pxCentralPostInstallHookJobName) ||
			strings.Contains(pod.Name, quickMaintenancePod) ||
			strings.Contains(pod.Name, fullMaintenancePod) {
			continue
		}
		log.Infof("Checking status for pod - %s", pod.GetName())
		err = core.Instance().ValidatePod(&pod, 5*time.Minute, 30*time.Second)
		if err != nil {
			return err
		}
	}
	return nil
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
func upgradeStorkVersion(storkImageToUpgrade string) error {
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

	storkImageVersionToUpgradeStr := strings.Split(storkImageToUpgrade, ":")[len(strings.Split(storkImageToUpgrade, ":"))-1]
	storkImageVersionToUpgrade, err := version.NewSemver(storkImageVersionToUpgradeStr)
	if err != nil {
		return err
	}

	log.Infof("Current stork version : %s", currentStorkVersion)
	log.Infof("Upgrading stork version to : %s", storkImageVersionToUpgrade)

	if currentStorkVersion.GreaterThanOrEqual(storkImageVersionToUpgrade) {
		return fmt.Errorf("Cannot upgrade stork version from %s to %s as the current version is higher than the provided version", currentStorkVersion, storkImageVersionToUpgrade)
	}

	isOpBased, _ := Inst().V.IsOperatorBasedInstall()
	if isOpBased {
		log.Infof("Operator based Portworx deployment, Upgrading stork via StorageCluster")
		storageSpec, err := Inst().V.GetDriver()
		if err != nil {
			return err
		}
		storageSpec.Spec.Stork.Image = storkImageToUpgrade
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

		storkDeployment.Spec.Template.Spec.Containers[0].Image = storkImageToUpgrade
		_, err = apps.Instance().UpdateDeployment(storkDeployment)
		if err != nil {
			return err
		}
	}

	// validate stork pods after upgrade
	updatedStorkDeployment, err := apps.Instance().GetDeployment(storkDeploymentName, storkDeploymentNamespace)
	if err != nil {
		return err
	}
	err = apps.Instance().ValidateDeployment(updatedStorkDeployment, k8s.DefaultTimeout, k8s.DefaultRetryInterval)
	if err != nil {
		return err
	}

	postUpgradeStorkImageVersionStr, err := getStorkImageVersion()
	if err != nil {
		return err
	}

	if !strings.EqualFold(postUpgradeStorkImageVersionStr, storkImageVersionToUpgradeStr) {
		return fmt.Errorf("expected version after upgrade was %s but got %s", storkImageVersionToUpgradeStr, postUpgradeStorkImageVersionStr)
	}

	log.Infof("Succesfully upgraded stork version from %v to %v", currentStorkImageStr, postUpgradeStorkImageVersionStr)
	return nil
}

// CreateBackupWithNamespaceLabel creates a backup with Namespace label
func CreateBackupWithNamespaceLabel(backupName string, clusterName string, bkpLocation string, bkpLocationUID string,
	labelSelectors map[string]string, orgID string, uid string, preRuleName string, preRuleUid string, postRuleName string,
	postRuleUid string, namespaceLabel string, ctx context.Context) error {

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
	_, err := backupDriver.CreateBackup(ctx, bkpCreateRequest)
	if err != nil {
		return err
	}
	err = backupSuccessCheck(backupName, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Successfully created backup [%s] with namespace label [%s]", backupName, namespaceLabel)
	return nil
}

// CreateScheduleBackupWithNamespaceLabel creates a schedule backup with namespace label
func CreateScheduleBackupWithNamespaceLabel(scheduleName string, clusterName string, bkpLocation string, bkpLocationUID string,
	labelSelectors map[string]string, orgID string, preRuleName string, preRuleUid string, postRuleName string,
	postRuleUid string, namespaceLabel, schPolicyName string, schPolicyUID string, ctx context.Context) error {

	var firstScheduleBackupName string
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
	_, err := backupDriver.CreateBackupSchedule(ctx, bkpSchCreateRequest)
	if err != nil {
		return err
	}
	time.Sleep(1 * time.Minute)
	firstScheduleBackupName, err = GetFirstScheduleBackupName(ctx, scheduleName, orgID)
	if err != nil {
		return err
	}
	err = backupSuccessCheck(firstScheduleBackupName, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
	if err != nil {
		return err
	}
	log.Infof("Successfully created schedule backup [%s] with namespace label [%s]", firstScheduleBackupName, namespaceLabel)
	return nil
}

// CreateNamespaceLabelBackupWithoutCheck creates backup with namespace label filter without waiting for success
func CreateNamespaceLabelBackupWithoutCheck(backupName string, clusterName string, bkpLocation string, bkpLocationUID string,
	labelSelectors map[string]string, orgID string, uid string, preRuleName string, preRuleUid string, postRuleName string,
	postRuleUid string, namespaceLabel string, ctx context.Context) (*api.BackupInspectResponse, error) {

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
	_, err := backupDriver.CreateBackup(ctx, bkpCreateRequest)
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

// CreateNamespaceLabelScheduleBackupWithoutCheck creates a schedule backup with namespace label filter without waiting for success
func CreateNamespaceLabelScheduleBackupWithoutCheck(scheduleName string, clusterName string, bkpLocation string, bkpLocationUID string,
	labelSelectors map[string]string, orgID string, preRuleName string, preRuleUid string, postRuleName string, postRuleUid string,
	schPolicyName string, schPolicyUID string, namespaceLabel string, ctx context.Context) (*api.BackupScheduleInspectResponse, error) {
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
	_, err := backupDriver.CreateBackupSchedule(ctx, bkpSchCreateRequest)
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

// suspendBackupSchedule will suspend backup schedule
func suspendBackupSchedule(backupScheduleName, schPolicyName, OrgID string, ctx context.Context) error {
	backupDriver := Inst().Backup
	backupScheduleUID, err := GetScheduleUID(backupScheduleName, orgID, ctx)
	if err != nil {
		return err
	}
	schPolicyUID, err := Inst().Backup.GetSchedulePolicyUid(orgID, ctx, schPolicyName)
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
func resumeBackupSchedule(backupScheduleName, schPolicyName, OrgID string, ctx context.Context) error {
	backupDriver := Inst().Backup
	backupScheduleUID, err := GetScheduleUID(backupScheduleName, orgID, ctx)
	if err != nil {
		return err
	}
	schPolicyUID, err := Inst().Backup.GetSchedulePolicyUid(orgID, ctx, schPolicyName)
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
func NamespaceLabelBackupSuccessCheck(backupName string, ctx context.Context, listOfLabelledNamespaces []string, namespaceLabel string) error {
	backupDriver := Inst().Backup
	log.Infof("Getting the Uid of backup %v", backupName)
	backupUid, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUid,
		OrgId: orgID,
	}
	resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return err
	}
	namespaceList := resp.GetBackup().GetNamespaces()
	log.Infof("The list of namespaces backed up are %v", namespaceList)
	if !AreSlicesEqual(namespaceList, listOfLabelledNamespaces) {
		return fmt.Errorf("list of namespaces backed up are %v which is not same as expected %v", namespaceList, listOfLabelledNamespaces)
	}
	backupLabels := resp.GetBackup().GetNsLabelSelectors()
	log.Infof("The list of labels applied to backup are %v", backupLabels)
	expectedLabels := strings.Split(namespaceLabel, ",")
	actualLabels := strings.Split(backupLabels, ",")
	if !AreSlicesEqual(expectedLabels, actualLabels) {
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
func VerifyLicenseConsumedCount(ctx context.Context, OrgId string, expectedLicenseConsumedCount int64) error {
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

// FetchNamespacesFromBackup fetches the namespace from backup
func FetchNamespacesFromBackup(ctx context.Context, backupName string, orgID string) ([]string, error) {
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

// GetNextScheduleBackupName returns the upcoming schedule backup when this function is called
func GetNextScheduleBackupName(scheduleName string, scheduleInterval time.Duration, ctx context.Context) (string, error) {
	var nextScheduleBackupName string
	allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(ctx, scheduleName, orgID)
	if err != nil {
		return "", err
	}
	currentScheduleBackupCount := len(allScheduleBackupNames)
	nextScheduleBackupOrdinal := currentScheduleBackupCount + 1
	checkOrdinalScheduleBackupCreation := func() (interface{}, bool, error) {
		ordinalScheduleBackupName, err := GetOrdinalScheduleBackupName(ctx, scheduleName, nextScheduleBackupOrdinal, orgID)
		if err != nil {
			return "", true, err
		}
		return ordinalScheduleBackupName, false, nil
	}
	log.InfoD("Waiting for the next schedule backup to be triggered")
	time.Sleep(scheduleInterval * time.Minute)
	nextScheduleBackup, err := task.DoRetryWithTimeout(checkOrdinalScheduleBackupCreation, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)

	log.InfoD("Next schedule backup name [%s]", nextScheduleBackup.(string))
	err = backupSuccessCheck(nextScheduleBackup.(string), orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
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
