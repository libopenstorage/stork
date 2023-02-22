package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

const (
	storkDeploymentNamespace                  = "kube-system"
	clusterName                               = "tp-cluster"
	restoreNamePrefix                         = "tp-restore"
	destinationClusterName                    = "destination-cluster"
	appReadinessTimeout                       = 10 * time.Minute
	taskNamePrefix                            = "pxbackuptask"
	orgID                                     = "default"
	usersToBeCreated                          = "USERS_TO_CREATE"
	groupsToBeCreated                         = "GROUPS_TO_CREATE"
	maxUsersInGroup                           = "MAX_USERS_IN_GROUP"
	maxBackupsToBeCreated                     = "MAX_BACKUPS"
	maxWaitPeriodForBackupCompletionInMinutes = 20
	maxWaitPeriodForRestoreCompletionInMinute = 20
	globalAWSBucketPrefix                     = "global-aws"
	globalAzureBucketPrefix                   = "global-azure"
	globalGCPBucketPrefix                     = "global-gcp"
	globalAWSLockedBucketPrefix               = "global-aws-locked"
	globalAzureLockedBucketPrefix             = "global-azure-locked"
	globalGCPLockedBucketPrefix               = "global-gcp-locked"
	userName                                  = "testuser"
	firstName                                 = "firstName"
	lastName                                  = "lastName"
	password                                  = "Password1"
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
	return storkDeploymentNamespace
}

// CreateBackup creates backup
func CreateBackup(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, ctx context.Context) error {

	var bkpUid string
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
	backupSuccessCheck := func() (interface{}, bool, error) {
		bkpUid, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
		if err != nil {
			return "", true, err
		}
		backupInspectRequest := &api.BackupInspectRequest{
			Name:  backupName,
			Uid:   bkpUid,
			OrgId: orgID,
		}
		resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
		if err != nil {
			return "", true, err
		}
		actual := resp.GetBackup().GetStatus().Status
		expected := api.BackupInfo_StatusInfo_Success
		if actual != expected {
			return "", true, fmt.Errorf("backup status for [%s] expected was [%s] but got [%s]", backupName, expected, actual)
		}
		return "", false, nil
	}

	_, err = task.DoRetryWithTimeout(backupSuccessCheck, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
	if err != nil {
		return err
	}
	bkpUid, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   bkpUid,
		OrgId: orgID,
	}
	_, err = backupDriver.InspectBackup(ctx, backupInspectRequest)
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

	var bkpUid string
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
	backupSuccessCheck := func() (interface{}, bool, error) {
		bkpUid, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
		if err != nil {
			return "", true, err
		}
		backupInspectRequest := &api.BackupInspectRequest{
			Name:  backupName,
			Uid:   bkpUid,
			OrgId: orgID,
		}
		resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
		if err != nil {
			return "", true, err
		}
		actual := resp.GetBackup().GetStatus().Status
		expected := api.BackupInfo_StatusInfo_Success
		if actual != expected {
			return "", true, fmt.Errorf("backup status for [%s] expected was [%s] but got [%s]", backupName, expected, actual)
		}
		return "", false, nil
	}

	_, err = task.DoRetryWithTimeout(backupSuccessCheck, 10*time.Minute, 30*time.Second)
	if err != nil {
		return err
	}
	bkpUid, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   bkpUid,
		OrgId: orgID,
	}
	_, err = backupDriver.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return err
	}
	log.Infof("Backup [%s] created successfully", backupName)
	return nil
}

// CreateScheduleBackup creates a scheduled backup
func CreateScheduleBackup(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, schPolicyName string, schPolicyUID string, ctx context.Context) (string, error) {
	var bkpUid string
	backupDriver := Inst().Backup
	bkpSchCreateRequest := &api.BackupScheduleCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  backupName,
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
		return "", err
	}
	time.Sleep(1 * time.Minute)
	backupSuccessCheck := func() (interface{}, bool, error) {
		bkpUid, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
		if err != nil {
			return "", true, err
		}
		backupSchInspectRequest := &api.BackupScheduleInspectRequest{
			Name:  backupName,
			Uid:   bkpUid,
			OrgId: orgID,
		}

		resp, err := backupDriver.InspectBackupSchedule(ctx, backupSchInspectRequest)
		if err != nil {
			return "", true, fmt.Errorf("error in fetching inspect backup schedule response for %v", backupName)
		}
		expected := api.BackupScheduleInfo_StatusInfo_Success
		actual := resp.GetBackupSchedule().GetBackupStatus()["interval"].GetStatus()[0].GetStatus()
		if actual != expected {
			return "", true, fmt.Errorf("backup status for [%s] expected was [%s] but got [%s]", backupName, expected, actual)
		}
		return fmt.Sprintf("actual [%v] is equal to expected [%v] string", actual, expected), false, nil
	}

	_, err = task.DoRetryWithTimeout(backupSuccessCheck, 10*time.Minute, 30*time.Second)
	if err != nil {
		return "", err
	}
	bkpUid, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return "", err
	}
	backupSchInspectRequest := &api.BackupScheduleInspectRequest{
		Name:  backupName,
		Uid:   bkpUid,
		OrgId: orgID,
	}
	resp, err := backupDriver.InspectBackupSchedule(ctx, backupSchInspectRequest)
	log.InfoD("InspectBackupSchedule response - [%v]", resp)
	if err != nil {
		return "", err
	}
	schBackupName := resp.GetBackupSchedule().GetBackupStatus()["interval"].GetStatus()[0].GetBackupName()
	log.InfoD("InspectBackupSchedule response - [%v]", schBackupName)
	return schBackupName, nil
}

// CreateBackupWithoutCheck creates backup without waiting for success
func CreateBackupWithoutCheck(backupName string, clusterName string, bLocation string, bLocationUID string,
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
	return nil
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
	_, clusterUID := backupDriver.RegisterBackupCluster(orgID, SourceClusterName, "")

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

	_, err := backupDriver.ClusterUpdateBackupShare(ctx, clusterBackupShareUpdateRequest)
	return err
}

func GetAllBackupsForUser(username, password string) ([]string, error) {
	var bkp *api.BackupObject
	backupNames := make([]string, 0)
	backupDriver := Inst().Backup
	ctx, err := backup.GetNonAdminCtx(username, password)
	if err != nil {
		return nil, nil
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
	return backupNames, err
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
	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}
	restoreSuccessCheck := func() (interface{}, bool, error) {
		resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
		if err != nil {
			return "", true, err
		}
		restoreResponseStatus := resp.GetRestore().GetStatus()
		if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_PartialSuccess || restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Success {
			log.Infof("Restore status - %s", restoreResponseStatus)
			log.InfoD("Status of %s - [%s]",
				restoreName, restoreResponseStatus.GetStatus())
			return "", false, nil
		}
		return "", true, fmt.Errorf("expected status of %s - [%s] or [%s], but got [%s]",
			restoreName, api.RestoreInfo_StatusInfo_PartialSuccess.String(), api.RestoreInfo_StatusInfo_Success, restoreResponseStatus.GetStatus())
	}
	_, err = task.DoRetryWithTimeout(restoreSuccessCheck, 10*time.Minute, 30*time.Second)
	if err != nil {
		return err
	}
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
	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}
	restoreSuccessCheck := func() (interface{}, bool, error) {
		resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
		if err != nil {
			return "", false, err
		}
		restoreResponseStatus := resp.GetRestore().GetStatus()
		if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_PartialSuccess || restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Success {
			log.Infof("Restore status - %s", restoreResponseStatus)
			log.InfoD("Status of %s - [%s]",
				restoreName, restoreResponseStatus.GetStatus())
			return "", false, nil
		}
		return "", true, fmt.Errorf("expected status of %s - [%s] or [%s], but got [%s]",
			restoreName, api.RestoreInfo_StatusInfo_PartialSuccess.String(), api.RestoreInfo_StatusInfo_Success, restoreResponseStatus.GetStatus())
	}
	_, err = task.DoRetryWithTimeout(restoreSuccessCheck, 10*time.Minute, 30*time.Second)
	if err != nil {
		return err
	}
	return nil
}

// CreateRestoreWithoutCheck creates restore without waiting for completion
func CreateRestoreWithoutCheck(restoreName string, backupName string,
	namespaceMapping map[string]string, clusterName string, orgID string, ctx context.Context) error {

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
		return err
	}
	return nil
}

func getBackupUID(backupName, orgID string) (string, error) {
	backupDriver := Inst().Backup
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		return "", err
	}
	backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return "", err
	}
	return backupUID, nil
}

func getSizeOfMountPoint(podName string, namespace string, kubeConfigFile string) (int, error) {
	var number int
	ret, err := kubectlExec([]string{podName, "-n", namespace, "--kubeconfig=", kubeConfigFile, " -- /bin/df"})
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
	cmd := exec.Command("kubectl exec -it", arguments...)
	output, err := cmd.Output()
	log.Debugf("command output for '%s': %s", cmd.String(), string(output))
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
			err := backup.AddUser(userName, firstName, lastName, email, password)
			Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Creating user - %s", userName))
			users = append(users, userName)
		}(userName, firstName, lastName, email)
	}
	wg.Wait()
	return users
}

func DeleteCloudAccounts(backupLocationMap map[string]string, credName string, cloudCredUID string, ctx context.Context) {
	if len(backupLocationMap) != 0 {
		for backupLocationUID, bkpLocationName := range backupLocationMap {
			err := DeleteBackupLocation(bkpLocationName, backupLocationUID, orgID)
			Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup location %s", bkpLocationName))
		}
		time.Sleep(time.Minute * 3)
		err := DeleteCloudCredential(credName, orgID, cloudCredUID)
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
		ctxNonAdmin, err := backup.GetNonAdminCtx(users[i], "Password1")
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
	userCtx, err := backup.GetNonAdminCtx(user, "Password1")
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
		Inst().Dash.VerifyFatal(strings.Contains(err.Error(), "failed to retrieve backup location"), true, "Verifying backup restore is not possible")
		// Try to delete the backup with user having ViewOnlyAccess, and it should not pass
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
		Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Getting backup UID for- %s", backupName))
		// Delete backup to confirm that the user has ViewOnlyAccess and cannot delete backup
		_, err = DeleteBackup(backupName, backupUID, orgID, userCtx)
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
		ctxNonAdmin, err = backup.GetNonAdminCtx(users[i], "Password1")
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
	bkpUid, err := backupDriver.GetBackupUID(ctx, scheduleName, orgID)
	if err != nil {
		return "", fmt.Errorf("failed to get backup UID for %s, Err: %v", scheduleName, err)
	}
	backupSchInspectRequest := &api.BackupScheduleInspectRequest{
		Name:  scheduleName,
		Uid:   bkpUid,
		OrgId: orgID,
	}
	resp, err := backupDriver.InspectBackupSchedule(ctx, backupSchInspectRequest)
	if err != nil {
		return "", fmt.Errorf("failed to inspect backup [%s] with UID [%s], Err: %v", scheduleName, bkpUid, err)
	}
	scheduleUid := resp.GetBackupSchedule().GetUid()
	log.InfoD("Schedule Name - %s Schedule UID - %s", scheduleName, scheduleUid)

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
		log.Infof("The backup locations and their UID are %v", cloudCredentialMap)
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
func backupSuccessCheck(backupName string, orgID string, retryDuration int, retryInterval int, ctx context.Context) (bool, error) {
	var bkpUid string
	backupDriver := Inst().Backup
	if retryDuration == 0 {
		retryDuration = maxWaitPeriodForBackupCompletionInMinutes
	}
	if retryInterval == 0 {
		retryInterval = 30
	}
	backupSuccessCheck := func() (interface{}, bool, error) {
		bkpUid, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
		if err != nil {
			return "", false, err
		}
		backupInspectRequest := &api.BackupInspectRequest{
			Name:  backupName,
			Uid:   bkpUid,
			OrgId: orgID,
		}
		resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
		if err != nil {
			return "", false, err
		}
		actual := resp.GetBackup().GetStatus().Status
		expected := api.BackupInfo_StatusInfo_Success
		if actual != expected {
			return "", true, fmt.Errorf("backup status for [%s] expected was [%s] but got [%s]", backupName, expected, actual)
		}
		return "", false, nil
	}

	task.DoRetryWithTimeout(backupSuccessCheck, time.Duration(retryDuration)*time.Minute, time.Duration(retryInterval)*time.Second)
	bkpUid, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return false, err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   bkpUid,
		OrgId: orgID,
	}
	resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return false, err
	}
	backupStatus := (resp.GetBackup().GetStatus().Status == api.BackupInfo_StatusInfo_Success)
	log.Infof("Backup [%s] created successfully", backupName)
	return backupStatus, nil
}

// restoreSuccessCheck inspects restore task
func restoreSuccessCheck(restoreName string, orgID string, retryDuration int, retryInterval int, ctx context.Context) (bool, error) {
	if retryDuration == 0 {
		retryDuration = maxWaitPeriodForRestoreCompletionInMinute
	}
	if retryInterval == 0 {
		retryInterval = 30
	}
	backupDriver := Inst().Backup
	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}
	restoreSuccessCheck := func() (interface{}, bool, error) {
		resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
		if err != nil {
			return "", false, err
		}
		restoreResponseStatus := resp.GetRestore().GetStatus()
		if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_PartialSuccess || restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Success {
			log.Infof("Restore status for [%s] - %s", restoreName, restoreResponseStatus.GetStatus())
			return "", false, nil
		}
		return "", true, fmt.Errorf("expected status of %s - [%s] or [%s], but got [%s]",
			restoreName, api.RestoreInfo_StatusInfo_PartialSuccess.String(), api.RestoreInfo_StatusInfo_Success, restoreResponseStatus.GetStatus())
	}
	task.DoRetryWithTimeout(restoreSuccessCheck, time.Duration(retryDuration)*time.Minute, time.Duration(retryInterval)*time.Second)

	restoreInspectRequest = &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}

	resp, err := backupDriver.InspectRestore(ctx, restoreInspectRequest)
	if err != nil {
		return false, err
	}
	restoreStatus := (resp.GetRestore().GetStatus().Status == api.RestoreInfo_StatusInfo_PartialSuccess) || (resp.GetRestore().GetStatus().Status == api.RestoreInfo_StatusInfo_Success)
	log.Infof("[%s] restored successfully", restoreName)
	return restoreStatus, nil
}
