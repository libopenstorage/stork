package portworx

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	driverName            = "pxb"
	pxbRestPort           = 10001
	defaultPxbServicePort = 10002
	pxbServiceName        = "px-backup"
	schedulerDriverName   = "k8s"
	nodeDriverName        = "ssh"
	volumeDriverName      = "pxd"
	licFeatureName        = "BackupNodeCount"
	enumerateBatchSize    = 100
	backup_api_endpoint   = "BACKUP_API_ENDPOINT"
	post_install_hook_pod = "pxcentral-post-install-hook"
	quick_maintenance_pod = "quick-maintenance-repo"
	full_maintenance_pod  = "full-maintenance-repo"
	defaultTimeout        = 5 * time.Minute
)

type portworx struct {
	clusterManager         api.ClusterClient
	backupLocationManager  api.BackupLocationClient
	cloudCredentialManager api.CloudCredentialClient
	backupManager          api.BackupClient
	restoreManager         api.RestoreClient
	backupScheduleManager  api.BackupScheduleClient
	schedulePolicyManager  api.SchedulePolicyClient
	organizationManager    api.OrganizationClient
	licenseManager         api.LicenseClient
	healthManager          api.HealthClient
	ruleManager            api.RulesClient

	schedulerDriver scheduler.Driver
	nodeDriver      node.Driver
	volumeDriver    volume.Driver
	schedOps        schedops.Driver
	refreshEndpoint bool
	token           string
}

func (p *portworx) String() string {
	return driverName
}

func getKubernetesRestConfig(clusterObj *api.ClusterObject) (*rest.Config, error) {
	if clusterObj.GetKubeconfig() == "" {
		return nil, fmt.Errorf("empty cluster kubeconfig")
	}
	config, err := base64.StdEncoding.DecodeString(clusterObj.GetKubeconfig())
	if err != nil {
		return nil, fmt.Errorf("unable to decode account details %v", err)
	}

	client, err := clientcmd.RESTConfigFromKubeConfig(config)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// getKubernetesInstance - Get hanlder to k8s cluster.
func getKubernetesInstance(cluster *api.ClusterObject) (core.Ops, stork.Ops, error) {
	client, err := getKubernetesRestConfig(cluster)
	if err != nil {
		return nil, nil, err
	}

	storkInst, err := stork.NewForConfig(client)
	if err != nil {
		return nil, nil, fmt.Errorf("error initializing stork client instance: %v", err)
	}

	coreInst, err := core.NewForConfig(client)
	if err != nil {
		return nil, nil, fmt.Errorf("error initializing core client instance: %v", err)
	}

	// validate we are able to access k8s apis
	_, err = coreInst.GetVersion()
	if err != nil {
		return nil, nil, fmt.Errorf("error getting cluster version: %v", err)
	}
	return coreInst, storkInst, nil
}

func (p *portworx) Init(schedulerDriverName string, nodeDriverName string, volumeDriverName string, token string) error {
	var err error

	log.Infof("using portworx backup driver under scheduler: %v", schedulerDriverName)

	p.nodeDriver, err = node.Get(nodeDriverName)
	if err != nil {
		return err
	}
	p.token = token

	p.schedulerDriver, err = scheduler.Get(schedulerDriverName)
	if err != nil {
		return fmt.Errorf("Error getting scheduler driver %v: %v", schedulerDriverName, err)
	}

	p.volumeDriver, err = volume.Get(volumeDriverName)
	if err != nil {
		return fmt.Errorf("Error getting volume driver %v: %v", volumeDriverName, err)
	}

	if err = p.setDriver(pxbServiceName, backup.GetPxBackupNamespace()); err != nil {
		return fmt.Errorf("Error setting px-backup endpoint: %v", err)
	}

	return err

}

func (p *portworx) constructURL(ip string) string {
	return net.JoinHostPort(ip, strconv.Itoa(int(defaultPxbServicePort)))
}

func (p *portworx) testAndSetEndpoint(endpoint string) error {
	pxEndpoint := os.Getenv(backup_api_endpoint)
	// This condition is added for cases when torpedo is not running as a pod in the cluster
	// Since gRPC calls to backup pod would fail while running from a VM or local machine using ginkgo CLI
	// This condition will check if there is an Env variable set
	if pxEndpoint == " " || len(pxEndpoint) == 0 {
		pxEndpoint = p.constructURL(endpoint)
	}
	conn, err := grpc.Dial(pxEndpoint, grpc.WithInsecure())
	if err != nil {
		log.Errorf("unable to get grpc connection: %v", err)
		return err
	}
	p.healthManager = api.NewHealthClient(conn)
	_, err = p.healthManager.Status(context.Background(), &api.HealthStatusRequest{})
	if err != nil {
		log.Errorf("HealthManager API error: %v", err)
		return err
	}

	p.clusterManager = api.NewClusterClient(conn)
	p.backupLocationManager = api.NewBackupLocationClient(conn)
	p.cloudCredentialManager = api.NewCloudCredentialClient(conn)
	p.backupManager = api.NewBackupClient(conn)
	p.restoreManager = api.NewRestoreClient(conn)
	p.backupScheduleManager = api.NewBackupScheduleClient(conn)
	p.schedulePolicyManager = api.NewSchedulePolicyClient(conn)
	p.organizationManager = api.NewOrganizationClient(conn)
	p.licenseManager = api.NewLicenseClient(conn)
	p.ruleManager = api.NewRulesClient(conn)

	log.Infof("Using %v as endpoint for portworx backup driver", pxEndpoint)

	return err
}

func (p *portworx) GetServiceEndpoint(serviceName string, namespace string) (string, error) {
	svc, err := core.Instance().GetService(serviceName, namespace)
	if err == nil {
		return svc.Spec.ClusterIP, nil
	}
	return "", err
}

func (p *portworx) setDriver(serviceName string, namespace string) error {
	var err error
	var endpoint string

	endpoint, err = p.GetServiceEndpoint(serviceName, namespace)
	if err == nil && endpoint != "" {
		if err = p.testAndSetEndpoint(endpoint); err == nil {
			return nil
		}
	}
	return fmt.Errorf("failed to get endpoint for portworx backup driver: %v", err)
}

func (p *portworx) CreateOrganization(ctx context.Context, req *api.OrganizationCreateRequest) (*api.OrganizationCreateResponse, error) {
	return p.organizationManager.Create(ctx, req)
}

func (p *portworx) EnumerateOrganization(ctx context.Context) (*api.OrganizationEnumerateResponse, error) {
	return p.organizationManager.Enumerate(ctx, &api.OrganizationEnumerateRequest{})
}

func (p *portworx) CreateCloudCredential(ctx context.Context, req *api.CloudCredentialCreateRequest) (*api.CloudCredentialCreateResponse, error) {
	return p.cloudCredentialManager.Create(ctx, req)
}

func (p *portworx) UpdateCloudCredential(ctx context.Context, req *api.CloudCredentialUpdateRequest) (*api.CloudCredentialUpdateResponse, error) {
	return p.cloudCredentialManager.Update(ctx, req)
}

func (p *portworx) InspectCloudCredential(ctx context.Context, req *api.CloudCredentialInspectRequest) (*api.CloudCredentialInspectResponse, error) {
	return p.cloudCredentialManager.Inspect(ctx, req)
}

func (p *portworx) EnumerateCloudCredential(ctx context.Context, req *api.CloudCredentialEnumerateRequest) (*api.CloudCredentialEnumerateResponse, error) {
	return p.cloudCredentialManager.Enumerate(ctx, req)
}

func (p *portworx) DeleteCloudCredential(ctx context.Context, req *api.CloudCredentialDeleteRequest) (*api.CloudCredentialDeleteResponse, error) {
	return p.cloudCredentialManager.Delete(ctx, req)
}

func (p *portworx) UpdateOwnershipCloudCredential(ctx context.Context, req *api.CloudCredentialOwnershipUpdateRequest) (*api.CloudCredentialOwnershipUpdateResponse, error) {
	return p.cloudCredentialManager.UpdateOwnership(ctx, req)
}

func (p *portworx) CreateCluster(ctx context.Context, req *api.ClusterCreateRequest) (*api.ClusterCreateResponse, error) {
	return p.clusterManager.Create(ctx, req)
}

func (p *portworx) UpdateCluster(ctx context.Context, req *api.ClusterUpdateRequest) (*api.ClusterUpdateResponse, error) {
	return p.clusterManager.Update(ctx, req)
}

func (p *portworx) InspectCluster(ctx context.Context, req *api.ClusterInspectRequest) (*api.ClusterInspectResponse, error) {
	return p.clusterManager.Inspect(ctx, req)
}

func (p *portworx) EnumerateCluster(ctx context.Context, req *api.ClusterEnumerateRequest) (*api.ClusterEnumerateResponse, error) {
	return p.clusterManager.Enumerate(ctx, req)
}

func (p *portworx) DeleteCluster(ctx context.Context, req *api.ClusterDeleteRequest) (*api.ClusterDeleteResponse, error) {
	return p.clusterManager.Delete(ctx, req)
}

func (p *portworx) ClusterUpdateBackupShare(ctx context.Context, req *api.ClusterBackupShareUpdateRequest) (*api.ClusterBackupShareUpdateResponse, error) {
	return p.clusterManager.UpdateBackupShare(ctx, req)
}

// WaitForClusterDeletion waits for cluster to be deleted successfully
// or till timeout is reached. API should poll every `timeBeforeRetry` duration
func (p *portworx) WaitForClusterDeletion(
	ctx context.Context,
	clusterName,
	orgID string,
	timeout time.Duration,
	timeBeforeRetry time.Duration,
) error {
	req := &api.ClusterInspectRequest{
		Name:  clusterName,
		OrgId: orgID,
	}
	f := func() (interface{}, bool, error) {
		inspectClusterResp, err := p.clusterManager.Inspect(ctx, req)
		if err == nil {
			// Object still exists, just retry
			currentStatus := inspectClusterResp.GetCluster().GetStatus().GetStatus()
			return nil, true, fmt.Errorf("cluster [%v] is in [%s] state. Waiting to become complete",
				req.GetName(), currentStatus)
		}
		code := status.Code(err)
		// If error has code.NotFound, the cluster object is deleted.
		if code == codes.NotFound {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("Fetching cluster[%v] failed with err: %v", req.GetName(), err.Error())
	}

	_, err := task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
	if err != nil {
		return fmt.Errorf("failed to wait for cluster deletion. Error:[%v]", err)
	}

	return nil
}

func (p *portworx) CreateBackupLocation(ctx context.Context, req *api.BackupLocationCreateRequest) (*api.BackupLocationCreateResponse, error) {
	return p.backupLocationManager.Create(ctx, req)
}

func (p *portworx) UpdateBackupLocation(ctx context.Context, req *api.BackupLocationUpdateRequest) (*api.BackupLocationUpdateResponse, error) {
	return p.backupLocationManager.Update(ctx, req)
}

func (p *portworx) EnumerateBackupLocation(ctx context.Context, req *api.BackupLocationEnumerateRequest) (*api.BackupLocationEnumerateResponse, error) {
	return p.backupLocationManager.Enumerate(ctx, req)
}

func (p *portworx) InspectBackupLocation(ctx context.Context, req *api.BackupLocationInspectRequest) (*api.BackupLocationInspectResponse, error) {
	return p.backupLocationManager.Inspect(ctx, req)
}

func (p *portworx) DeleteBackupLocation(ctx context.Context, req *api.BackupLocationDeleteRequest) (*api.BackupLocationDeleteResponse, error) {
	return p.backupLocationManager.Delete(ctx, req)
}

func (p *portworx) ValidateBackupLocation(ctx context.Context, req *api.BackupLocationValidateRequest) (*api.BackupLocationValidateResponse, error) {
	return p.backupLocationManager.Validate(ctx, req)
}

func (p *portworx) UpdateOwnershipBackupLocation(ctx context.Context, req *api.BackupLocationOwnershipUpdateRequest) (*api.BackupLocationOwnershipUpdateResponse, error) {
	return p.backupLocationManager.UpdateOwnership(ctx, req)
}

// WaitForBackupLocationDeletion waits for backup location to be deleted successfully
// or till timeout is reached. API should poll every `timeBeforeRetry` duration
func (p *portworx) WaitForBackupLocationDeletion(
	ctx context.Context,
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
	var blError error
	f := func() (interface{}, bool, error) {
		inspectBlResp, err := p.backupLocationManager.Inspect(ctx, req)
		if err == nil {
			// Object still exsts, just retry
			currentStatus := inspectBlResp.GetBackupLocation().GetBackupLocationInfo().GetStatus().GetStatus()
			return nil, true, fmt.Errorf("backup location [%v] is in [%s] state",
				req.GetName(), currentStatus)
		}

		if inspectBlResp == nil {
			return nil, false, nil
		}
		currentStatus := inspectBlResp.GetBackupLocation().GetBackupLocationInfo().GetStatus().GetStatus()
		if currentStatus == api.BackupLocationInfo_StatusInfo_Invalid {
			log.Infof("in invalid state")
			blError = fmt.Errorf("backup location is [%v] is in [%s] state",
				req.GetName(), currentStatus)
			return nil, false, blError
		}
		return nil, false, nil
	}

	_, err := task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
	if err != nil {
		return fmt.Errorf("failed to wait for backup location deletion. Error:[%v]", err)
	}
	return nil
}

func (p *portworx) CreateBackup(ctx context.Context, req *api.BackupCreateRequest) (*api.BackupCreateResponse, error) {
	return p.backupManager.Create(ctx, req)
}

func (p *portworx) UpdateBackup(ctx context.Context, req *api.BackupUpdateRequest) (*api.BackupUpdateResponse, error) {
	return p.backupManager.Update(ctx, req)
}

func (p *portworx) EnumerateBackup(ctx context.Context, req *api.BackupEnumerateRequest) (*api.BackupEnumerateResponse, error) {
	return p.backupManager.Enumerate(ctx, req)
}

func (p *portworx) InspectBackup(ctx context.Context, req *api.BackupInspectRequest) (*api.BackupInspectResponse, error) {
	return p.backupManager.Inspect(ctx, req)
}

func (p *portworx) DeleteBackup(ctx context.Context, req *api.BackupDeleteRequest) (*api.BackupDeleteResponse, error) {
	return p.backupManager.Delete(ctx, req)
}

func (p *portworx) UpdateBackupShare(ctx context.Context, req *api.BackupShareUpdateRequest) (*api.BackupShareUpdateResponse, error) {
	return p.backupManager.UpdateBackupShare(ctx, req)
}

// GetVolumeBackupIDs returns backup IDs of volumes
func (p *portworx) GetVolumeBackupIDs(
	ctx context.Context,
	backupName string,
	namespace string,
	clusterObj *api.ClusterObject,
	orgID string,
) ([]string, error) {

	var volumeBackupIDs []string
	_, storkClient, err := getKubernetesInstance(clusterObj)
	if err != nil {
		return volumeBackupIDs, err
	}

	backupUUID, err := p.GetBackupUID(ctx, orgID, backupName)
	if err != nil {
		return volumeBackupIDs, err
	}
	backupInspectReq := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
		Uid:   backupUUID,
	}
	_, err = p.backupManager.Inspect(ctx, backupInspectReq)
	if err != nil {
		return volumeBackupIDs, err
	}
	storkApplicationBackupCRName := fmt.Sprintf("%s-%s", backupName, backupUUID[0:7])
	var storkApplicationBackupCR *v1alpha1.ApplicationBackup

	getBackupIDfromStork := func() (interface{}, bool, error) {
		storkApplicationBackupCR, err = storkClient.GetApplicationBackup(storkApplicationBackupCRName, namespace)
		if err != nil {
			log.Warnf("failed to get application backup CR [%s], Error:[%v]", storkApplicationBackupCRName, err)
			return false, true, err
		}
		log.Debugf("GetVolumeBackupIDs storkApplicationBackupCR: [%+v]\n", storkApplicationBackupCR)
		if len(storkApplicationBackupCR.Status.Volumes) > 0 {
			isComplete := true
			for _, backupVolume := range storkApplicationBackupCR.Status.Volumes {
				log.Debugf("Volume [%v] has backup ID: [%v]\n", backupVolume.Volume, backupVolume.BackupID)
				if !strings.Contains(backupVolume.BackupID, "/") {
					isComplete = false
				}
			}
			if isComplete {
				return false, false, nil
			}
		}

		return false, true, fmt.Errorf("Volume backup has not started yet")
	}

	_, err = task.DoRetryWithTimeout(getBackupIDfromStork, 5*time.Minute, 15*time.Second)
	if err != nil {
		return volumeBackupIDs, err
	}

	if len(storkApplicationBackupCR.Status.Volumes) == 0 {
		return nil, fmt.Errorf("no volumes are being backed up by backup [%s]/applicationBackup CR [%s]",
			backupName, storkApplicationBackupCRName)
	}

	for _, backupVolume := range storkApplicationBackupCR.Status.Volumes {
		log.Debugf("For backupVolume [%+v] with Status [%v] while getting backup id: [%+v]", backupVolume, backupVolume.Status, backupVolume.BackupID)
		if backupVolume.Status == "InProgress" && backupVolume.BackupID != "" {
			volumeBackupIDs = append(volumeBackupIDs, backupVolume.BackupID)
		} else {
			log.Debugf("Status of backup of volume [+%v] is [%s]. BackupID: [%+v] Reason: [%+v]",
				backupVolume, backupVolume.Status, backupVolume.BackupID, backupVolume.Reason)
		}
	}
	return volumeBackupIDs, nil
}

// WaitForBackupCompletion waits for backup to complete successfully
// or till timeout is reached. API should poll every `timeBeforeRetry` duration
func (p *portworx) WaitForBackupCompletion(
	ctx context.Context,
	backupName,
	orgID string,
	timeout time.Duration,
	timeBeforeRetry time.Duration,
) error {

	backupUID, err := p.GetBackupUID(ctx, orgID, backupName)
	if err != nil {
		return err
	}
	req := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
		Uid:   backupUID,
	}
	var backupError error
	f := func() (interface{}, bool, error) {
		inspectBkpResp, err := p.backupManager.Inspect(ctx, req)
		if err != nil {
			// Error occured, just retry
			return nil, true, err
		}

		// Check if backup status is complete
		currentStatus := inspectBkpResp.GetBackup().GetStatus().GetStatus()
		if currentStatus == api.BackupInfo_StatusInfo_Success {
			// If backup is complete, dont retry again
			return nil, false, nil
		} else if currentStatus == api.BackupInfo_StatusInfo_Failed ||
			currentStatus == api.BackupInfo_StatusInfo_Aborted ||
			currentStatus == api.BackupInfo_StatusInfo_Invalid {
			backupError = fmt.Errorf("backup [%v] is in [%s] state. reason: [%v]",
				req.GetName(), currentStatus,
				inspectBkpResp.GetBackup().GetStatus().GetReason())
			return nil, false, backupError
		}
		return nil,
			true,
			fmt.Errorf("backup [%v] is in [%s] state. Waiting to become Complete",
				req.GetName(), currentStatus)
	}

	_, err = task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
	if err != nil || backupError != nil {
		return fmt.Errorf("failed to wait for backup. Error:[%v] Reason:[%v]", err, backupError)
	}

	return nil
}

// WaitForBackupDeletion waits for backup to be deleted successfully
// or till timeout is reached. API should poll every `timeBeforeRetry` duration
func (p *portworx) WaitForBackupDeletion(
	ctx context.Context,
	backupName,
	orgID string,
	timeout time.Duration,
	timeBeforeRetry time.Duration,
) error {
	backupUID, err := p.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return err
	}
	req := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
		Uid:   backupUID,
	}
	var backupError error
	f := func() (interface{}, bool, error) {
		inspectBackupResp, err := p.backupManager.Inspect(ctx, req)
		if err == nil {
			// Object still exists, just retry
			currentStatus := inspectBackupResp.GetBackup().GetStatus().GetStatus()
			return nil, true, fmt.Errorf("backup [%v] is in [%s] state",
				req.GetName(), currentStatus)
		}

		if inspectBackupResp == nil {
			return nil, false, nil
		}
		// Check if backup delete status is complete
		currentStatus := inspectBackupResp.GetBackup().GetStatus().GetStatus()
		if currentStatus == api.BackupInfo_StatusInfo_Deleting ||
			currentStatus == api.BackupInfo_StatusInfo_DeletePending {
			// Backup deletion is not complete, retry again
			return nil,
				true,
				fmt.Errorf("backup [%v] is in [%s] state. Waiting to become Complete",
					req.GetName(), currentStatus)
		} else if currentStatus == api.BackupInfo_StatusInfo_Failed ||
			currentStatus == api.BackupInfo_StatusInfo_Aborted ||
			currentStatus == api.BackupInfo_StatusInfo_Invalid {
			backupError = fmt.Errorf("backup [%v] is in [%s] state",
				req.GetName(), currentStatus)
			return nil, false, backupError
		}
		return nil, false, nil
	}

	_, err = task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
	if err != nil {
		return fmt.Errorf("failed to wait for backup deletion. Error:[%v] Reason:[%v]", err, backupError)
	}

	return nil
}

// WaitForBackupDeletion waits for restore to be deleted successfully
// or till timeout is reached. API should poll every `timeBeforeRetry
func (p *portworx) WaitForRestoreDeletion(
	ctx context.Context,
	restoreName,
	orgID string,
	timeout time.Duration,
	timeBeforeRetry time.Duration,
) error {
	req := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}
	var backupError error
	f := func() (interface{}, bool, error) {
		restoreInspectResponse, err := p.restoreManager.Inspect(ctx, req)
		if err == nil {
			// Object still exists, just retry
			currentStatus := restoreInspectResponse.GetRestore().GetStatus().GetStatus()
			return nil, true, fmt.Errorf("restore [%v] is in [%s] state",
				req.GetName(), currentStatus)
		}

		if restoreInspectResponse == nil {
			return nil, false, nil
		}
		// Check if restore delete status is complete
		currentStatus := restoreInspectResponse.GetRestore().GetStatus().GetStatus()
		if currentStatus == api.RestoreInfo_StatusInfo_Deleting {
			// Restore deletion is not complete, retry again
			return nil,
				true,
				fmt.Errorf("restore [%v] is in [%s] state. Waiting to become Complete",
					req.GetName(), currentStatus)
		} else if currentStatus == api.RestoreInfo_StatusInfo_Failed ||
			currentStatus == api.RestoreInfo_StatusInfo_Aborted ||
			currentStatus == api.RestoreInfo_StatusInfo_Invalid {
			backupError = fmt.Errorf("restore [%v] is in [%s] state",
				req.GetName(), currentStatus)
			return nil, false, backupError
		}
		return nil, false, nil
	}

	_, err := task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
	if err != nil {
		return fmt.Errorf("failed to wait for restore deletion. Error:[%v] Reason:[%v]", err, backupError)
	}

	return nil
}

// WaitForDeletePending checking if a given backup object is in delete pending state
func (p *portworx) WaitForDeletePending(
	ctx context.Context,
	backupName,
	orgID string,
	timeout time.Duration,
	timeBeforeRetry time.Duration,
) error {
	backupUID, err := p.GetBackupUID(ctx, orgID, backupName)
	if err != nil {
		return err
	}
	req := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
		Uid:   backupUID,
	}
	var backupError error
	f := func() (interface{}, bool, error) {
		inspectBackupResp, err := p.backupManager.Inspect(ctx, req)
		if err != nil {
			// Error occured, just retry
			return nil, true, err
		}

		if inspectBackupResp == nil {
			return nil, false, nil
		}
		// Check if backup delete status is complete
		currentStatus := inspectBackupResp.GetBackup().GetStatus().GetStatus()
		if currentStatus != api.BackupInfo_StatusInfo_DeletePending {
			return nil,
				true,
				fmt.Errorf("backup [%v] is in [%s] state. Waiting to transition to delete pending",
					req.GetName(), currentStatus)
		}

		return nil, false, nil
	}

	_, err = task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
	if err != nil {
		return fmt.Errorf("failed to transition to delete pending. Error:[%v] Reason:[%v]", err, backupError)
	}

	return nil
}

func (p *portworx) CreateRestore(ctx context.Context, req *api.RestoreCreateRequest) (*api.RestoreCreateResponse, error) {
	return p.restoreManager.Create(ctx, req)
}

func (p *portworx) UpdateRestore(ctx context.Context, req *api.RestoreUpdateRequest) (*api.RestoreUpdateResponse, error) {
	return p.restoreManager.Update(ctx, req)
}

func (p *portworx) EnumerateRestore(ctx context.Context, req *api.RestoreEnumerateRequest) (*api.RestoreEnumerateResponse, error) {
	return p.restoreManager.Enumerate(ctx, req)
}

func (p *portworx) InspectRestore(ctx context.Context, req *api.RestoreInspectRequest) (*api.RestoreInspectResponse, error) {
	return p.restoreManager.Inspect(ctx, req)
}

func (p *portworx) DeleteRestore(ctx context.Context, req *api.RestoreDeleteRequest) (*api.RestoreDeleteResponse, error) {
	return p.restoreManager.Delete(ctx, req)
}

// WaitForRestoreCompletion waits for restore to complete successfully
// or till timeout is reached. API should poll every `timeBeforeRetry` duration
func (p *portworx) WaitForRestoreCompletion(
	ctx context.Context,
	restoreName,
	orgID string,
	timeout time.Duration,
	timeBeforeRetry time.Duration,
) error {
	req := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}
	var restoreError error
	f := func() (interface{}, bool, error) {
		inspectRestoreResp, err := p.restoreManager.Inspect(ctx, req)
		if err != nil {
			// Error occured, just retry
			return nil, true, err
		}

		// Check if restore is complete
		currentStatus := inspectRestoreResp.GetRestore().GetStatus().GetStatus()
		if currentStatus == api.RestoreInfo_StatusInfo_Success ||
			currentStatus == api.RestoreInfo_StatusInfo_PartialSuccess {
			// If restore is complete, dont retry again
			return nil, false, nil
		} else if currentStatus == api.RestoreInfo_StatusInfo_Failed ||
			currentStatus == api.RestoreInfo_StatusInfo_Aborted ||
			currentStatus == api.RestoreInfo_StatusInfo_Invalid {
			restoreError = fmt.Errorf("restore [%v] is in [%s] state. Reason: [%s]",
				req.GetName(), currentStatus, inspectRestoreResp.GetRestore().GetStatus().GetReason())
			return nil, false, restoreError
		}
		return nil,
			true,
			fmt.Errorf("restore [%v] is in [%s] state. Waiting to become Complete",
				req.GetName(), currentStatus)
	}

	_, err := task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
	if err != nil || restoreError != nil {
		return fmt.Errorf("failed to wait for restore to complete. Error:[%v] Reason:[%v]", err, restoreError)
	}

	return nil
}

func (p *portworx) CreateSchedulePolicy(ctx context.Context, req *api.SchedulePolicyCreateRequest) (*api.SchedulePolicyCreateResponse, error) {
	return p.schedulePolicyManager.Create(ctx, req)
}

func (p *portworx) UpdateSchedulePolicy(ctx context.Context, req *api.SchedulePolicyUpdateRequest) (*api.SchedulePolicyUpdateResponse, error) {
	return p.schedulePolicyManager.Update(ctx, req)
}

func (p *portworx) EnumerateSchedulePolicy(ctx context.Context, req *api.SchedulePolicyEnumerateRequest) (*api.SchedulePolicyEnumerateResponse, error) {
	return p.schedulePolicyManager.Enumerate(ctx, req)
}

func (p *portworx) InspectSchedulePolicy(ctx context.Context, req *api.SchedulePolicyInspectRequest) (*api.SchedulePolicyInspectResponse, error) {
	return p.schedulePolicyManager.Inspect(ctx, req)
}

func (p *portworx) DeleteSchedulePolicy(ctx context.Context, req *api.SchedulePolicyDeleteRequest) (*api.SchedulePolicyDeleteResponse, error) {
	return p.schedulePolicyManager.Delete(ctx, req)
}

func (p *portworx) UpdateOwnershiSchedulePolicy(ctx context.Context, req *api.SchedulePolicyOwnershipUpdateRequest) (*api.SchedulePolicyOwnershipUpdateResponse, error) {
	return p.schedulePolicyManager.UpdateOwnership(ctx, req)
}

func (p *portworx) CreateBackupSchedule(ctx context.Context, req *api.BackupScheduleCreateRequest) (*api.BackupScheduleCreateResponse, error) {
	return p.backupScheduleManager.Create(ctx, req)
}

func (p *portworx) UpdateBackupSchedule(ctx context.Context, req *api.BackupScheduleUpdateRequest) (*api.BackupScheduleUpdateResponse, error) {
	return p.backupScheduleManager.Update(ctx, req)
}

func (p *portworx) EnumerateBackupSchedule(ctx context.Context, req *api.BackupScheduleEnumerateRequest) (*api.BackupScheduleEnumerateResponse, error) {
	return p.backupScheduleManager.Enumerate(ctx, req)
}

func (p *portworx) InspectBackupSchedule(ctx context.Context, req *api.BackupScheduleInspectRequest) (*api.BackupScheduleInspectResponse, error) {
	return p.backupScheduleManager.Inspect(ctx, req)
}

func (p *portworx) DeleteBackupSchedule(ctx context.Context, req *api.BackupScheduleDeleteRequest) (*api.BackupScheduleDeleteResponse, error) {
	return p.backupScheduleManager.Delete(ctx, req)
}

// BackupScheduleWaitForNBackupsCompletion waits for given number of backup to be complete successfully
// or till timeout is reached. API should poll every `timeBeforeRetry` duration
func (p *portworx) BackupScheduleWaitForNBackupsCompletion(
	ctx context.Context,
	name,
	orgID string,
	count int,
	timeout time.Duration,
	timeBeforeRetry time.Duration,
) error {
	req := &api.BackupEnumerateRequest{
		OrgId: orgID,
	}
	req.EnumerateOptions = &api.EnumerateOptions{
		MaxObjects: uint64(count),
	}
	f := func() (interface{}, bool, error) {
		var backups []*api.BackupObject
		// Get backup list
		resp, err := p.backupManager.Enumerate(ctx, req)
		if err != nil {
			return nil, true, err
		}
		backups = append(backups, resp.GetBackups()...)
		if len(backups) < count {
			return nil,
				true,
				fmt.Errorf("waiting for request number of backup. Current[%v] and requested[%v]", len(backups), count)
		}
		for _, backupObj := range backups {
			if backupObj.GetStatus().GetStatus() == api.BackupInfo_StatusInfo_Success ||
				backupObj.GetStatus().GetStatus() == api.BackupInfo_StatusInfo_PartialSuccess {
				continue
			} else if backupObj.GetStatus().GetStatus() == api.BackupInfo_StatusInfo_Failed ||
				backupObj.GetStatus().GetStatus() == api.BackupInfo_StatusInfo_Aborted ||
				backupObj.GetStatus().GetStatus() == api.BackupInfo_StatusInfo_Invalid {
				backupError := fmt.Errorf("backup[%v] is in [%s] state. Reason: [%s]",
					backupObj.GetName(), backupObj.GetStatus().GetStatus(), backupObj.GetStatus().GetReason())
				return nil, false, backupError
			}
			return nil,
				true,
				fmt.Errorf("backup [%v] is in [%v] state. Waiting to become completed", backupObj.GetName(), backupObj.GetStatus().GetStatus())
		}

		return nil, false, nil
	}
	_, err := task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
	if err != nil {
		return fmt.Errorf("failed to wait for backupschedule. Error:[%v]", err)
	}
	return nil
}

// WaitForBackupScheduleDeleteWithDeleteFlag waits for backupschedule to be deleted successfully
// or till timeout is reached. API should poll every `timeBeforeRetry` duration
// This wait function is for the backupschedule deletion with delete-backup option set.
func (p *portworx) WaitForBackupScheduleDeletion(
	ctx context.Context,
	backupScheduleName,
	namespace,
	orgID string,
	clusterObj *api.ClusterObject,
	timeout time.Duration,
	timeBeforeRetry time.Duration,
) error {
	req := &api.BackupScheduleInspectRequest{
		Name:  backupScheduleName,
		OrgId: orgID,
	}
	f := func() (interface{}, bool, error) {
		enumerateBatchSize := 10
		inspectBackupScheduleResp, err := p.backupScheduleManager.Inspect(ctx, req)
		if err == nil {
			// Object still exists, just retry
			currentStatus := inspectBackupScheduleResp.GetBackupSchedule().GetStatus().GetStatus()
			return nil, true, fmt.Errorf("backupSchedule [%v] is in [%s] state",
				req.GetName(), currentStatus)
		}
		// Object does not exist.
		if inspectBackupScheduleResp == nil {
			return nil, false, nil
		}
		// Make sure the backup objects are deleted.
		var backups []*api.BackupObject
		req := &api.BackupEnumerateRequest{
			OrgId: orgID,
		}
		req.EnumerateOptions = &api.EnumerateOptions{
			MaxObjects: uint64(enumerateBatchSize),
		}
		// Get backup list
		for true {
			resp, err := p.backupManager.Enumerate(ctx, req)
			if err != nil {
				return nil, true, err
			}
			backups = append(backups, resp.GetBackups()...)
			if resp.GetComplete() {
				break
			} else {
				req.EnumerateOptions.ObjectIndex += uint64(len(resp.GetBackups()))
			}
		}
		// retry again, if backup objects remained undeleted.
		if len(backups) != 0 {
			return nil,
				true,
				fmt.Errorf("[%v] number of backups remain undeleted", len(backups))
		}
		// Check all the backup CRs are deleted.
		_, inst, err := getKubernetesInstance(clusterObj)
		if err != nil {
			return nil, true, err
		}
		backupCrs, err := inst.ListApplicationBackups(namespace, metav1.ListOptions{})
		if err != nil {
			return nil, true, err
		}
		if len(backupCrs.Items) != 0 {
			return nil,
				true,
				fmt.Errorf("[%v] number of backup CR remain undeleted", len(backupCrs.Items))
		}

		return nil, false, nil
	}
	_, err := task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
	if err != nil {
		return fmt.Errorf("failed to wait for backup schedule deletion. Error:[%v]", err)
	}

	return nil
}

// WaitForBackupRunning wait for backup to start running
func (p *portworx) WaitForBackupRunning(
	ctx context.Context,
	req *api.BackupInspectRequest,
	timeout,
	retryInterval time.Duration,
) error {
	var backupErr error

	t := func() (interface{}, bool, error) {
		log.Debugf("WaitForBackupRunning inspect backup state for %s", req.Name)
		resp, err := p.backupManager.Inspect(ctx, req)

		if err != nil {
			return nil, true, err
		}

		// Check if backup in progress - stop
		currentStatus := resp.GetBackup().GetStatus().GetStatus()
		if currentStatus == api.BackupInfo_StatusInfo_InProgress {
			return nil, false, nil
		} else if currentStatus == api.BackupInfo_StatusInfo_Failed ||
			currentStatus == api.BackupInfo_StatusInfo_Aborted ||
			currentStatus == api.BackupInfo_StatusInfo_Invalid {

			backupErr = fmt.Errorf("backup [%v] is in [%s] state",
				req.GetName(), currentStatus)
			return nil, false, backupErr
		}

		// Otherwise retry
		return nil, true, nil
	}

	_, err := task.DoRetryWithTimeout(t, timeout, retryInterval)

	if err != nil || backupErr != nil {
		return fmt.Errorf("failed to wait for running start. Error:[%v] Reason:[%v]", err, backupErr)
	}

	return nil
}

// WaitForRestoreRunning wait for backup to start running
func (p *portworx) WaitForRestoreRunning(
	ctx context.Context,
	req *api.RestoreInspectRequest,
	timeout,
	retryInterval time.Duration,
) error {
	var backupErr error

	t := func() (interface{}, bool, error) {
		log.Debugf("WaitForRestoreRunning inspect backup state for %s", req.Name)
		resp, err := p.restoreManager.Inspect(ctx, req)

		if err != nil {
			return nil, true, err
		}

		// Check if backup in progress - stop
		currentStatus := resp.GetRestore().GetStatus().GetStatus()
		if currentStatus == api.RestoreInfo_StatusInfo_InProgress {
			return nil, false, nil
		} else if currentStatus == api.RestoreInfo_StatusInfo_Failed ||
			currentStatus == api.RestoreInfo_StatusInfo_Aborted ||
			currentStatus == api.RestoreInfo_StatusInfo_Invalid {

			backupErr = fmt.Errorf("restore [%v] is in [%s] state",
				req.GetName(), currentStatus)
			return nil, false, backupErr
		}

		// Otherwise retry
		return nil, true, nil
	}

	_, err := task.DoRetryWithTimeout(t, timeout, retryInterval)
	if err != nil || backupErr != nil {
		return fmt.Errorf("failed to wait for running start. Error:[%v] Reason:[%v]", err, backupErr)
	}

	return nil
}

func (p *portworx) ActivateLicense(ctx context.Context, req *api.LicenseActivateRequest) (*api.LicenseActivateResponse, error) {
	return p.licenseManager.Activate(ctx, req)
}

func (p *portworx) InspectLicense(ctx context.Context, req *api.LicenseInspectRequest) (*api.LicenseInspectResponse, error) {
	return p.licenseManager.Inspect(ctx, req)
}

func (p *portworx) WaitForLicenseActivation(ctx context.Context, req *api.LicenseInspectRequest, timeout, retryInterval time.Duration) error {
	var licenseErr error

	t := func() (interface{}, bool, error) {
		resp, err := p.licenseManager.Inspect(ctx, req)

		if err != nil {
			return nil, true, err
		}

		// Check if we got response from license server
		if len(resp.GetLicenseRespInfo().GetFeatureInfo()) == 0 {
			licenseErr = fmt.Errorf("failed to activate license for orgID %v", req.GetOrgId())
			return nil, false, licenseErr
		}
		// iterate over the feature and check if valid feature is present
		for _, featureInfo := range resp.GetLicenseRespInfo().GetFeatureInfo() {
			if featureInfo.GetName() != licFeatureName {
				licenseErr = fmt.Errorf("found invalid feature name")
				return nil, false, licenseErr
			}
		}
		// All good
		return nil, true, nil
	}

	_, err := task.DoRetryWithTimeout(t, timeout, retryInterval)

	if err != nil || licenseErr != nil {
		return fmt.Errorf("failed to wait for license activation. Error:[%v] Reason:[%v]", err, licenseErr)
	}

	return nil
}

func (p *portworx) CreateRule(ctx context.Context, req *api.RuleCreateRequest) (*api.RuleCreateResponse, error) {
	return p.ruleManager.Create(ctx, req)
}

func (p *portworx) UpdateRule(ctx context.Context, req *api.RuleUpdateRequest) (*api.RuleUpdateResponse, error) {
	return p.ruleManager.Update(ctx, req)
}

func (p *portworx) EnumerateRule(ctx context.Context, req *api.RuleEnumerateRequest) (*api.RuleEnumerateResponse, error) {
	return p.ruleManager.Enumerate(ctx, req)
}

func (p *portworx) InspectRule(ctx context.Context, req *api.RuleInspectRequest) (*api.RuleInspectResponse, error) {
	return p.ruleManager.Inspect(ctx, req)
}

func (p *portworx) DeleteRule(ctx context.Context, req *api.RuleDeleteRequest) (*api.RuleDeleteResponse, error) {
	return p.ruleManager.Delete(ctx, req)
}

func (p *portworx) UpdateOwnershipRule(ctx context.Context, req *api.RuleOwnershipUpdateRequest) (*api.RuleOwnershipUpdateResponse, error) {
	return p.ruleManager.UpdateOwnership(ctx, req)
}

func (p *portworx) GetBackupUID(ctx context.Context, backupName string, orgID string) (string, error) {
	var backupUID string
	var totalBackups int
	bkpEnumerateReq := &api.BackupEnumerateRequest{OrgId: orgID}
	bkpEnumerateReq.EnumerateOptions = &api.EnumerateOptions{MaxObjects: uint64(enumerateBatchSize), ObjectIndex: 0}
	for {
		enumerateRsp, err := p.EnumerateBackup(ctx, bkpEnumerateReq)
		log.FailOnError(err, "Failed to enumerate backups for org %s ctx: [%v]\n Backup enumerate request: [%v]", orgID, err, bkpEnumerateReq)
		for _, backup := range enumerateRsp.GetBackups() {
			if backup.GetName() == backupName {
				return backup.GetUid(), nil
			}
			totalBackups++
		}
		if uint64(totalBackups) >= enumerateRsp.GetTotalCount() {
			break
		} else {
			bkpEnumerateReq.EnumerateOptions.ObjectIndex += uint64(len(enumerateRsp.GetBackups()))
		}
	}
	return backupUID, nil
}

var (
	// AppParameters Here the len of "pre_action_list","pod_selector_list","background", "runInSinglePod", "container"
	//should be same for any given app for a pre,post rule
	AppParameters = map[string]map[string]map[string][]string{
		"cassandra": {"pre": {"pre_action_list": {"nodetool flush -- keyspace1;", "echo 'test"},
			"pod_selector_list": {"app=cassandra", "app=cassandra1"},
			"background":        {"false", "false"},
			"runInSinglePod":    {"false", "false"},
			"container":         {"", ""},
		},
			"post": {"post_action_list": {"nodetool verify -- keyspace1;", "nodetool verify -- keyspace1;"},
				"background":        {"false", "false"},
				"pod_selector_list": {"app=cassandra", "app=cassandra1"},
				"runInSinglePod":    {"false", "false"},
				"container":         {"", ""},
			},
		},
		"postgres": {"pre": {"pre_action_list": {"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\";"},
			"background":        {"false"},
			"runInSinglePod":    {"false"},
			"pod_selector_list": {"app=postgres"},
			"container":         {"", ""},
		},
		},
		"postgres-backup": {"pre": {"pre_action_list": {"PGPASSWORD=$POSTGRES_PASSWORD; psql -U \"$POSTGRES_USER\" -c \"CHECKPOINT\";"},
			"background":        {"false"},
			"runInSinglePod":    {"false"},
			"pod_selector_list": {"app=postgres"},
			"container":         {"", ""},
		},
		},
	}
)

func (p *portworx) CreateRuleForBackup(appName string, orgID string, prePostFlag string) (bool, string, error) {
	var podSelector []map[string]string
	var actionValue []string
	var container []string
	var background []bool
	var runInSinglePod []bool
	var rulesInfo api.RulesInfo
	var uid string
	if prePostFlag == "pre" {
		if _, ok := AppParameters[appName]["pre"]; ok {
			for i := 0; i < len(AppParameters[appName]["pre"]["pre_action_list"]); i++ {
				ps := strings.Split(AppParameters[appName]["pre"]["pod_selector_list"][i], "=")
				psMap := make(map[string]string)
				psMap[ps[0]] = ps[1]
				podSelector = append(podSelector, psMap)
				actionValue = append(actionValue, AppParameters[appName]["pre"]["pre_action_list"][i])
				backgroundVal, _ := strconv.ParseBool(AppParameters[appName]["pre"]["background"][i])
				background = append(background, backgroundVal)
				podVal, _ := strconv.ParseBool(AppParameters[appName]["pre"]["runInSinglePod"][i])
				runInSinglePod = append(runInSinglePod, podVal)
				container = append(container, AppParameters[appName]["pre"]["container"][i])
			}
		} else {
			log.Infof("Pre rule not required for this application")
		}
	} else {
		if _, ok := AppParameters[appName]["post"]; ok {
			for i := 0; i < len(AppParameters[appName]["post"]["post_action_list"]); i++ {
				ps := strings.Split(AppParameters[appName]["post"]["pod_selector_list"][i], "=")
				psMap := make(map[string]string)
				psMap[ps[0]] = ps[1]
				podSelector = append(podSelector, psMap)
				actionValue = append(actionValue, AppParameters[appName]["post"]["post_action_list"][i])
				backgroundVal, _ := strconv.ParseBool(AppParameters[appName]["post"]["background"][i])
				background = append(background, backgroundVal)
				podVal, _ := strconv.ParseBool(AppParameters[appName]["post"]["runInSinglePod"][i])
				runInSinglePod = append(runInSinglePod, podVal)
				container = append(container, AppParameters[appName]["post"]["container"][i])
			}
		} else {
			log.Infof("Post rule not required for this application")
		}
	}
	totalRules := len(actionValue)
	if totalRules == 0 {
		log.Info("Rules not required for the apps")
		return true, "", nil
	}
	timestamp := strconv.Itoa(int(time.Now().Unix()))
	ruleName := fmt.Sprintf("%s-%s-rule-%s", appName, prePostFlag, timestamp)
	rulesInfoRuleItem := make([]api.RulesInfo_RuleItem, totalRules)
	for i := 0; i < totalRules; i++ {
		ruleAction := api.RulesInfo_Action{Background: background[i], RunInSinglePod: runInSinglePod[i],
			Value: actionValue[i]}
		var actions = []*api.RulesInfo_Action{&ruleAction}
		rulesInfoRuleItem[i].PodSelector = podSelector[i]
		rulesInfoRuleItem[i].Actions = actions
		rulesInfoRuleItem[i].Container = container[i]
		rulesInfo.Rules = append(rulesInfo.Rules, &rulesInfoRuleItem[i])
	}
	RuleCreateReq := &api.RuleCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  ruleName,
			OrgId: orgID,
		},
		RulesInfo: &rulesInfo,
	}
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		err = fmt.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
		return false, ruleName, err
	}

	_, err = p.CreateRule(ctx, RuleCreateReq)
	if err != nil {
		err = fmt.Errorf("Failed to create backup rules: [%v]", err)
		return false, ruleName, err
	}
	log.Infof("Validate rules for backup")
	RuleEnumerateReq := &api.RuleEnumerateRequest{
		OrgId: orgID,
	}
	ruleList, err := p.EnumerateRule(ctx, RuleEnumerateReq)
	for i := 0; i < len(ruleList.Rules); i++ {
		if ruleList.Rules[i].Metadata.Name == ruleName {
			uid = ruleList.Rules[i].Metadata.Uid
			break
		}
	}
	RuleInspectReq := &api.RuleInspectRequest{
		OrgId: orgID,
		Name:  ruleName,
		Uid:   uid,
	}
	_, err = p.InspectRule(ctx, RuleInspectReq)
	if err != nil {
		err = fmt.Errorf("Failed to validate the created rule with Error: [%v]", err)
		return false, ruleName, err
	}
	return true, ruleName, nil
}

func (p *portworx) CreateIntervalSchedulePolicy(retain int64, min int64, incrCount uint64) *api.SchedulePolicyInfo {
	SchedulePolicy := &api.SchedulePolicyInfo{
		Interval: &api.SchedulePolicyInfo_IntervalPolicy{
			Retain:  retain,
			Minutes: min,
			IncrementalCount: &api.SchedulePolicyInfo_IncrementalCount{
				Count: incrCount,
			},
		},
	}
	return SchedulePolicy
}

func (p *portworx) CreateDailySchedulePolicy(retain int64, time string, incrCount uint64) *api.SchedulePolicyInfo {
	SchedulePolicy := &api.SchedulePolicyInfo{
		Daily: &api.SchedulePolicyInfo_DailyPolicy{
			Retain: retain,
			Time:   time,
			IncrementalCount: &api.SchedulePolicyInfo_IncrementalCount{
				Count: incrCount,
			},
		},
	}
	return SchedulePolicy
}

func (p *portworx) CreateWeeklySchedulePolicy(retain int64, day backup.Weekday, time string, incrCount uint64) *api.SchedulePolicyInfo {

	SchedulePolicy := &api.SchedulePolicyInfo{
		Weekly: &api.SchedulePolicyInfo_WeeklyPolicy{
			Retain: retain,
			Day:    string(day),
			Time:   time,
			IncrementalCount: &api.SchedulePolicyInfo_IncrementalCount{
				Count: incrCount,
			},
		},
	}
	return SchedulePolicy
}

func (p *portworx) CreateMonthlySchedulePolicy(retain int64, date int64, time string, incrCount uint64) *api.SchedulePolicyInfo {
	SchedulePolicy := &api.SchedulePolicyInfo{
		Monthly: &api.SchedulePolicyInfo_MonthlyPolicy{
			Retain: retain,
			Date:   date,
			Time:   time,
			IncrementalCount: &api.SchedulePolicyInfo_IncrementalCount{
				Count: incrCount,
			},
		},
	}
	return SchedulePolicy
}

func (p *portworx) BackupSchedulePolicy(name string, uid string, orgId string, schedulePolicyInfo *api.SchedulePolicyInfo) error {
	log.InfoD("Create Backup Schedule Policy")
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		err = fmt.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
		return err
	}
	schedulePolicyCreateRequest := &api.SchedulePolicyCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			Uid:   uid,
			OrgId: orgId,
		},
		SchedulePolicy: schedulePolicyInfo,
	}
	_, err = p.CreateSchedulePolicy(ctx, schedulePolicyCreateRequest)
	if err != nil {
		err = fmt.Errorf("Error in creating schedule policy is +%v", err)
		return err
	}
	return nil
}

// GetSchedulePolicyUid gets the uid for the given schedule policy
func (p *portworx) GetSchedulePolicyUid(orgID string, ctx context.Context, schPolicyName string) (string, error) {
	SchedulePolicyEnumerateReq := &api.SchedulePolicyEnumerateRequest{
		OrgId: orgID,
	}
	schPolicyList, err := p.EnumerateSchedulePolicy(ctx, SchedulePolicyEnumerateReq)
	if err != nil {
		return "", fmt.Errorf("Failed to enumerate schedule policies with error: [%v]", err)
	}
	for i := 0; i < len(schPolicyList.SchedulePolicies); i++ {
		if schPolicyList.SchedulePolicies[i].Metadata.Name == schPolicyName {
			schPolicyUid := schPolicyList.SchedulePolicies[i].Metadata.Uid
			return schPolicyUid, nil
		}
	}
	return "", fmt.Errorf("Unable to find schedule policy Uid")
}

func (p *portworx) RegisterBackupCluster(orgID, clusterName, uid string) (api.ClusterInfo_StatusInfo_Status, string) {
	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Failed to fetch px-central-admin ctx")
	clusterReq := &api.ClusterInspectRequest{OrgId: orgID, Name: clusterName, IncludeSecrets: true}
	clusterResp, err := p.InspectCluster(ctx, clusterReq)
	log.FailOnError(err, "Cluster Object for cluster %s and Org id %s is empty", clusterName, orgID)
	clusterObj := clusterResp.GetCluster()
	return clusterObj.Status.Status, clusterObj.Uid
}

func (p *portworx) RegisterBackupClusterNonAdminUser(orgID, clusterName, uid string, ctx context.Context) (api.ClusterInfo_StatusInfo_Status, string) {
	clusterReq := &api.ClusterInspectRequest{OrgId: orgID, Name: clusterName, IncludeSecrets: true}
	clusterResp, err := p.InspectCluster(ctx, clusterReq)
	log.FailOnError(err, "Cluster Object for cluster %s and Org id %s is empty", clusterName, orgID)
	clusterObj := clusterResp.GetCluster()
	return clusterObj.Status.Status, clusterObj.Uid
}

func (p *portworx) GetRuleUid(orgID string, ctx context.Context, ruleName string) (string, error) {
	RuleEnumerateReq := &api.RuleEnumerateRequest{
		OrgId: orgID,
	}
	ruleList, err := p.EnumerateRule(ctx, RuleEnumerateReq)
	if err != nil {
		err = fmt.Errorf("Failed to enumerate rules with error: [%v]", err)
		return "", err
	}
	for i := 0; i < len(ruleList.Rules); i++ {
		if ruleList.Rules[i].Metadata.Name == ruleName {
			ruleUid := ruleList.Rules[i].Metadata.Uid
			return ruleUid, nil
		}
	}
	return "", nil
}

func (p *portworx) DeleteRuleForBackup(orgID string, ruleName string) error {
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		err = fmt.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
		return err
	}
	ruleUid, err := p.GetRuleUid(orgID, ctx, ruleName)
	if err != nil {
		err = fmt.Errorf("Failed to get rule UID: [%v]", err)
		return err
	}
	if ruleUid != "" {
		RuleDeleteReq := &api.RuleDeleteRequest{
			Name:  ruleName,
			OrgId: orgID,
			Uid:   ruleUid,
		}
		_, err = p.DeleteRule(ctx, RuleDeleteReq)
		if err != nil {
			err = fmt.Errorf("Failed to delete rule: [%v]", err)
			return err
		}
	} else {
		err = fmt.Errorf("Unable to fetch rule Uid")
		return err
	}
	return nil
}

func (p *portworx) DeleteBackupSchedulePolicy(orgID string, policyList []string) error {
	ctx, err := backup.GetAdminCtxFromSecret()
	if err != nil {
		err = fmt.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
		return err
	}
	schedPolicyMap := make(map[string]string)
	schedPolicyEnumerateReq := &api.SchedulePolicyEnumerateRequest{
		OrgId: orgID,
	}
	schedulePolicyList, err := p.EnumerateSchedulePolicy(ctx, schedPolicyEnumerateReq)
	if err != nil {
		err = fmt.Errorf("Failed to get list of schedule policies with error: [%v]", err)
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
		_, err := p.DeleteSchedulePolicy(ctx, schedPolicydeleteReq)
		if err != nil {
			err = fmt.Errorf("Failed to delete schedule policy %s with error [%v]", policyList[i], err)
			return err
		}
	}
	return nil
}

func (p *portworx) ValidateBackupCluster() error {
	flag := false
	labelSelectors := map[string]string{"job-name": post_install_hook_pod}
	ns := backup.GetPxBackupNamespace()
	pods, err := core.Instance().GetPods(ns, labelSelectors)
	if err != nil {
		err = fmt.Errorf("Unable to fetch pxcentral-post-install-hook pod from backup namespace\n Error : [%v]\n",
			err)
		return err
	}
	for _, pod := range pods.Items {
		log.Infof("Checking if the pxcentral-post-install-hook pod is in Completed state or not")
		bkpPod, err := core.Instance().GetPodByName(pod.GetName(), ns)
		if err != nil {
			err = fmt.Errorf("Error: %v Occured while getting the pxcentral-post-install-hook pod details", err)
			return err
		}
		containerList := bkpPod.Status.ContainerStatuses
		for i := 0; i < len(containerList); i++ {
			status := containerList[i].State.Terminated.Reason
			if status == "Completed" {
				log.Infof("pxcentral-post-install-hook pod is in completed state")
				flag = true
				break
			}
		}
	}
	if flag == false {
		err = fmt.Errorf("pxcentral-post-install-hook pod is not in completed state")
		return err
	}
	bkpPods, err := core.Instance().GetPods(ns, nil)
	if err != nil {
		err = fmt.Errorf("Unable to get pods in namespace %s with error %v", ns, err)
		return err
	}
	for _, pod := range bkpPods.Items {
		matched, _ := regexp.MatchString(post_install_hook_pod, pod.GetName())
		if !matched {
			equal, _ := regexp.MatchString(quick_maintenance_pod, pod.GetName())
			equal1, _ := regexp.MatchString(full_maintenance_pod, pod.GetName())
			if !(equal || equal1) {
				log.Info("Checking if all the containers are up or not")
				res := core.Instance().IsPodRunning(pod)
				if !res {
					err = fmt.Errorf("All the containers of pod %s are not Up", pod.GetName())
					return err
				}
				err = core.Instance().ValidatePod(&pod, defaultTimeout, defaultTimeout)
				if err != nil {
					err = fmt.Errorf("An Error: %v  Occured while validating the pod %s", err, pod.GetName())
					return err
				}
			}
		}
	}
	return nil
}

func init() {
	backup.Register(driverName, &portworx{})
}
