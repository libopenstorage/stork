package portworx

import (
	"context"
	"encoding/base64"
	"fmt"
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
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	driverName            = "pxb"
	pxbRestPort           = 10001
	defaultPxbServicePort = 10002
	pxbServiceName        = "px-backup"
	pxbNamespace          = "px-backup"
	schedulerDriverName   = "k8s"
	nodeDriverName        = "ssh"
	volumeDriverName      = "pxd"
	licFeatureName        = "BackupNodeCount"
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

	logrus.Infof("using portworx backup driver under scheduler: %v", schedulerDriverName)

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

	if err = p.setDriver(pxbServiceName, pxbNamespace); err != nil {
		return fmt.Errorf("Error setting px-backup endpoint: %v", err)
	}

	return err

}

func (p *portworx) constructURL(ip string) string {
	return fmt.Sprintf("%s:%d", ip, defaultPxbServicePort)
}

func (p *portworx) testAndSetEndpoint(endpoint string) error {
	pxEndpoint := p.constructURL(endpoint)
	conn, err := grpc.Dial(pxEndpoint, grpc.WithInsecure())
	if err != nil {
		logrus.Errorf("unable to get grpc connection: %v", err)
		return err
	}

	p.healthManager = api.NewHealthClient(conn)
	_, err = p.healthManager.Status(context.Background(), &api.HealthStatusRequest{})
	if err != nil {
		logrus.Errorf("HealthManager API error: %v", err)
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

	logrus.Infof("Using %v as endpoint for portworx backup driver", pxEndpoint)

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

func (p *portworx) CreateOrganization(req *api.OrganizationCreateRequest) (*api.OrganizationCreateResponse, error) {
	return p.organizationManager.Create(context.Background(), req)
}

func (p *portworx) EnumerateOrganization() (*api.OrganizationEnumerateResponse, error) {
	return p.organizationManager.Enumerate(context.Background(), &api.OrganizationEnumerateRequest{})
}

func (p *portworx) CreateCloudCredential(req *api.CloudCredentialCreateRequest) (*api.CloudCredentialCreateResponse, error) {
	return p.cloudCredentialManager.Create(context.Background(), req)
}

func (p *portworx) UpdateCloudCredential(req *api.CloudCredentialUpdateRequest) (*api.CloudCredentialUpdateResponse, error) {
	return p.cloudCredentialManager.Update(context.Background(), req)
}

func (p *portworx) InspectCloudCredential(req *api.CloudCredentialInspectRequest) (*api.CloudCredentialInspectResponse, error) {
	return p.cloudCredentialManager.Inspect(context.Background(), req)
}

func (p *portworx) EnumerateCloudCredential(req *api.CloudCredentialEnumerateRequest) (*api.CloudCredentialEnumerateResponse, error) {
	return p.cloudCredentialManager.Enumerate(context.Background(), req)
}

func (p *portworx) DeleteCloudCredential(req *api.CloudCredentialDeleteRequest) (*api.CloudCredentialDeleteResponse, error) {
	return p.cloudCredentialManager.Delete(context.Background(), req)
}

func (p *portworx) CreateCluster(req *api.ClusterCreateRequest) (*api.ClusterCreateResponse, error) {
	return p.clusterManager.Create(context.Background(), req)
}

func (p *portworx) UpdateCluster(req *api.ClusterUpdateRequest) (*api.ClusterUpdateResponse, error) {
	return p.clusterManager.Update(context.Background(), req)
}

func (p *portworx) InspectCluster(req *api.ClusterInspectRequest) (*api.ClusterInspectResponse, error) {
	return p.clusterManager.Inspect(context.Background(), req)
}

func (p *portworx) EnumerateCluster(req *api.ClusterEnumerateRequest) (*api.ClusterEnumerateResponse, error) {
	return p.clusterManager.Enumerate(context.Background(), req)
}

func (p *portworx) DeleteCluster(req *api.ClusterDeleteRequest) (*api.ClusterDeleteResponse, error) {
	return p.clusterManager.Delete(context.Background(), req)
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

func (p *portworx) CreateBackupLocation(req *api.BackupLocationCreateRequest) (*api.BackupLocationCreateResponse, error) {
	return p.backupLocationManager.Create(context.Background(), req)
}

func (p *portworx) UpdateBackupLocation(req *api.BackupLocationUpdateRequest) (*api.BackupLocationUpdateResponse, error) {
	return p.backupLocationManager.Update(context.Background(), req)
}

func (p *portworx) EnumerateBackupLocation(req *api.BackupLocationEnumerateRequest) (*api.BackupLocationEnumerateResponse, error) {
	return p.backupLocationManager.Enumerate(context.Background(), req)
}

func (p *portworx) InspectBackupLocation(req *api.BackupLocationInspectRequest) (*api.BackupLocationInspectResponse, error) {
	return p.backupLocationManager.Inspect(context.Background(), req)
}

func (p *portworx) DeleteBackupLocation(req *api.BackupLocationDeleteRequest) (*api.BackupLocationDeleteResponse, error) {
	return p.backupLocationManager.Delete(context.Background(), req)
}

func (p *portworx) ValidateBackupLocation(req *api.BackupLocationValidateRequest) (*api.BackupLocationValidateResponse, error) {
	return p.backupLocationManager.Validate(context.Background(), req)
}

// WaitForBackupLocationDeletion waits for backup location to be deleted successfully
// or till timeout is reached. API should poll every `timeBeforeRetry` duration
func (p *portworx) WaitForBackupLocationDeletion(
	ctx context.Context,
	backupLocationName,
	orgID string,
	timeout time.Duration,
	timeBeforeRetry time.Duration,
) error {
	req := &api.BackupLocationInspectRequest{
		Name:  backupLocationName,
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
			logrus.Infof("in invalid state")
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

func (p *portworx) CreateBackup(req *api.BackupCreateRequest) (*api.BackupCreateResponse, error) {
	return p.backupManager.Create(context.Background(), req)
}

func (p *portworx) UpdateBackup(req *api.BackupUpdateRequest) (*api.BackupUpdateResponse, error) {
	return p.backupManager.Update(context.Background(), req)
}

func (p *portworx) EnumerateBackup(req *api.BackupEnumerateRequest) (*api.BackupEnumerateResponse, error) {
	return p.backupManager.Enumerate(context.Background(), req)
}

func (p *portworx) InspectBackup(req *api.BackupInspectRequest) (*api.BackupInspectResponse, error) {
	return p.backupManager.Inspect(context.Background(), req)
}

func (p *portworx) DeleteBackup(req *api.BackupDeleteRequest) (*api.BackupDeleteResponse, error) {
	return p.backupManager.Delete(context.Background(), req)
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

	backupInspectReq := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
	}
	inspectResp, err := p.backupManager.Inspect(ctx, backupInspectReq)
	if err != nil {
		return volumeBackupIDs, err
	}
	backupUUID := inspectResp.GetBackup().GetUid()
	storkApplicationBackupCRName := fmt.Sprintf("%s-%s", backupName, backupUUID[0:7])
	var storkApplicationBackupCR *v1alpha1.ApplicationBackup

	getBackupIDfromStork := func() (interface{}, bool, error) {
		storkApplicationBackupCR, err = storkClient.GetApplicationBackup(storkApplicationBackupCRName, namespace)
		if err != nil {
			logrus.Warnf("failed to get application backup CR [%s], Error:[%v]", storkApplicationBackupCRName, err)
			return false, true, err
		}
		logrus.Debugf("GetVolumeBackupIDs storkApplicationBackupCR: [%+v]\n", storkApplicationBackupCR)
		if len(storkApplicationBackupCR.Status.Volumes) > 0 {
			isComplete := true
			for _, backupVolume := range storkApplicationBackupCR.Status.Volumes {
				logrus.Debugf("Volume [%v] has backup ID: [%v]\n", backupVolume.Volume, backupVolume.BackupID)
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
		logrus.Debugf("For backupVolume [%+v] with Status [%v] while getting backup id: [%+v]", backupVolume, backupVolume.Status, backupVolume.BackupID)
		if backupVolume.Status == "InProgress" && backupVolume.BackupID != "" {
			volumeBackupIDs = append(volumeBackupIDs, backupVolume.BackupID)
		} else {
			logrus.Debugf("Status of backup of volume [+%v] is [%s]. BackupID: [%+v] Reason: [%+v]",
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
	req := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
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

	_, err := task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
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
	req := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
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

	_, err := task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
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
	req := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
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

	_, err := task.DoRetryWithTimeout(f, timeout, timeBeforeRetry)
	if err != nil {
		return fmt.Errorf("failed to transition to delete pending. Error:[%v] Reason:[%v]", err, backupError)
	}

	return nil
}

func (p *portworx) CreateRestore(req *api.RestoreCreateRequest) (*api.RestoreCreateResponse, error) {
	return p.restoreManager.Create(context.Background(), req)
}

func (p *portworx) UpdateRestore(req *api.RestoreUpdateRequest) (*api.RestoreUpdateResponse, error) {
	return p.restoreManager.Update(context.Background(), req)
}

func (p *portworx) EnumerateRestore(req *api.RestoreEnumerateRequest) (*api.RestoreEnumerateResponse, error) {
	return p.restoreManager.Enumerate(context.Background(), req)
}

func (p *portworx) InspectRestore(req *api.RestoreInspectRequest) (*api.RestoreInspectResponse, error) {
	return p.restoreManager.Inspect(context.Background(), req)
}

func (p *portworx) DeleteRestore(req *api.RestoreDeleteRequest) (*api.RestoreDeleteResponse, error) {
	return p.restoreManager.Delete(context.Background(), req)
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

func (p *portworx) CreateSchedulePolicy(req *api.SchedulePolicyCreateRequest) (*api.SchedulePolicyCreateResponse, error) {
	return p.schedulePolicyManager.Create(context.Background(), req)
}

func (p *portworx) UpdateSchedulePolicy(req *api.SchedulePolicyUpdateRequest) (*api.SchedulePolicyUpdateResponse, error) {
	return p.schedulePolicyManager.Update(context.Background(), req)
}

func (p *portworx) EnumerateSchedulePolicy(req *api.SchedulePolicyEnumerateRequest) (*api.SchedulePolicyEnumerateResponse, error) {
	return p.schedulePolicyManager.Enumerate(context.Background(), req)
}

func (p *portworx) InspectSchedulePolicy(req *api.SchedulePolicyInspectRequest) (*api.SchedulePolicyInspectResponse, error) {
	return p.schedulePolicyManager.Inspect(context.Background(), req)
}

func (p *portworx) DeleteSchedulePolicy(req *api.SchedulePolicyDeleteRequest) (*api.SchedulePolicyDeleteResponse, error) {
	return p.schedulePolicyManager.Delete(context.Background(), req)
}

func (p *portworx) CreateBackupSchedule(req *api.BackupScheduleCreateRequest) (*api.BackupScheduleCreateResponse, error) {
	return p.backupScheduleManager.Create(context.Background(), req)
}

func (p *portworx) UpdateBackupSchedule(req *api.BackupScheduleUpdateRequest) (*api.BackupScheduleUpdateResponse, error) {
	return p.backupScheduleManager.Update(context.Background(), req)
}

func (p *portworx) EnumerateBackupSchedule(req *api.BackupScheduleEnumerateRequest) (*api.BackupScheduleEnumerateResponse, error) {
	return p.backupScheduleManager.Enumerate(context.Background(), req)
}

func (p *portworx) InspectBackupSchedule(req *api.BackupScheduleInspectRequest) (*api.BackupScheduleInspectResponse, error) {
	return p.backupScheduleManager.Inspect(context.Background(), req)
}

func (p *portworx) DeleteBackupSchedule(req *api.BackupScheduleDeleteRequest) (*api.BackupScheduleDeleteResponse, error) {
	return p.backupScheduleManager.Delete(context.Background(), req)
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
		backupCrs, err := inst.ListApplicationBackups(namespace)
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
		logrus.Debugf("WaitForBackupRunning inspect backup state for %s", req.Name)
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
		logrus.Debugf("WaitForRestoreRunning inspect backup state for %s", req.Name)
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

func (p *portworx) ActivateLicense(req *api.LicenseActivateRequest) (*api.LicenseActivateResponse, error) {
	return p.licenseManager.Activate(context.Background(), req)
}

func (p *portworx) InspectLicense(req *api.LicenseInspectRequest) (*api.LicenseInspectResponse, error) {
	return p.licenseManager.Inspect(context.Background(), req)
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

func (p *portworx) CreateRule(req *api.RuleCreateRequest) (*api.RuleCreateResponse, error) {
	return p.ruleManager.Create(context.Background(), req)
}

func (p *portworx) UpdateRule(req *api.RuleUpdateRequest) (*api.RuleUpdateResponse, error) {
	return p.ruleManager.Update(context.Background(), req)
}

func (p *portworx) EnumerateRule(req *api.RuleEnumerateRequest) (*api.RuleEnumerateResponse, error) {
	return p.ruleManager.Enumerate(context.Background(), req)
}

func (p *portworx) InspectRule(req *api.RuleInspectRequest) (*api.RuleInspectResponse, error) {
	return p.ruleManager.Inspect(context.Background(), req)
}

func (p *portworx) DeleteRule(req *api.RuleDeleteRequest) (*api.RuleDeleteResponse, error) {
	return p.ruleManager.Delete(context.Background(), req)
}

func init() {
	backup.Register(driverName, &portworx{})
}
