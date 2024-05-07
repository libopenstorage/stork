package portworx

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	DriverName            = "pxb"
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
	clusterManager          api.ClusterClient
	backupLocationManager   api.BackupLocationClient
	cloudCredentialManager  api.CloudCredentialClient
	backupManager           api.BackupClient
	restoreManager          api.RestoreClient
	backupScheduleManager   api.BackupScheduleClient
	schedulePolicyManager   api.SchedulePolicyClient
	organizationManager     api.OrganizationClient
	licenseManager          api.LicenseClient
	healthManager           api.HealthClient
	ruleManager             api.RulesClient
	roleManager             api.RoleClient
	versionManager          api.VersionClient
	activityTimeLineManager api.ActivityTimeLineClient
	metricsManager          api.MetricsClient
	receiverManager         api.ReceiverClient
	recipientManager        api.RecipientClient

	schedulerDriver scheduler.Driver
	nodeDriver      node.Driver
	volumeDriver    volume.Driver
	schedOps        schedops.Driver
	refreshEndpoint bool
	token           string
}

func (p *portworx) String() string {
	return DriverName
}

func GetKubernetesRestConfig(clusterObj *api.ClusterObject) (*rest.Config, error) {
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

// GetKubernetesInstance - Get handler to k8s cluster.
func GetKubernetesInstance(cluster *api.ClusterObject) (core.Ops, stork.Ops, error) {
	client, err := GetKubernetesRestConfig(cluster)
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

	pxbNamespace, err := backup.GetPxBackupNamespace()
	if err != nil {
		return err
	}
	if err = p.setDriver(pxbServiceName, pxbNamespace); err != nil {
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
	p.versionManager = api.NewVersionClient(conn)
	p.activityTimeLineManager = api.NewActivityTimeLineClient(conn)
	p.metricsManager = api.NewMetricsClient(conn)
	p.roleManager = api.NewRoleClient(conn)
	p.receiverManager = api.NewReceiverClient(conn)
	p.recipientManager = api.NewRecipientClient(conn)

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
	reqInterface, err := p.SetMissingCloudCredentialUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithCloudCredentialUID, ok := reqInterface.(*api.CloudCredentialUpdateRequest); ok {
		return p.cloudCredentialManager.Update(ctx, reqWithCloudCredentialUID)
	}
	return nil, fmt.Errorf("expected *api.CloudCredentialUpdateRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) InspectCloudCredential(ctx context.Context, req *api.CloudCredentialInspectRequest) (*api.CloudCredentialInspectResponse, error) {
	reqInterface, err := p.SetMissingCloudCredentialUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithCloudCredentialUID, ok := reqInterface.(*api.CloudCredentialInspectRequest); ok {
		return p.cloudCredentialManager.Inspect(ctx, reqWithCloudCredentialUID)
	}
	return nil, fmt.Errorf("expected *api.CloudCredentialInspectRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) EnumerateCloudCredential(ctx context.Context, req *api.CloudCredentialEnumerateRequest) (*api.CloudCredentialEnumerateResponse, error) {
	return p.cloudCredentialManager.Enumerate(ctx, req)
}

func (p *portworx) EnumerateCloudCredentialByUser(ctx context.Context, req *api.CloudCredentialEnumerateRequest) (*api.CloudCredentialEnumerateResponse, error) {
	cloudCredentialEnumerateResponse, err := p.cloudCredentialManager.Enumerate(ctx, req)
	if err != nil {
		return nil, err
	}
	sub, err := GetSubFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	var userCloudCredentials []*api.CloudCredentialObject
	for _, cloudCredentialObject := range cloudCredentialEnumerateResponse.GetCloudCredentials() {
		ownerId := cloudCredentialObject.GetOwnership().GetOwner()
		if ownerId == sub {
			userCloudCredentials = append(userCloudCredentials, cloudCredentialObject)
		}
	}
	cloudCredentialEnumerateResponse.CloudCredentials = userCloudCredentials
	return cloudCredentialEnumerateResponse, nil
}

func (p *portworx) DeleteCloudCredential(ctx context.Context, req *api.CloudCredentialDeleteRequest) (*api.CloudCredentialDeleteResponse, error) {
	reqInterface, err := p.SetMissingCloudCredentialUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithCloudCredentialUID, ok := reqInterface.(*api.CloudCredentialDeleteRequest); ok {
		return p.cloudCredentialManager.Delete(ctx, reqWithCloudCredentialUID)
	}
	return nil, fmt.Errorf("expected *api.CloudCredentialDeleteRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) UpdateOwnershipCloudCredential(ctx context.Context, req *api.CloudCredentialOwnershipUpdateRequest) (*api.CloudCredentialOwnershipUpdateResponse, error) {
	reqInterface, err := p.SetMissingCloudCredentialUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithCloudCredentialUID, ok := reqInterface.(*api.CloudCredentialOwnershipUpdateRequest); ok {
		return p.cloudCredentialManager.UpdateOwnership(ctx, reqWithCloudCredentialUID)
	}
	return nil, fmt.Errorf("expected *api.CloudCredentialOwnershipUpdateRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) GetCloudCredentialUID(ctx context.Context, orgID string, cloudCredentialName string) (string, error) {
	cloudCredentialEnumerateReq := &api.CloudCredentialEnumerateRequest{
		OrgId: orgID,
	}
	enumerateRsp, err := p.EnumerateCloudCredential(ctx, cloudCredentialEnumerateReq)
	if err != nil {
		return "", err
	}
	for _, cloudCredential := range enumerateRsp.GetCloudCredentials() {
		if cloudCredential.GetName() == cloudCredentialName {
			return cloudCredential.GetUid(), nil
		}
	}
	return "", fmt.Errorf("cloud credential with name '%s' not found for org '%s'", cloudCredentialName, orgID)
}

// SetMissingCloudCredentialUID sets the missing cloud-credential UID for cloud-credential-related requests
func (p *portworx) SetMissingCloudCredentialUID(ctx context.Context, req interface{}) (interface{}, error) {
	switch r := req.(type) {
	case *api.CloudCredentialInspectRequest:
		if r.GetUid() == "" {
			cloudCredentialUid, err := p.GetCloudCredentialUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = cloudCredentialUid
		}
		return r, nil
	case *api.CloudCredentialDeleteRequest:
		if r.GetUid() == "" {
			cloudCredentialUid, err := p.GetCloudCredentialUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = cloudCredentialUid
		}
		return r, nil
	case *api.CloudCredentialUpdateRequest:
		if r.GetUid() == "" {
			cloudCredentialUid, err := p.GetCloudCredentialUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = cloudCredentialUid
		}
		return r, nil
	case *api.CloudCredentialOwnershipUpdateRequest:
		if r.GetUid() == "" {
			cloudCredentialUid, err := p.GetCloudCredentialUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = cloudCredentialUid
		}
		return r, nil
	default:
		return nil, fmt.Errorf("received unsupported request type %T", req)
	}
}

func (p *portworx) CreateCluster(ctx context.Context, req *api.ClusterCreateRequest) (*api.ClusterCreateResponse, error) {
	return p.clusterManager.Create(ctx, req)
}

func (p *portworx) UpdateCluster(ctx context.Context, req *api.ClusterUpdateRequest) (*api.ClusterUpdateResponse, error) {
	reqInterface, err := p.SetMissingClusterUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithClusterUID, ok := reqInterface.(*api.ClusterUpdateRequest); ok {
		return p.clusterManager.Update(ctx, reqWithClusterUID)
	}
	return nil, fmt.Errorf("expected *api.ClusterUpdateRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) InspectCluster(ctx context.Context, req *api.ClusterInspectRequest) (*api.ClusterInspectResponse, error) {
	return p.clusterManager.Inspect(ctx, req)
}

func (p *portworx) EnumerateCluster(ctx context.Context, req *api.ClusterEnumerateRequest) (*api.ClusterEnumerateResponse, error) {
	clusterEnumerateResponse, err := p.clusterManager.Enumerate(ctx, req)
	if err != nil {
		return nil, err
	}
	sub, err := GetSubFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	var userClusters []*api.ClusterObject
	for _, clusterObject := range clusterEnumerateResponse.GetClusters() {
		ownerId := clusterObject.GetOwnership().GetOwner()
		if ownerId == sub {
			userClusters = append(userClusters, clusterObject)
		}
	}
	clusterEnumerateResponse.Clusters = userClusters
	return clusterEnumerateResponse, nil
}

func (p *portworx) EnumerateAllCluster(ctx context.Context, req *api.ClusterEnumerateRequest) (*api.ClusterEnumerateResponse, error) {
	return p.clusterManager.Enumerate(ctx, req)
}

func (p *portworx) DeleteCluster(ctx context.Context, req *api.ClusterDeleteRequest) (*api.ClusterDeleteResponse, error) {
	return p.clusterManager.Delete(ctx, req)
}

func (p *portworx) ClusterUpdateBackupShare(ctx context.Context, req *api.ClusterBackupShareUpdateRequest) (*api.ClusterBackupShareUpdateResponse, error) {
	reqInterface, err := p.SetMissingClusterUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithClusterUID, ok := reqInterface.(*api.ClusterBackupShareUpdateRequest); ok {
		return p.clusterManager.UpdateBackupShare(ctx, reqWithClusterUID)
	}
	return nil, fmt.Errorf("expected *api.ClusterBackupShareUpdateRequest type after filling missing UID, but received %T", reqInterface)
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

// WaitForClusterDeletionWithUID waits for cluster to be deleted successfully
// or till timeout is reached. API should poll every `timeBeforeRetry` duration using cluster uid
func (p *portworx) WaitForClusterDeletionWithUID(
	ctx context.Context,
	clusterName,
	clusterUid,
	orgID string,
	timeout time.Duration,
	timeBeforeRetry time.Duration,
) error {
	req := &api.ClusterInspectRequest{
		Name:  clusterName,
		OrgId: orgID,
		Uid:   clusterUid,
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

func (p *portworx) GetClusterUID(ctx context.Context, orgID string, clusterName string) (string, error) {
	clusterEnumerateReq := &api.ClusterEnumerateRequest{
		OrgId: orgID,
	}
	enumerateRsp, err := p.EnumerateCluster(ctx, clusterEnumerateReq)
	if err != nil {
		return "", err
	}
	for _, cluster := range enumerateRsp.GetClusters() {
		if cluster.GetName() == clusterName {
			return cluster.GetUid(), nil
		}
	}
	return "", fmt.Errorf("cluster with name '%s' not found for org '%s'", clusterName, orgID)
}

func (p *portworx) GetClusterName(ctx context.Context, orgID string, clusterUid string) (string, error) {
	clusterEnumerateReq := &api.ClusterEnumerateRequest{
		OrgId: orgID,
	}
	enumerateRsp, err := p.EnumerateCluster(ctx, clusterEnumerateReq)
	if err != nil {
		return "", err
	}
	for _, cluster := range enumerateRsp.GetClusters() {
		if cluster.GetUid() == clusterUid {
			return cluster.GetName(), nil
		}
	}
	return "", fmt.Errorf("cluster with uid '%s' not found for org '%s'", clusterUid, orgID)
}

func (p *portworx) GetClusterStatus(orgID string, clusterName string, ctx context.Context) (api.ClusterInfo_StatusInfo_Status, error) {
	clusterEnumerateReq := &api.ClusterEnumerateRequest{
		OrgId: orgID,
	}
	enumerateRsp, err := p.EnumerateCluster(ctx, clusterEnumerateReq)
	if err != nil {
		return api.ClusterInfo_StatusInfo_Invalid, err
	}
	for _, cluster := range enumerateRsp.GetClusters() {
		if cluster.GetName() == clusterName {
			return cluster.Status.Status, nil
		}
	}
	return api.ClusterInfo_StatusInfo_Invalid, fmt.Errorf("cluster with name '%s' not found for org '%s'", clusterName, orgID)
}

// SetMissingClusterUID sets the missing cluster UID for cluster-related requests
func (p *portworx) SetMissingClusterUID(ctx context.Context, req interface{}) (interface{}, error) {
	switch r := req.(type) {
	case *api.ClusterInspectRequest:
		if r.GetUid() == "" {
			clusterUid, err := p.GetClusterUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = clusterUid
		}
		return r, nil
	case *api.ClusterDeleteRequest:
		if r.GetUid() == "" {
			clusterUid, err := p.GetClusterUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = clusterUid
		}
		return r, nil
	case *api.ClusterBackupShareUpdateRequest:
		if r.GetUid() == "" {
			clusterUid, err := p.GetClusterUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = clusterUid
		}
		return r, nil
	case *api.ClusterUpdateRequest:
		if r.GetUid() == "" {
			clusterUid, err := p.GetClusterUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = clusterUid
		}
		return r, nil
	default:
		return nil, fmt.Errorf("received unsupported request type %T", req)
	}
}

func (p *portworx) CreateBackupLocation(ctx context.Context, req *api.BackupLocationCreateRequest) (*api.BackupLocationCreateResponse, error) {
	return p.backupLocationManager.Create(ctx, req)
}

func (p *portworx) UpdateBackupLocation(ctx context.Context, req *api.BackupLocationUpdateRequest) (*api.BackupLocationUpdateResponse, error) {
	reqInterface, err := p.SetMissingBackupLocationUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithBackupLocationUID, ok := reqInterface.(*api.BackupLocationUpdateRequest); ok {
		return p.backupLocationManager.Update(ctx, reqWithBackupLocationUID)
	}
	return nil, fmt.Errorf("expected *api.BackupLocationUpdateRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) EnumerateBackupLocation(ctx context.Context, req *api.BackupLocationEnumerateRequest) (*api.BackupLocationEnumerateResponse, error) {
	return p.backupLocationManager.Enumerate(ctx, req)
}

func (p *portworx) EnumerateBackupLocationByUser(ctx context.Context, req *api.BackupLocationEnumerateRequest) (*api.BackupLocationEnumerateResponse, error) {
	backupLocationEnumerateResponse, err := p.backupLocationManager.Enumerate(ctx, req)
	if err != nil {
		return nil, err
	}
	sub, err := GetSubFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	var userBackupLocations []*api.BackupLocationObject
	for _, backupLocationObject := range backupLocationEnumerateResponse.GetBackupLocations() {
		ownerId := backupLocationObject.GetOwnership().GetOwner()
		if ownerId == sub {
			userBackupLocations = append(userBackupLocations, backupLocationObject)
		}
	}
	backupLocationEnumerateResponse.BackupLocations = userBackupLocations
	return backupLocationEnumerateResponse, nil
}

func (p *portworx) InspectBackupLocation(ctx context.Context, req *api.BackupLocationInspectRequest) (*api.BackupLocationInspectResponse, error) {
	reqInterface, err := p.SetMissingBackupLocationUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithBackupLocationUID, ok := reqInterface.(*api.BackupLocationInspectRequest); ok {
		return p.backupLocationManager.Inspect(ctx, reqWithBackupLocationUID)
	}
	return nil, fmt.Errorf("expected *api.BackupLocationInspectRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) DeleteBackupLocation(ctx context.Context, req *api.BackupLocationDeleteRequest) (*api.BackupLocationDeleteResponse, error) {
	reqInterface, err := p.SetMissingBackupLocationUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithBackupLocationUID, ok := reqInterface.(*api.BackupLocationDeleteRequest); ok {
		return p.backupLocationManager.Delete(ctx, reqWithBackupLocationUID)
	}
	return nil, fmt.Errorf("expected *api.BackupLocationDeleteRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) ValidateBackupLocation(ctx context.Context, req *api.BackupLocationValidateRequest) (*api.BackupLocationValidateResponse, error) {
	reqInterface, err := p.SetMissingBackupLocationUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithBackupLocationUID, ok := reqInterface.(*api.BackupLocationValidateRequest); ok {
		return p.backupLocationManager.Validate(ctx, reqWithBackupLocationUID)
	}
	return nil, fmt.Errorf("expected *api.BackupLocationValidateRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) UpdateOwnershipBackupLocation(ctx context.Context, req *api.BackupLocationOwnershipUpdateRequest) (*api.BackupLocationOwnershipUpdateResponse, error) {
	reqInterface, err := p.SetMissingBackupLocationUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithBackupLocationUID, ok := reqInterface.(*api.BackupLocationOwnershipUpdateRequest); ok {
		return p.backupLocationManager.UpdateOwnership(ctx, reqWithBackupLocationUID)
	}
	return nil, fmt.Errorf("expected *api.BackupLocationOwnershipUpdateRequest type after filling missing UID, but received %T", reqInterface)
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

func (p *portworx) GetBackupLocationUID(ctx context.Context, orgID string, backupLocationName string) (string, error) {
	backupLocationEnumerateReq := &api.BackupLocationEnumerateRequest{
		OrgId: orgID,
	}
	enumerateRsp, err := p.EnumerateBackupLocation(ctx, backupLocationEnumerateReq)
	if err != nil {
		return "", err
	}
	for _, backupLocation := range enumerateRsp.GetBackupLocations() {
		if backupLocation.GetName() == backupLocationName {
			return backupLocation.GetUid(), nil
		}
	}
	return "", fmt.Errorf("backup location with name '%s' not found for org '%s'", backupLocationName, orgID)
}

// SetMissingBackupLocationUID sets the missing backup-location UID for backup-location-related requests
func (p *portworx) SetMissingBackupLocationUID(ctx context.Context, req interface{}) (interface{}, error) {
	switch r := req.(type) {
	case *api.BackupLocationInspectRequest:
		if r.GetUid() == "" {
			backupLocationUid, err := p.GetBackupLocationUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = backupLocationUid
		}
		return r, nil
	case *api.BackupLocationUpdateRequest:
		if r.GetUid() == "" {
			backupLocationUid, err := p.GetBackupLocationUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = backupLocationUid
		}
		return r, nil
	case *api.BackupLocationValidateRequest:
		if r.GetUid() == "" {
			backupLocationUid, err := p.GetBackupLocationUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = backupLocationUid
		}
		return r, nil
	case *api.BackupLocationOwnershipUpdateRequest:
		if r.GetUid() == "" {
			backupLocationUid, err := p.GetBackupLocationUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = backupLocationUid
		}
		return r, nil
	case *api.BackupLocationDeleteRequest:
		if r.GetUid() == "" {
			backupLocationUid, err := p.GetBackupLocationUID(ctx, r.GetOrgId(), r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = backupLocationUid
		}
		return r, nil
	default:
		return nil, fmt.Errorf("received unsupported request type %T", req)
	}
}

func (p *portworx) CreateBackup(ctx context.Context, req *api.BackupCreateRequest) (*api.BackupCreateResponse, error) {
	return p.backupManager.Create(ctx, req)
}

func (p *portworx) UpdateBackup(ctx context.Context, req *api.BackupUpdateRequest) (*api.BackupUpdateResponse, error) {
	reqInterface, err := p.SetMissingBackupUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithBackupUID, ok := reqInterface.(*api.BackupUpdateRequest); ok {
		return p.backupManager.Update(ctx, reqWithBackupUID)
	}
	return nil, fmt.Errorf("expected *api.BackupUpdateRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) EnumerateBackup(ctx context.Context, req *api.BackupEnumerateRequest) (*api.BackupEnumerateResponse, error) {
	return p.backupManager.Enumerate(ctx, req)
}

func (p *portworx) EnumerateBackupByUser(ctx context.Context, req *api.BackupEnumerateRequest) (*api.BackupEnumerateResponse, error) {
	backupEnumerateResponse, err := p.backupManager.Enumerate(ctx, req)
	if err != nil {
		return nil, err
	}
	sub, err := GetSubFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	var userBackups []*api.BackupObject
	for _, backupObject := range backupEnumerateResponse.GetBackups() {
		ownerId := backupObject.GetOwnership().GetOwner()
		if ownerId == sub {
			userBackups = append(userBackups, backupObject)
		}
	}
	backupEnumerateResponse.Backups = userBackups
	return backupEnumerateResponse, nil
}

func (p *portworx) InspectBackup(ctx context.Context, req *api.BackupInspectRequest) (*api.BackupInspectResponse, error) {
	reqInterface, err := p.SetMissingBackupUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithBackupUID, ok := reqInterface.(*api.BackupInspectRequest); ok {
		return p.backupManager.Inspect(ctx, reqWithBackupUID)
	}
	return nil, fmt.Errorf("expected *api.BackupInspectRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) DeleteBackup(ctx context.Context, req *api.BackupDeleteRequest) (*api.BackupDeleteResponse, error) {
	reqInterface, err := p.SetMissingBackupUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithBackupUID, ok := reqInterface.(*api.BackupDeleteRequest); ok {
		return p.backupManager.Delete(ctx, reqWithBackupUID)
	}
	return nil, fmt.Errorf("expected *api.BackupDeleteRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) UpdateBackupShare(ctx context.Context, req *api.BackupShareUpdateRequest) (*api.BackupShareUpdateResponse, error) {
	reqInterface, err := p.SetMissingBackupUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithBackupUID, ok := reqInterface.(*api.BackupShareUpdateRequest); ok {
		return p.backupManager.UpdateBackupShare(ctx, reqWithBackupUID)
	}
	return nil, fmt.Errorf("expected *api.BackupShareUpdateRequest type after filling missing UID, but received %T", reqInterface)
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
	_, storkClient, err := GetKubernetesInstance(clusterObj)
	if err != nil {
		return volumeBackupIDs, err
	}

	backupUUID, err := p.GetBackupUID(ctx, backupName, orgID)
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

// WaitForBackupPartialCompletion waits for backup to complete successfully
// or till timeout is reached. API should poll every `timeBeforeRetry` duration
func (p *portworx) WaitForBackupPartialCompletion(
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
		inspectBkpResp, err := p.backupManager.Inspect(ctx, req)
		if err != nil {
			// Error occured, just retry
			return nil, true, err
		}

		// Check if backup status is complete
		currentStatus := inspectBkpResp.GetBackup().GetStatus().GetStatus()
		if currentStatus == api.BackupInfo_StatusInfo_PartialSuccess {
			// If backup is complete, dont retry again
			return nil, false, nil
		} else if currentStatus == api.BackupInfo_StatusInfo_Failed ||
			currentStatus == api.BackupInfo_StatusInfo_Aborted ||
			currentStatus == api.BackupInfo_StatusInfo_Invalid ||
			currentStatus == api.BackupInfo_StatusInfo_Success {
			backupError = fmt.Errorf("backup [%v] is in [%s] state. reason: [%v]",
				req.GetName(), currentStatus,
				inspectBkpResp.GetBackup().GetStatus().GetReason())
			return nil, false, backupError
		}
		return nil,
			true,
			fmt.Errorf("backup [%v] is in [%s] state. Waiting to become Partial Complete",
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

// SetMissingBackupUID sets the missing backup UID for backup-related requests
func (p *portworx) SetMissingBackupUID(ctx context.Context, req interface{}) (interface{}, error) {
	switch r := req.(type) {
	case *api.BackupInspectRequest:
		if r.GetUid() == "" {
			backupUid, err := p.GetBackupUID(ctx, r.GetName(), r.GetOrgId())
			if err != nil {
				return nil, err
			}
			r.Uid = backupUid
		}
		return r, nil
	case *api.BackupDeleteRequest:
		if r.GetUid() == "" {
			backupUid, err := p.GetBackupUID(ctx, r.GetName(), r.GetOrgId())
			if err != nil {
				return nil, err
			}
			r.Uid = backupUid
		}
		return r, nil
	case *api.BackupUpdateRequest:
		if r.GetUid() == "" {
			backupUid, err := p.GetBackupUID(ctx, r.GetName(), r.GetOrgId())
			if err != nil {
				return nil, err
			}
			r.Uid = backupUid
		}
		return r, nil
	case *api.BackupShareUpdateRequest:
		if r.GetUid() == "" {
			backupUid, err := p.GetBackupUID(ctx, r.GetName(), r.GetOrgId())
			if err != nil {
				return nil, err
			}
			r.Uid = backupUid
		}
		return r, nil
	default:
		return nil, fmt.Errorf("received unsupported request type %T", req)
	}
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
	reqInterface, err := p.SetMissingRestoreUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithRestoreUID, ok := reqInterface.(*api.RestoreUpdateRequest); ok {
		return p.restoreManager.Update(ctx, reqWithRestoreUID)
	}
	return nil, fmt.Errorf("expected *api.RestoreUpdateRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) EnumerateRestore(ctx context.Context, req *api.RestoreEnumerateRequest) (*api.RestoreEnumerateResponse, error) {
	return p.restoreManager.Enumerate(ctx, req)
}

func (p *portworx) EnumerateRestoreByUser(ctx context.Context, req *api.RestoreEnumerateRequest) (*api.RestoreEnumerateResponse, error) {
	restoreEnumerateResponse, err := p.restoreManager.Enumerate(ctx, req)
	if err != nil {
		return nil, err
	}
	sub, err := GetSubFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	var userRestores []*api.RestoreObject
	for _, backupObject := range restoreEnumerateResponse.GetRestores() {
		ownerId := backupObject.GetOwnership().GetOwner()
		if ownerId == sub {
			userRestores = append(userRestores, backupObject)
		}
	}
	restoreEnumerateResponse.Restores = userRestores
	return restoreEnumerateResponse, nil
}

func (p *portworx) InspectRestore(ctx context.Context, req *api.RestoreInspectRequest) (*api.RestoreInspectResponse, error) {
	reqInterface, err := p.SetMissingRestoreUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithRestoreUID, ok := reqInterface.(*api.RestoreInspectRequest); ok {
		return p.restoreManager.Inspect(ctx, reqWithRestoreUID)
	}
	return nil, fmt.Errorf("expected *api.RestoreInspectRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) DeleteRestore(ctx context.Context, req *api.RestoreDeleteRequest) (*api.RestoreDeleteResponse, error) {
	reqInterface, err := p.SetMissingRestoreUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithRestoreUID, ok := reqInterface.(*api.RestoreDeleteRequest); ok {
		return p.restoreManager.Delete(ctx, reqWithRestoreUID)
	}
	return nil, fmt.Errorf("expected *api.RestoreDeleteRequest type after filling missing UID, but received %T", reqInterface)
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

func (p *portworx) GetRestoreUID(ctx context.Context, restoreName string, orgID string) (string, error) {
	var totalRestores int
	restoreEnumerateRequest := &api.RestoreEnumerateRequest{OrgId: orgID}
	restoreEnumerateRequest.EnumerateOptions = &api.EnumerateOptions{MaxObjects: uint64(enumerateBatchSize), ObjectIndex: 0}
	for {
		restoreEnumerateResponse, err := p.EnumerateRestore(ctx, restoreEnumerateRequest)
		if err != nil {
			log.InfoD("Restore enumeration for the ctx [%v] within org [%s] failed with error [%v]. Restore enumerate request: [%v].", ctx, orgID, err, restoreEnumerateRequest)
			return "", err
		}
		for _, restore := range restoreEnumerateResponse.GetRestores() {
			if restore.GetName() == restoreName {
				return restore.GetUid(), nil
			}
			totalRestores++
		}
		if uint64(totalRestores) >= restoreEnumerateResponse.GetTotalCount() {
			break
		} else {
			restoreEnumerateRequest.EnumerateOptions.ObjectIndex += uint64(len(restoreEnumerateResponse.GetRestores()))
		}
	}

	return "", fmt.Errorf("restore with name '%s' not found for org '%s'", restoreName, orgID)
}

// SetMissingRestoreUID sets the missing restore UID for restore-related requests
func (p *portworx) SetMissingRestoreUID(ctx context.Context, req interface{}) (interface{}, error) {
	switch r := req.(type) {
	case *api.RestoreInspectRequest:
		if r.GetUid() == "" {
			restoreUid, err := p.GetRestoreUID(ctx, r.GetName(), r.GetOrgId())
			if err != nil {
				return nil, err
			}
			r.Uid = restoreUid
		}
		return r, nil
	case *api.RestoreDeleteRequest:
		if r.GetUid() == "" {
			restoreUid, err := p.GetRestoreUID(ctx, r.GetName(), r.GetOrgId())
			if err != nil {
				return nil, err
			}
			r.Uid = restoreUid
		}
		return r, nil
	case *api.RestoreUpdateRequest:
		if r.GetUid() == "" {
			restoreUid, err := p.GetRestoreUID(ctx, r.GetName(), r.GetOrgId())
			if err != nil {
				return nil, err
			}
			r.Uid = restoreUid
		}
		return r, nil
	default:
		return nil, fmt.Errorf("received unsupported request type %T", req)
	}
}

func (p *portworx) CreateSchedulePolicy(ctx context.Context, req *api.SchedulePolicyCreateRequest) (*api.SchedulePolicyCreateResponse, error) {
	return p.schedulePolicyManager.Create(ctx, req)
}

func (p *portworx) UpdateSchedulePolicy(ctx context.Context, req *api.SchedulePolicyUpdateRequest) (*api.SchedulePolicyUpdateResponse, error) {
	reqInterface, err := p.SetMissingSchedulePolicyUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithSchedulePolicyUID, ok := reqInterface.(*api.SchedulePolicyUpdateRequest); ok {
		return p.schedulePolicyManager.Update(ctx, reqWithSchedulePolicyUID)
	}
	return nil, fmt.Errorf("expected *api.SchedulePolicyUpdateRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) EnumerateSchedulePolicy(ctx context.Context, req *api.SchedulePolicyEnumerateRequest) (*api.SchedulePolicyEnumerateResponse, error) {
	return p.schedulePolicyManager.Enumerate(ctx, req)
}

func (p *portworx) EnumerateSchedulePolicyByUser(ctx context.Context, req *api.SchedulePolicyEnumerateRequest) (*api.SchedulePolicyEnumerateResponse, error) {
	schedulePolicyEnumerateResponse, err := p.schedulePolicyManager.Enumerate(ctx, req)
	if err != nil {
		return nil, err
	}
	sub, err := GetSubFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	var userSchedulePolicies []*api.SchedulePolicyObject
	for _, clusterObject := range schedulePolicyEnumerateResponse.GetSchedulePolicies() {
		ownerId := clusterObject.GetOwnership().GetOwner()
		if ownerId == sub {
			userSchedulePolicies = append(userSchedulePolicies, clusterObject)
		}
	}
	schedulePolicyEnumerateResponse.SchedulePolicies = userSchedulePolicies
	return schedulePolicyEnumerateResponse, nil
}

func (p *portworx) InspectSchedulePolicy(ctx context.Context, req *api.SchedulePolicyInspectRequest) (*api.SchedulePolicyInspectResponse, error) {
	reqInterface, err := p.SetMissingSchedulePolicyUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithSchedulePolicyUID, ok := reqInterface.(*api.SchedulePolicyInspectRequest); ok {
		return p.schedulePolicyManager.Inspect(ctx, reqWithSchedulePolicyUID)
	}
	return nil, fmt.Errorf("expected *api.SchedulePolicyInspectRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) DeleteSchedulePolicy(ctx context.Context, req *api.SchedulePolicyDeleteRequest) (*api.SchedulePolicyDeleteResponse, error) {
	reqInterface, err := p.SetMissingSchedulePolicyUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithSchedulePolicyUID, ok := reqInterface.(*api.SchedulePolicyDeleteRequest); ok {
		return p.schedulePolicyManager.Delete(ctx, reqWithSchedulePolicyUID)
	}
	return nil, fmt.Errorf("expected *api.SchedulePolicyDeleteRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) UpdateOwnershipSchedulePolicy(ctx context.Context, req *api.SchedulePolicyOwnershipUpdateRequest) (*api.SchedulePolicyOwnershipUpdateResponse, error) {
	reqInterface, err := p.SetMissingSchedulePolicyUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithSchedulePolicyUID, ok := reqInterface.(*api.SchedulePolicyOwnershipUpdateRequest); ok {
		return p.schedulePolicyManager.UpdateOwnership(ctx, reqWithSchedulePolicyUID)
	}
	return nil, fmt.Errorf("expected *api.SchedulePolicyOwnershipUpdateRequest type after filling missing UID, but received %T", reqInterface)
}

// SetMissingSchedulePolicyUID sets the missing schedule-policy UID for schedule-policy-related requests
func (p *portworx) SetMissingSchedulePolicyUID(ctx context.Context, req interface{}) (interface{}, error) {
	switch r := req.(type) {
	case *api.SchedulePolicyInspectRequest:
		if r.GetUid() == "" {
			backupPolicyUid, err := p.GetSchedulePolicyUid(r.GetOrgId(), ctx, r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = backupPolicyUid
		}
		return r, nil
	case *api.SchedulePolicyDeleteRequest:
		if r.GetUid() == "" {
			backupPolicyUid, err := p.GetSchedulePolicyUid(r.GetOrgId(), ctx, r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = backupPolicyUid
		}
		return r, nil
	case *api.SchedulePolicyUpdateRequest:
		if r.GetUid() == "" {
			backupPolicyUid, err := p.GetSchedulePolicyUid(r.GetOrgId(), ctx, r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = backupPolicyUid
		}
		return r, nil
	case *api.SchedulePolicyOwnershipUpdateRequest:
		if r.GetUid() == "" {
			backupPolicyUid, err := p.GetSchedulePolicyUid(r.GetOrgId(), ctx, r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = backupPolicyUid
		}
		return r, nil
	default:
		return nil, fmt.Errorf("received unsupported request type %T", req)
	}
}

func (p *portworx) CreateBackupSchedule(ctx context.Context, req *api.BackupScheduleCreateRequest) (*api.BackupScheduleCreateResponse, error) {
	return p.backupScheduleManager.Create(ctx, req)
}

func (p *portworx) UpdateBackupSchedule(ctx context.Context, req *api.BackupScheduleUpdateRequest) (*api.BackupScheduleUpdateResponse, error) {
	reqInterface, err := p.SetMissingBackupScheduleUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithBackupScheduleUID, ok := reqInterface.(*api.BackupScheduleUpdateRequest); ok {
		return p.backupScheduleManager.Update(ctx, reqWithBackupScheduleUID)
	}
	return nil, fmt.Errorf("expected *api.BackupScheduleUpdateRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) EnumerateBackupSchedule(ctx context.Context, req *api.BackupScheduleEnumerateRequest) (*api.BackupScheduleEnumerateResponse, error) {
	return p.backupScheduleManager.Enumerate(ctx, req)
}

func (p *portworx) EnumerateBackupScheduleByUser(ctx context.Context, req *api.BackupScheduleEnumerateRequest) (*api.BackupScheduleEnumerateResponse, error) {
	backupScheduleEnumerateResponse, err := p.backupScheduleManager.Enumerate(ctx, req)
	if err != nil {
		return nil, err
	}
	sub, err := GetSubFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	var userBackupSchedules []*api.BackupScheduleObject
	for _, backupObject := range backupScheduleEnumerateResponse.GetBackupSchedules() {
		ownerId := backupObject.GetOwnership().GetOwner()
		if ownerId == sub {
			userBackupSchedules = append(userBackupSchedules, backupObject)
		}
	}
	backupScheduleEnumerateResponse.BackupSchedules = userBackupSchedules
	return backupScheduleEnumerateResponse, nil
}

func (p *portworx) InspectBackupSchedule(ctx context.Context, req *api.BackupScheduleInspectRequest) (*api.BackupScheduleInspectResponse, error) {
	reqInterface, err := p.SetMissingBackupScheduleUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithBackupScheduleUID, ok := reqInterface.(*api.BackupScheduleInspectRequest); ok {
		return p.backupScheduleManager.Inspect(ctx, reqWithBackupScheduleUID)
	}
	return nil, fmt.Errorf("expected *api.BackupScheduleInspectRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) DeleteBackupSchedule(ctx context.Context, req *api.BackupScheduleDeleteRequest) (*api.BackupScheduleDeleteResponse, error) {
	reqInterface, err := p.SetMissingBackupScheduleUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithBackupScheduleUID, ok := reqInterface.(*api.BackupScheduleDeleteRequest); ok {
		return p.backupScheduleManager.Delete(ctx, reqWithBackupScheduleUID)
	}
	return nil, fmt.Errorf("expected *api.BackupScheduleDeleteRequest type after filling missing UID, but received %T", reqInterface)
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
		_, inst, err := GetKubernetesInstance(clusterObj)
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

func (p *portworx) GetBackupScheduleUID(ctx context.Context, scheduleName string, orgID string) (string, error) {
	var totalBackupSchedules int
	backupScheduleEnumerateRequest := &api.BackupScheduleEnumerateRequest{OrgId: orgID}
	backupScheduleEnumerateRequest.EnumerateOptions = &api.EnumerateOptions{MaxObjects: uint64(enumerateBatchSize), ObjectIndex: 0}
	for {
		backupScheduleEnumerateResponse, err := p.EnumerateBackupSchedule(ctx, backupScheduleEnumerateRequest)
		if err != nil {
			log.InfoD("BackupSchedule enumeration for the ctx [%v] within org [%s] failed with error [%v]. BackupSchedule enumerate request: [%v].", ctx, orgID, err, backupScheduleEnumerateRequest)
			return "", err
		}
		for _, backupSchedule := range backupScheduleEnumerateResponse.GetBackupSchedules() {
			if backupSchedule.GetName() == scheduleName {
				return backupSchedule.GetUid(), nil
			}
			totalBackupSchedules++
		}
		if uint64(totalBackupSchedules) >= backupScheduleEnumerateResponse.GetTotalCount() {
			break
		} else {
			backupScheduleEnumerateRequest.EnumerateOptions.ObjectIndex += uint64(len(backupScheduleEnumerateResponse.GetBackupSchedules()))
		}
	}

	return "", fmt.Errorf("backup schedule with name '%s' not found for org '%s'", scheduleName, orgID)
}

// SetMissingBackupScheduleUID sets the missing backup-schedule UID for backup-schedule-related requests
func (p *portworx) SetMissingBackupScheduleUID(ctx context.Context, req interface{}) (interface{}, error) {
	switch r := req.(type) {
	case *api.BackupScheduleInspectRequest:
		if r.GetUid() == "" {
			backupScheduleUid, err := p.GetBackupScheduleUID(ctx, r.GetName(), r.GetOrgId())
			if err != nil {
				return nil, err
			}
			r.Uid = backupScheduleUid
		}
		return r, nil
	case *api.BackupScheduleDeleteRequest:
		if r.GetUid() == "" {
			backupScheduleUid, err := p.GetBackupScheduleUID(ctx, r.GetName(), r.GetOrgId())
			if err != nil {
				return nil, err
			}
			r.Uid = backupScheduleUid
		}
		return r, nil
	case *api.BackupScheduleUpdateRequest:
		if r.GetUid() == "" {
			backupScheduleUid, err := p.GetBackupScheduleUID(ctx, r.GetName(), r.GetOrgId())
			if err != nil {
				return nil, err
			}
			r.Uid = backupScheduleUid
		}
		return r, nil
	default:
		return nil, fmt.Errorf("received unsupported request type %T", req)
	}
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
	reqInterface, err := p.SetMissingRuleUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithRuleUID, ok := reqInterface.(*api.RuleUpdateRequest); ok {
		return p.ruleManager.Update(ctx, reqWithRuleUID)
	}
	return nil, fmt.Errorf("expected *api.RuleUpdateRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) EnumerateRule(ctx context.Context, req *api.RuleEnumerateRequest) (*api.RuleEnumerateResponse, error) {
	return p.ruleManager.Enumerate(ctx, req)
}

func (p *portworx) EnumerateRuleByUser(ctx context.Context, req *api.RuleEnumerateRequest) (*api.RuleEnumerateResponse, error) {
	ruleEnumerateResponse, err := p.ruleManager.Enumerate(ctx, req)
	if err != nil {
		return nil, err
	}
	sub, err := GetSubFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	var userRules []*api.RuleObject
	for _, ruleObject := range ruleEnumerateResponse.GetRules() {
		ownerId := ruleObject.GetOwnership().GetOwner()
		if ownerId == sub {
			userRules = append(userRules, ruleObject)
		}
	}
	ruleEnumerateResponse.Rules = userRules
	return ruleEnumerateResponse, nil
}

func (p *portworx) InspectRule(ctx context.Context, req *api.RuleInspectRequest) (*api.RuleInspectResponse, error) {
	reqInterface, err := p.SetMissingRuleUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithRuleUID, ok := reqInterface.(*api.RuleInspectRequest); ok {
		return p.ruleManager.Inspect(ctx, reqWithRuleUID)
	}
	return nil, fmt.Errorf("expected *api.RuleInspectRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) DeleteRule(ctx context.Context, req *api.RuleDeleteRequest) (*api.RuleDeleteResponse, error) {
	reqInterface, err := p.SetMissingRuleUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithRuleUID, ok := reqInterface.(*api.RuleDeleteRequest); ok {
		return p.ruleManager.Delete(ctx, reqWithRuleUID)
	}
	return nil, fmt.Errorf("expected *api.RuleDeleteRequest type after filling missing UID, but received %T", reqInterface)
}

func (p *portworx) UpdateOwnershipRule(ctx context.Context, req *api.RuleOwnershipUpdateRequest) (*api.RuleOwnershipUpdateResponse, error) {
	reqInterface, err := p.SetMissingRuleUID(ctx, req)
	if err != nil {
		return nil, err
	}
	if reqWithRuleUID, ok := reqInterface.(*api.RuleOwnershipUpdateRequest); ok {
		return p.ruleManager.UpdateOwnership(ctx, reqWithRuleUID)
	}
	return nil, fmt.Errorf("expected *api.RuleOwnershipUpdateRequest type after filling missing UID, but received %T", reqInterface)
}

// SetMissingRuleUID sets the missing rule UID for rule-related requests
func (p *portworx) SetMissingRuleUID(ctx context.Context, req interface{}) (interface{}, error) {
	switch r := req.(type) {
	case *api.RuleInspectRequest:
		if r.GetUid() == "" {
			ruleUid, err := p.GetRuleUid(r.GetOrgId(), ctx, r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = ruleUid
		}
		return r, nil
	case *api.RuleDeleteRequest:
		if r.GetUid() == "" {
			ruleUid, err := p.GetRuleUid(r.GetOrgId(), ctx, r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = ruleUid
		}
		return r, nil
	case *api.RuleUpdateRequest:
		if r.GetUid() == "" {
			ruleUid, err := p.GetRuleUid(r.GetOrgId(), ctx, r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = ruleUid
		}
		return r, nil
	case *api.RuleOwnershipUpdateRequest:
		if r.GetUid() == "" {
			ruleUid, err := p.GetRuleUid(r.GetOrgId(), ctx, r.GetName())
			if err != nil {
				return nil, err
			}
			r.Uid = ruleUid
		}
		return r, nil
	default:
		return nil, fmt.Errorf("received unsupported request type %T", req)
	}
}

func (p *portworx) GetPxBackupVersion(ctx context.Context, req *api.VersionGetRequest) (*api.VersionGetResponse, error) {
	return p.versionManager.Get(ctx, req)
}

func (p *portworx) GetBackupUID(ctx context.Context, backupName string, orgID string) (string, error) {
	var totalBackups int
	bkpEnumerateReq := &api.BackupEnumerateRequest{OrgId: orgID}
	bkpEnumerateReq.EnumerateOptions = &api.EnumerateOptions{MaxObjects: uint64(enumerateBatchSize), ObjectIndex: 0}
	for {
		enumerateRsp, err := p.EnumerateBackup(ctx, bkpEnumerateReq)
		if err != nil {
			log.InfoD("Backup enumeration for the ctx [%v] within org [%s] failed with error [%v]. Backup enumerate request: [%v].", ctx, orgID, err, bkpEnumerateReq)
			return "", err
		}
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

	return "", fmt.Errorf("backup with name '%s' not found for org '%s'", backupName, orgID)
}

func (p *portworx) GetBackupName(ctx context.Context, backupUid string, orgID string) (string, error) {
	var totalBackups int
	bkpEnumerateReq := &api.BackupEnumerateRequest{OrgId: orgID}
	bkpEnumerateReq.EnumerateOptions = &api.EnumerateOptions{MaxObjects: uint64(enumerateBatchSize), ObjectIndex: 0}
	for {
		enumerateRsp, err := p.EnumerateBackup(ctx, bkpEnumerateReq)
		if err != nil {
			log.InfoD("Backup enumeration for the ctx [%v] within org [%s] failed with error [%v]. Backup enumerate request: [%v].", ctx, orgID, err, bkpEnumerateReq)
			return "", err
		}
		for _, backup := range enumerateRsp.GetBackups() {
			if backup.GetUid() == backupUid {
				return backup.GetName(), nil
			}
			totalBackups++
		}
		if uint64(totalBackups) >= enumerateRsp.GetTotalCount() {
			break
		} else {
			bkpEnumerateReq.EnumerateOptions.ObjectIndex += uint64(len(enumerateRsp.GetBackups()))
		}
	}

	return "", fmt.Errorf("backup with uid '%s' not found for org '%s'", backupUid, orgID)
}

// func GetBackupStatusWithReason return backup status and reason for a given backup name.
func (p *portworx) GetBackupStatusWithReason(backupName string, ctx context.Context, orgID string) (api.BackupInfo_StatusInfo_Status, string, error) {
	backupUid, err := p.GetBackupUID(ctx, backupName, orgID)
	if err != nil {
		return 0, "", err
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   backupUid,
		OrgId: orgID,
	}
	resp, err := p.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return 0, "", err
	}
	status := resp.GetBackup().GetStatus().Status
	reason := resp.GetBackup().GetStatus().Reason
	return status, reason, nil
}

func (p *portworx) GetAllScheduleBackupNames(ctx context.Context, scheduleName string, orgID string) ([]string, error) {
	var scheduleBackupNames []string
	backupScheduleInspectRequest := &api.BackupScheduleInspectRequest{
		OrgId: orgID,
		Name:  scheduleName,
		Uid:   "",
	}
	resp, err := p.InspectBackupSchedule(ctx, backupScheduleInspectRequest)
	if err != nil {
		return scheduleBackupNames, err
	}
	scheduleBackups := resp.GetBackupSchedule().GetBackupStatus()["interval"].GetStatus()
	for _, scheduleBackup := range scheduleBackups {
		scheduleBackupNames = append(scheduleBackupNames, scheduleBackup.GetBackupName())
	}
	return scheduleBackupNames, nil
}

func (p *portworx) GetAllScheduleBackupUIDs(ctx context.Context, scheduleName string, orgID string) ([]string, error) {
	var scheduleBackupUIDs []string
	scheduleBackupNames, err := p.GetAllScheduleBackupNames(ctx, scheduleName, orgID)
	if err != nil {
		return scheduleBackupUIDs, err
	}
	for _, scheduleBackupName := range scheduleBackupNames {
		scheduleBackupUID, err := p.GetBackupUID(ctx, scheduleBackupName, orgID)
		if err != nil {
			return scheduleBackupUIDs, err
		}
		scheduleBackupUIDs = append(scheduleBackupUIDs, scheduleBackupUID)
	}
	return scheduleBackupUIDs, nil
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
		"mysql-backup": {"pre": {"pre_action_list": {"mysql --user=root --password=$MYSQL_ROOT_PASSWORD -Bse 'FLUSH TABLES WITH READ LOCK;system ${WAIT_CMD};'"},
			"background":        {"true"},
			"runInSinglePod":    {"false"},
			"pod_selector_list": {"app=mysql"},
			"container":         {"", ""},
		},
			"post": {"post_action_list": {"mysql --user=root --password=$MYSQL_ROOT_PASSWORD -Bse 'FLUSH LOGS; UNLOCK TABLES;'"},
				"background":        {"false"},
				"pod_selector_list": {"app=mysql"},
				"runInSinglePod":    {"false"},
				"container":         {"", ""},
			},
		},
	}
)

var (
	// KubeVirtRule template contains the template for pre, post rules
	// and pod selector to be used for kubevirt backup
	KubevirtRuleSpec = map[string]map[string]string{
		"default": {
			"pre":            "/usr/bin/virt-freezer --freeze --name <vm-name> --namespace <namespace>",
			"post":           "/usr/bin/virt-freezer --unfreeze --name <vm-name> --namespace <namespace>",
			"podSelector":    "vm.kubevirt.io/name=<vm-name>",
			"container":      "",
			"runInSinglePod": "false",
			"background":     "false",
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
	return "", fmt.Errorf("unable to find uid for rule [%s]", ruleName)
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

// GetTokenClaimsFromCtx returns JWT claims from the outgoing metadata of the given context
func GetTokenClaimsFromCtx(ctx context.Context) (jwt.MapClaims, error) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no metadata found in context")
	}
	authHeaders, ok := md[backup.AuthHeader]
	if !ok || len(authHeaders) == 0 {
		return nil, fmt.Errorf("no authorization header found")
	}
	tokenString := strings.TrimPrefix(authHeaders[0], "bearer ")
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return nil, err
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("invalid JWT claims")
	}
	return claims, nil
}

// GetPreferredUsernameFromCtx extracts and decodes the JWT token from the outgoing context and then returns the preferred username
func GetPreferredUsernameFromCtx(ctx context.Context) (string, error) {
	claims, err := GetTokenClaimsFromCtx(ctx)
	if err != nil {
		return "", err
	}
	preferredUsername, ok := claims["preferred_username"].(string)
	if !ok {
		return "", fmt.Errorf("preferred_username not found or is of invalid type")
	}
	return preferredUsername, nil
}

// GetSubFromCtx extracts and decodes the JWT token from the outgoing context and then returns the sub which corresponds to Keycloak user id
func GetSubFromCtx(ctx context.Context) (string, error) {
	claims, err := GetTokenClaimsFromCtx(ctx)
	if err != nil {
		return "", err
	}
	sub, ok := claims["sub"].(string)
	if !ok {
		return "", fmt.Errorf("sub not found or is of invalid type")
	}
	return sub, nil
}

// GetGroupsFromCtx extracts and decodes the JWT token from the outgoing context and then returns the groups
func GetGroupsFromCtx(ctx context.Context) ([]string, error) {
	claims, err := GetTokenClaimsFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	groups := make([]string, 0)
	groupClaims, ok := claims["groups"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("groups not found or is of invalid type")
	}
	for _, groupClaim := range groupClaims {
		groups = append(groups, groupClaim.(string))
	}
	return groups, nil
}

// IsAdminCtx checks if the given ctx is associated with any user in px-admin-group
func IsAdminCtx(ctx context.Context) (bool, error) {
	ctxGroups, err := GetGroupsFromCtx(ctx)
	if err != nil {
		return false, err
	}
	found := false
	for _, group := range ctxGroups {
		if group == "/px-admin-group" {
			found = true
		}
	}
	if found {
		return true, nil
	}
	return false, nil
}

func (p *portworx) EnumerateActivityTimeLine(ctx context.Context, req *api.ActivityEnumerateRequest) (*api.ActivityEnumerateResponse, error) {
	return p.activityTimeLineManager.Enumerate(ctx, req)
}

// GetAllSchedulePolicies gets the all SchedulePolicys names for the given org
func (p *portworx) GetAllSchedulePolicies(ctx context.Context, orgID string) ([]string, error) {
	var schedulePolicyNames []string
	schedulePolicyRequest := &api.SchedulePolicyEnumerateRequest{
		OrgId: orgID,
	}
	resp, err := p.EnumerateSchedulePolicy(ctx, schedulePolicyRequest)
	if err != nil {
		return schedulePolicyNames, err
	}
	schedulePolicys := resp.GetSchedulePolicies()
	for _, schedulePolicy := range schedulePolicys {
		schedulePolicyNames = append(schedulePolicyNames, schedulePolicy.Name)
	}
	return schedulePolicyNames, nil
}

// GetAllRules gets the all rule names for the given org
func (p *portworx) GetAllRules(ctx context.Context, orgID string) ([]string, error) {
	var ruleNames []string
	rulesEnumerateRequest := &api.RuleEnumerateRequest{
		OrgId: orgID,
	}
	resp, err := p.EnumerateRule(ctx, rulesEnumerateRequest)
	if err != nil {
		return ruleNames, err
	}
	Rules := resp.GetRules()
	for _, rule := range Rules {
		ruleNames = append(ruleNames, rule.Name)
	}
	return ruleNames, nil
}

// InspectMetrics gets the metrics data for the given org
func (p *portworx) InspectMetrics(ctx context.Context, req *api.MetricsInspectRequest) (*api.MetricsInspectResponse, error) {
	return p.metricsManager.Inspect(ctx, req)
}

func (p *portworx) CreateRole(ctx context.Context, req *api.RoleCreateRequest) (*api.RoleCreateResponse, error) {
	return p.roleManager.Create(ctx, req)
}

func (p *portworx) DeleteRole(ctx context.Context, req *api.RoleDeleteRequest) (*api.RoleDeleteResponse, error) {
	return p.roleManager.Delete(ctx, req)
}

func (p *portworx) EnumerateRole(ctx context.Context, req *api.RoleEnumerateRequest) (*api.RoleEnumerateResponse, error) {
	return p.roleManager.Enumerate(ctx, req)
}

func (p *portworx) UpdateRole(ctx context.Context, req *api.RoleUpdateRequest) (*api.RoleUpdateResponse, error) {
	return p.roleManager.Update(ctx, req)
}

func (p *portworx) InspectRole(ctx context.Context, req *api.RoleInspectRequest) (*api.RoleInspectResponse, error) {
	return p.roleManager.Inspect(ctx, req)
}

// CreateReceiver creates receiver object
func (p *portworx) CreateReceiver(ctx context.Context, req *api.ReceiverCreateRequest) (*api.ReceiverCreateResponse, error) {
	return p.receiverManager.Create(ctx, req)
}

// DeleteReceiver deletes receiver object
func (p *portworx) DeleteReceiver(ctx context.Context, req *api.ReceiverDeleteRequest) (*api.ReceiverDeleteResponse, error) {
	return p.receiverManager.Delete(ctx, req)
}

// EnumerateReceiver enumerates receiver object
func (p *portworx) EnumerateReceiver(ctx context.Context, req *api.ReceiverEnumerateRequest) (*api.ReceiverEnumerateResponse, error) {
	return p.receiverManager.Enumerate(ctx, req)
}

// UpdateReceiver updates receiver object
func (p *portworx) UpdateReceiver(ctx context.Context, req *api.ReceiverUpdateRequest) (*api.ReceiverUpdateResponse, error) {
	return p.receiverManager.Update(ctx, req)
}

// InspectReceiver inspects receiver object
func (p *portworx) InspectReceiver(ctx context.Context, req *api.ReceiverInspectRequest) (*api.ReceiverInspectResponse, error) {
	return p.receiverManager.Inspect(ctx, req)
}

// ValidateReceiver validates receiver object
func (p *portworx) ValidateReceiver(ctx context.Context, req *api.ReceiverValidateSMTPRequest) (*api.ReceiverValidateSMTPResponse, error) {
	return p.receiverManager.ValidateSMTP(ctx, req)
}

// CreateRecipient creates recipient object
func (p *portworx) CreateRecipient(ctx context.Context, req *api.RecipientCreateRequest) (*api.RecipientCreateResponse, error) {
	return p.recipientManager.Create(ctx, req)
}

// DeleteRecipient deletes recipient object
func (p *portworx) DeleteRecipient(ctx context.Context, req *api.RecipientDeleteRequest) (*api.RecipientDeleteResponse, error) {
	return p.recipientManager.Delete(ctx, req)
}

// EnumerateRecipient enumerates recipient object
func (p *portworx) EnumerateRecipient(ctx context.Context, req *api.RecipientEnumerateRequest) (*api.RecipientEnumerateResponse, error) {
	return p.recipientManager.Enumerate(ctx, req)
}

// UpdateRecipient updates recipient object
func (p *portworx) UpdateRecipient(ctx context.Context, req *api.RecipientUpdateRequest) (*api.RecipientUpdateResponse, error) {
	return p.recipientManager.Update(ctx, req)
}

// InspectRecipient inspects recipient object
func (p *portworx) InspectRecipient(ctx context.Context, req *api.RecipientInspectRequest) (*api.RecipientInspectResponse, error) {
	return p.recipientManager.Inspect(ctx, req)
}

func init() {
	backup.Register(DriverName, &portworx{})
}
