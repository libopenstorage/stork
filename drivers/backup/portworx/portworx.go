package portworx

import (
	"context"
	"fmt"

	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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
	healthManager          api.HealthClient

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

func (p *portworx) InspectCluster(req *api.ClusterInspectRequest) (*api.ClusterInspectResponse, error) {
	return p.clusterManager.Inspect(context.Background(), req)
}

func (p *portworx) EnumerateCluster(req *api.ClusterEnumerateRequest) (*api.ClusterEnumerateResponse, error) {
	return p.clusterManager.Enumerate(context.Background(), req)
}

func (p *portworx) DeleteCluster(req *api.ClusterDeleteRequest) (*api.ClusterDeleteResponse, error) {
	return p.clusterManager.Delete(context.Background(), req)
}

func (p *portworx) CreateBackupLocation(req *api.BackupLocationCreateRequest) (*api.BackupLocationCreateResponse, error) {
	return p.backupLocationManager.Create(context.Background(), req)
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

func (p *portworx) CreateBackup(req *api.BackupCreateRequest) (*api.BackupCreateResponse, error) {
	return p.backupManager.Create(context.Background(), req)
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

func init() {
	backup.Register(driverName, &portworx{})
}
