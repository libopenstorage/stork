package drivers

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
)

// Known drivers.
const (
	Rsync            = "rsync"
	ResticBackup     = "resticbackup"
	ResticRestore    = "resticrestore"
	KopiaBackup      = "kopiabackup"
	KopiaRestore     = "kopiarestore"
	KopiaDelete      = "kopiadelete"
	KopiaMaintenance = "kopiamaintenance"
	NFSBackup        = "nfsbackup"
	NFSRestore       = "nfsrestore"
	NFSCSIRestore    = "nfscsirestore"
	NFSDelete        = "nfsdelete"
)

// Docker images.
const (
	ResticExecutorImage = "portworx/resticexecutor"
	KopiaExecutorImage  = "kopiaexecutor"
	RsyncImage          = "eeacms/rsync"
	NfsExecutorImage    = "nfsexecutor"
)

// Driver labels.
const (
	DriverNameLabel = "kdmp.portworx.com/driver-name"
)

const (
	// TransferProgressCompleted is a status for a data transfer.
	TransferProgressCompleted float64 = 100
)

// Common parameters for restic secret.
const (
	SecretKey            = "secret"
	SecretValue          = "resticsecret"
	SecretMount          = "/etc/resticsecret"
	KopiaSecretValue     = "kopiasecret"
	KopiaSecretMount     = "/tmp/kopiasecret"
	KopiaSecretKey       = "password"
	KopiaCredSecretMount = "/etc/cred-secret"
	CertDirPath          = "SSL_CERT_DIR"
	CertFileName         = "public.crt"
	CertSecretName       = "tls-s3-cert"
	CertMount            = "/etc/tls-s3-cert"
	NfsMount             = "/mnt/nfs-target/"
)

// Driver job options.
const (
	RsyncFlags                   = "KDMP_RSYNC_FLAGS"
	RsyncOpenshiftSCC            = "KDMP_RSYNC_OPENSHIFT_SCC"
	RsyncImageKey                = "KDMP_RSYNC_IMAGE"
	RsyncImageSecretKey          = "KDMP_RSYNC_IMAGE_SECRET"
	RsyncRequestCPU              = "KDMP_RSYNC_REQUEST_CPU"
	RsyncRequestMemory           = "KDMP_RSYNC_REQUEST_MEMORY"
	RsyncLimitCPU                = "KDMP_RSYNC_LIMIT_CPU"
	RsyncLimitMemory             = "KDMP_RSYNC_LIMIT_MEMORY"
	ResticExecutorImageKey       = "KDMP_RESTICEXECUTOR_IMAGE"
	ResticExecutorImageSecretKey = "KDMP_RESTICEXECUTOR_IMAGE_SECRET"
	ResticExecutorRequestCPU     = "KDMP_RESTICEXECUTOR_REQUEST_CPU"
	ResticExecutorRequestMemory  = "KDMP_RESTICEXECUTOR_REQUEST_MEMORY"
	ResticExecutorLimitCPU       = "KDMP_RESTICEXECUTOR_LIMIT_CPU"
	ResticExecutorLimitMemory    = "KDMP_RESTICEXECUTOR_LIMIT_MEMORY"
	KopiaExecutorImageKey        = "KDMP_KOPIAEXECUTOR_IMAGE"
	KopiaExecutorImageSecretKey  = "KDMP_KOPIAEXECUTOR_IMAGE_SECRET"
	KopiaExecutorRequestCPU      = "KDMP_KOPIAEXECUTOR_REQUEST_CPU"
	KopiaExecutorRequestMemory   = "KDMP_KOPIAEXECUTOR_REQUEST_MEMORY"
	KopiaExecutorLimitCPU        = "KDMP_KOPIAEXECUTOR_LIMIT_CPU"
	KopiaExecutorLimitMemory     = "KDMP_KOPIAEXECUTOR_LIMIT_MEMORY"
	NFSExecutorRequestCPU        = "KDMP_NFSEXECUTOR_REQUEST_CPU"
	NFSExecutorRequestMemory     = "KDMP_NFSEXECUTOR_REQUEST_MEMORY"
	NFSExecutorLimitCPU          = "KDMP_NFSEXECUTOR_LIMIT_CPU"
	NFSExecutorLimitMemory       = "KDMP_NFSEXECUTOR_LIMIT_MEMORNFS"
	KdmpDisableIstioConfig       = "KDMP_DISABLE_ISTIO_CONFIG"
)

// Default parameters for job options.
const (
	DefaultRsyncRequestCPU             = "1"
	DefaultRsyncRequestMemory          = "700Mi"
	DefaultRsyncLimitCPU               = "2"
	DefaultRsyncLimitMemory            = "1Gi"
	DefaultResticExecutorRequestCPU    = "1"
	DefaultResticExecutorRequestMemory = "700Mi"
	DefaultResticExecutorLimitCPU      = "2"
	DefaultResticExecutorLimitMemory   = "1Gi"
	DefaultKopiaExecutorRequestCPU     = "0.1"
	DefaultKopiaExecutorRequestMemory  = "700Mi"
	DefaultKopiaExecutorLimitCPU       = "0.2"
	DefaultKopiaExecutorLimitMemory    = "1Gi"
	DefaultNFSExecutorRequestCPU       = "0.1"
	DefaultNFSExecutorRequestMemory    = "700Mi"
	DefaultNFSExecutorLimitCPU         = "0.5"
	DefaultNFSExecutorLimitMemory      = "1.5Gi"
)

var (
	// CertFilePath path where certificates are mounted in the pod for TLS
	CertFilePath string
)

// JobState represents a data transfer job state.
type JobState string

const (
	// JobStateInProgress means data transfer is processing.
	JobStateInProgress = "InProgress"
	// JobStateCompleted means data transfer is completed.
	JobStateCompleted = "Completed"
	// JobStateFailed means data transfer is failed.
	JobStateFailed = "Failed"
)

var (
	// ErrJobFailed is a know error for a data transfer job failure.
	ErrJobFailed       = fmt.Errorf("data transfer job failed")
	PxbJobNodeLabelKey = "PXB_JOB_NODE_AFFINITY_LABEL"
)

// Interface defines a data export driver behaviour.
type Interface interface {
	// Name returns a name of the driver.
	Name() string
	// StartJob creates a job for data transfer between volumes.
	StartJob(opts ...JobOption) (id string, err error)
	// DeleteJob stops data transfer between volumes.
	DeleteJob(id string) error
	// JobStatus returns a progress status for a data transfer.
	JobStatus(id string) (status *JobStatus, err error)
}

// JobStatus provides information about data transfer job.
type JobStatus struct {
	ProgressPercents float64
	State            JobState
	Reason           string
	Status           batchv1.JobConditionType
}

// IsTransferCompleted allows to check transfer status.
func IsTransferCompleted(progress float64) bool {
	return progress == TransferProgressCompleted
}
