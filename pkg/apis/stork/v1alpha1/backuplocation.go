package v1alpha1

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// BackupLocationResourceName is name for "backuplocation" resource
	BackupLocationResourceName = "backuplocation"
	// BackupLocationResourcePlural is plural for "backuplocation" resource
	BackupLocationResourcePlural = "backuplocations"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BackupLocation represents a backuplocation object
type BackupLocation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Location          BackupLocationItem `json:"location"`
	Cluster           ClusterItem        `json:"cluster"`
}

// BackupLocationItem is the spec used to store a backup location
// Only one of S3Config, AzureConfig or GoogleConfig should be specified and
// should match the Type field. Members of the config can be specified inline or
// through the SecretConfig
type BackupLocationItem struct {
	Type BackupLocationType `json:"type"`
	// Path is either the bucket or any other path for the backup location
	Path string `json:"path"`
	// EncryptionKey is deprecated. Instead use EncryptionV2Key field to pass the encryption key.
	EncryptionKey      string        `json:"encryptionKey"`
	S3Config           *S3Config     `json:"s3Config,omitempty"`
	AzureConfig        *AzureConfig  `json:"azureConfig,omitempty"`
	GoogleConfig       *GoogleConfig `json:"googleConfig,omitempty"`
	SecretConfig       string        `json:"secretConfig"`
	Sync               bool          `json:"sync"`
	RepositoryPassword string        `json:"repositoryPassword"`
	// EncryptionV2Key will be used to pass encryption key.
	EncryptionV2Key string `json:"encryptionV2Key"`
}

// ClusterItem is the spec used to store a the credentials associated with the cluster
// Only one of AWSClusterConfig, AzureClusterConfig or GCPClusterConfig should be specified and
// should match the Type field. Members of the config can be specified inline or
// through the SecretConfig
type ClusterItem struct {
	Type ClusterType `json:"type"`
	// Path is either the bucket or any other path for the backup location
	EncryptionKey      string        `json:"encryptionKey"`
	AWSClusterConfig   *S3Config     `json:"awsClusterConfig,omitempty"`
	AzureClusterConfig *AzureConfig  `json:"azureClusterConfig,omitempty"`
	GCPClusterConfig   *GoogleConfig `json:"gcpClusterConfig,omitempty"`
	SecretConfig       string        `json:"secretConfig"`
	Sync               bool          `json:"sync"`
}

// BackupLocationType is the type of the backup location
type BackupLocationType string

const (
	// BackupLocationS3 stores the backup in an S3-compliant objectstore
	BackupLocationS3 BackupLocationType = "s3"
	// BackupLocationAzure stores the backup in Azure Blob Storage
	BackupLocationAzure BackupLocationType = "azure"
	// BackupLocationGoogle stores the backup in Google Cloud Storage
	BackupLocationGoogle BackupLocationType = "google"
)

// ClusterType is the type of the cluster
type ClusterType string

const (
	// AWSCluster type represents the cluster has aws volumes
	AWSCluster ClusterType = "aws"
	// GCPCluster type represents the cluster has gcp volumes
	GCPCluster ClusterType = "gcp"
	// AzureCluster type represents the cluster has azure volumes
	AzureCluster ClusterType = "azure"
)

// S3Config specifies the config required to connect to an S3-compliant
// objectstore
type S3Config struct {
	// Endpoint will be defaulted to s3.amazonaws.com by the controller if not provided
	Endpoint        string `json:"endpoint"`
	AccessKeyID     string `json:"accessKeyID"`
	SecretAccessKey string `json:"secretAccessKey"`
	// Region will be defaulted to us-east-1 by the controller if not provided
	Region string `json:"region"`
	// Disable SSL option if using with a non-AWS S3 objectstore which doesn't
	// have SSL enabled
	DisableSSL bool `json:"disableSSL"`
	// The S3 Storage Class to use when uploading objects. Glacier storage
	// classes are not supported
	StorageClass string `json:"storageClass"`
	// UseIam when set stork will use the instance IAM role associated with the nodes
	// on which stork pods run
	UseIam bool `json:"useIam"`
}

// AzureConfig specifies the config required to connect to Azure Blob Storage
type AzureConfig struct {
	StorageAccountName string `json:"storageAccountName"`
	StorageAccountKey  string `json:"storageAccountKey"`
	TenantID           string `json:"tenantID"`
	SubscriptionID     string `json:"subscriptionID"`
	ClientID           string `json:"clientID"`
	ClientSecret       string `json:"clientSecret"`
}

// GoogleConfig specifies the config required to connect to Google Cloud Storage
type GoogleConfig struct {
	ProjectID  string `json:"projectID"`
	AccountKey string `json:"accountKey"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BackupLocationList is a list of ApplicationBackups
type BackupLocationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []BackupLocation `json:"items"`
}

// UpdateFromSecret updated the config information from the secret if not provided inline
func (bl *BackupLocation) UpdateFromSecret(client kubernetes.Interface) error {
	if bl.Location.SecretConfig != "" {
		secretConfig, err := client.CoreV1().Secrets(bl.Namespace).Get(context.TODO(), bl.Location.SecretConfig, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error getting secretConfig for backupLocation: %v", err)
		}
		if val, ok := secretConfig.Data["encryptionKey"]; ok && val != nil {
			bl.Location.EncryptionV2Key = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["path"]; ok && val != nil {
			bl.Location.Path = strings.TrimSuffix(string(val), "\n")
		}
	}
	switch bl.Location.Type {
	case BackupLocationS3:
		return bl.getMergedS3Config(client)
	case BackupLocationAzure:
		return bl.getMergedAzureConfig(client)
	case BackupLocationGoogle:
		return bl.getMergedGoogleConfig(client)
	default:
		return fmt.Errorf("Invalid BackupLocation type %v", bl.Location.Type)
	}

}

// UpdateFromClusterSecret updated the config information from the cluster secret if not provided inline
func (bl *BackupLocation) UpdateFromClusterSecret(client kubernetes.Interface) error {
	if bl.Cluster.SecretConfig != "" {
		switch bl.Cluster.Type {
		case AWSCluster:
			return bl.getMergedAWSClusterCred(client)
		case GCPCluster:
			return bl.getMergedGCPClusterCred(client)
		case AzureCluster:
			return bl.getMergedAzureClusterCred(client)
		default:
		}
	}
	return nil
}

func (bl *BackupLocation) getMergedS3Config(client kubernetes.Interface) error {
	if bl.Location.S3Config == nil {
		bl.Location.S3Config = &S3Config{}
		bl.Location.S3Config.Endpoint = "s3.amazonaws.com"
		bl.Location.S3Config.Region = "us-east-1"
		bl.Location.S3Config.DisableSSL = false
	}
	if bl.Location.SecretConfig != "" {
		secretConfig, err := client.CoreV1().Secrets(bl.Namespace).Get(context.TODO(), bl.Location.SecretConfig, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error getting secretConfig for backupLocation: %v", err)
		}
		if val, ok := secretConfig.Data["endpoint"]; ok && val != nil {
			bl.Location.S3Config.Endpoint = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["accessKeyID"]; ok && val != nil {
			bl.Location.S3Config.AccessKeyID = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["secretAccessKey"]; ok && val != nil {
			bl.Location.S3Config.SecretAccessKey = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["region"]; ok && val != nil {
			bl.Location.S3Config.Region = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["disableSSL"]; ok && val != nil {
			bl.Location.S3Config.DisableSSL, err = strconv.ParseBool(strings.TrimSuffix(string(val), "\n"))
			if err != nil {
				return fmt.Errorf("error parding disableSSL from Secret: %v", err)
			}
		}
		if val, ok := secretConfig.Data["storageClass"]; ok && val != nil {
			bl.Location.S3Config.StorageClass = strings.TrimSuffix(string(val), "\n")
		}
	}
	return nil
}

func (bl *BackupLocation) getMergedAzureConfig(client kubernetes.Interface) error {
	if bl.Location.AzureConfig == nil {
		bl.Location.AzureConfig = &AzureConfig{}
	}
	if bl.Location.SecretConfig != "" {
		secretConfig, err := client.CoreV1().Secrets(bl.Namespace).Get(context.TODO(), bl.Location.SecretConfig, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error getting secretConfig for backupLocation: %v", err)
		}
		if val, ok := secretConfig.Data["storageAccountName"]; ok && val != nil {
			bl.Location.AzureConfig.StorageAccountName = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["storageAccountKey"]; ok && val != nil {
			bl.Location.AzureConfig.StorageAccountKey = strings.TrimSuffix(string(val), "\n")
		}
	}
	return nil

}

func (bl *BackupLocation) getMergedGoogleConfig(client kubernetes.Interface) error {
	if bl.Location.GoogleConfig == nil {
		bl.Location.GoogleConfig = &GoogleConfig{}
	}
	if bl.Location.SecretConfig != "" {
		secretConfig, err := client.CoreV1().Secrets(bl.Namespace).Get(context.TODO(), bl.Location.SecretConfig, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error getting secretConfig for backupLocation: %v", err)
		}
		if val, ok := secretConfig.Data["projectID"]; ok && val != nil {
			bl.Location.GoogleConfig.ProjectID = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["accountKey"]; ok && val != nil {
			bl.Location.GoogleConfig.AccountKey = strings.TrimSuffix(string(val), "\n")
		}
	}
	return nil
}

func (bl *BackupLocation) getMergedAWSClusterCred(client kubernetes.Interface) error {
	if bl.Cluster.SecretConfig != "" {
		secretConfig, err := client.CoreV1().Secrets(bl.Namespace).Get(context.TODO(), bl.Cluster.SecretConfig, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error getting secretConfig for cluster from backuplocation:  %v", err)
		}
		if val, ok := secretConfig.Data["accessKeyID"]; ok && val != nil {
			bl.Cluster.AWSClusterConfig.AccessKeyID = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["secretAccessKey"]; ok && val != nil {
			bl.Cluster.AWSClusterConfig.SecretAccessKey = strings.TrimSuffix(string(val), "\n")
		}
	}
	return nil
}

func (bl *BackupLocation) getMergedGCPClusterCred(client kubernetes.Interface) error {
	if bl.Cluster.SecretConfig != "" {
		secretConfig, err := client.CoreV1().Secrets(bl.Namespace).Get(context.TODO(), bl.Cluster.SecretConfig, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error getting secretConfig for cluster from backuplocation: %v", err)
		}
		if val, ok := secretConfig.Data["projectID"]; ok && val != nil {
			bl.Cluster.GCPClusterConfig.ProjectID = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["accountKey"]; ok && val != nil {
			bl.Cluster.GCPClusterConfig.AccountKey = strings.TrimSuffix(string(val), "\n")
		}
	}
	return nil
}

func (bl *BackupLocation) getMergedAzureClusterCred(client kubernetes.Interface) error {
	if bl.Cluster.SecretConfig != "" {
		secretConfig, err := client.CoreV1().Secrets(bl.Namespace).Get(context.TODO(), bl.Cluster.SecretConfig, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error getting secretConfig for backupLocation: %v", err)
		}
		if val, ok := secretConfig.Data["tenantID"]; ok && val != nil {
			bl.Cluster.AzureClusterConfig.TenantID = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["clientID"]; ok && val != nil {
			bl.Cluster.AzureClusterConfig.ClientID = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["clientSecret"]; ok && val != nil {
			bl.Cluster.AzureClusterConfig.ClientSecret = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["subscriptionID"]; ok && val != nil {
			bl.Cluster.AzureClusterConfig.SubscriptionID = strings.TrimSuffix(string(val), "\n")
		}
	}
	return nil
}
