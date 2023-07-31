package backup

// CloudProviders represents the cloud providers configuration.
type CloudProviders struct {
	AWS     map[string]AWSCredential     `json:"aws"`
	Azure   map[string]AzureCredential   `json:"azure"`
	GKE     map[string]GKECredential     `json:"gke"`
	IBM     map[string]IBMCredential     `json:"ibm"`
	RANCHER map[string]RANCHERCredential `json:"rancher"`
}

// AWSCredential represents the AWS credential information.
type AWSCredential struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Region          string `json:"region"`
}

// AzureCredential represents the Azure credential information.
type AzureCredential struct {
	AccountName    string `json:"account_name"`
	AccountKey     string `json:"account_key"`
	SubscriptionID string `json:"subscription_id"`
	ClientID       string `json:"client_id"`
	ClientSecret   string `json:"client_secret"`
	TenantID       string `json:"tenant_id"`
}

// GKECredential represents the GKE credential information.
type GKECredential struct {
	ProjectID       string `json:"project_id"`
	ClusterName     string `json:"cluster_name"`
	Location        string `json:"location"`
	CredentialsFile string `json:"credentials_file"`
}

// IBMCredential represents the IBM credential information.
type IBMCredential struct {
	AccountName   string `json:"account_name"`
	APIKey        string `json:"api_key"`
	Region        string `json:"region"`
	ResourceGroup string `json:"resource_group"`
}

// RANCHERCredential represents the IBM credential information.
type RANCHERCredential struct {
	UserName string `json:"username"`
	Password string `json:"password"`
}

// BackupTargets represents the backup targets configuration.
type BackupTargets struct {
	AWS map[string]Bucket    `json:"aws"`
	NFS map[string]NFSServer `json:"nfs"`
}

// Bucket represents the bucket information for backup.
type Bucket struct {
	Name            string `json:"name"`
	Provider        string `json:"provider"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Region          string `json:"region"`
	Tag             string `json:"tag"`
}

// NFSServer represents the NFS server information for backup.
type NFSServer struct {
	Name          string `json:"name"`
	IP            string `json:"ip"`
	ExportPath    string `json:"export_path"`
	SubPath       string `json:"sub_path"`
	MountOptions  string `json:"mount_options"`
	EncryptionKey string `json:"encryption_key"`
	Tag           string `json:"tag"`
}

// BackupCloudConfig representing the credentials for cloud platforms.
type BackupCloudConfig struct {
	CloudProviders CloudProviders `json:"cloudProviders"`
	BackupTargets  BackupTargets  `json:"backupTargets"`
}

// GetAWSCredential retrieves the AWS credential based on the provided tag.
func (p *CloudProviders) GetAWSCredential(tag string) AWSCredential {
	return p.AWS[tag]
}

// GetAzureCredential retrieves the Azure credential based on the provided tag.
func (p *CloudProviders) GetAzureCredential(tag string) AzureCredential {
	return p.Azure[tag]
}

// GetGKECredential retrieves the GKE credential based on the provided tag.
func (p *CloudProviders) GetGKECredential(tag string) GKECredential {
	return p.GKE[tag]
}

// GetIBMCredential retrieves the IBM credential based on the provided tag.
func (p *CloudProviders) GetIBMCredential(tag string) IBMCredential {
	return p.IBM[tag]
}

// GetNFSServer retrieves the NFS server based on the provided tag.
func (p *BackupTargets) GetNFSServer(tag string) NFSServer {
	return p.NFS[tag]
}

// GetAWSBucket retrieves the AWS bucket based on the provided tag.
func (p *BackupTargets) GetAWSBucket(tag string) Bucket {
	return p.AWS[tag]
}
