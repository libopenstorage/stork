package tests

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
	appsapi "k8s.io/api/apps/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	configMapName       = "kubeconfigs"
	kubeconfigDirectory = "/tmp"
)

// getProvider validates and return object store provider
func getProvider() (string, error) {
	provider, _ := os.LookupEnv("OBJECT_STORE_PROVIDER")
	if provider == "" {
		return "", fmt.Errorf("no environment variable 'PROVIDER' supplied. Valid values are: %s, %s, %s",
			drivers.ProviderAws, drivers.ProviderAzure, drivers.ProviderGke)
	}
	switch provider {
	case drivers.ProviderAws, drivers.ProviderAzure, drivers.ProviderGke:
	default:
		return "", fmt.Errorf("valid values for 'PROVIDER' environment variables are: %s, %s, %s",
			drivers.ProviderAws, drivers.ProviderAzure, drivers.ProviderGke)
	}
	return provider, nil
}

func setClusterContext(clusterConfigPath string) error {
	err := tests.Inst().S.SetConfig(clusterConfigPath)
	if err != nil {
		return nil
	}
	err = tests.Inst().S.RefreshNodeRegistry()
	if err != nil {
		return nil
	}
	err = tests.Inst().V.RefreshDriverEndpoints()
	if err != nil {
		return nil
	}
	return nil
}

func setupBackup(provider, testName, orgID, credName, backupLocationName, bucketName string) error {
	logrus.Infof("Run Setup backup with object store provider: %s", provider)
	if err := createBucket(provider, bucketName); err != nil {
		return err
	}
	if err := createOrganization(orgID); err != nil {
		return err
	}
	if err := createCloudCredential(provider, credName, orgID); err != nil {
		return err
	}
	if err := createBackupLocation(provider, backupLocationName, credName, bucketName, orgID); err != nil {
		return err
	}
	if err := createSourceAndDestClusters(credName, orgID, sourceClusterName, destinationClusterName); err != nil {
		return err
	}
	return nil
}

func tearDownBackupRestore(contexts []*scheduler.Context, provider, taskNamePrefix string) error {
	for _, ctx := range contexts {
		for i := 0; i < tests.Inst().ScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			bkpNamespace := tests.GetAppNamespace(ctx, taskName)
			BackupName := fmt.Sprintf("%s-%s", backupNamePrefix, bkpNamespace)
			RestoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, bkpNamespace)
			if err := deleteBackup(BackupName, orgID); err != nil {
				return err
			}

			if err := deleteRestore(RestoreName, orgID); err != nil {
				return err
			}
		}
	}
	if err := deleteCluster(destinationClusterName, orgID); err != nil {
		return err
	}
	if err := deleteCluster(sourceClusterName, orgID); err != nil {
		return err
	}
	if err := deleteBackupLocation(backupLocationName, orgID); err != nil {
		return err
	}
	if err := deleteCloudCredential(credName, orgID); err != nil {
		return err
	}
	if err := deleteBucket(provider, bucketName); err != nil {
		return err
	}
	return nil
}

func killStork(storkDeploymentName, storkDeploymentNamespace string) error {
	ctx := &scheduler.Context{
		App: &spec.AppSpec{
			SpecList: []interface{}{
				&appsapi.Deployment{
					ObjectMeta: meta_v1.ObjectMeta{
						Name:      storkDeploymentName,
						Namespace: storkDeploymentNamespace,
					},
				},
			},
		},
	}
	err := tests.Inst().S.DeleteTasks(ctx, nil)

	if err != nil {
		return fmt.Errorf("delete stork failed with [%v]", err)
	}

	return nil
}

func createOrganization(orgID string) error {
	backupDriver := tests.Inst().Backup
	req := &api.OrganizationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name: orgID,
		},
	}
	_, err := backupDriver.CreateOrganization(req)

	if err != nil {
		return fmt.Errorf("failed to create organization [%s]. Error: [%v]",
			orgID, err)
	}

	return nil
}

func deleteCloudCredential(name string, orgID string) error {
	backupDriver := tests.Inst().Backup
	credDeleteRequest := &api.CloudCredentialDeleteRequest{
		Name:  name,
		OrgId: orgID,
	}
	if _, err := backupDriver.DeleteCloudCredential(credDeleteRequest); err != nil {
		return fmt.Errorf("failed delete coud credential %s in org %s error: "+
			"[%v]", name, orgID, err)
	}
	return nil
}

func createBucket(provider string, bucketName string) error {
	switch provider {
	case drivers.ProviderAws:
		return createS3Bucket(bucketName)
	case drivers.ProviderAzure:
		return createAzureBucket(bucketName)
	}
	return fmt.Errorf("unknown provider")
}

func createS3Bucket(bucketName string) error {
	id, secret, endpoint, s3Region, disableSSLBool, err := getAWSDetailsFromEnv()
	if err != nil {
		return fmt.Errorf("failed get aws creds [%v]", err)
	}
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(id, secret, ""),
		Region:           aws.String(s3Region),
		DisableSSL:       aws.Bool(disableSSLBool),
		S3ForcePathStyle: aws.Bool(true),
	},
	)

	if err != nil {
		return fmt.Errorf("failed to get S3 session to create bucket. Error: [%v]", err)
	}
	S3Client := s3.New(sess)
	_, err = S3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return fmt.Errorf("failed to create bucket [%v]. Error: [%v]", bucketName, err)
	}
	err = S3Client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return fmt.Errorf("failed to wait for bucket [%v] to get created. Error: [%v]", bucketName, err)
	}

	return nil
}

func createAzureBucket(bucketName string) error {
	// From the Azure portal, get your Storage account blob service URL endpoint.
	_, _, _, _, accountName, accountKey, err := getAzureCredsFromEnv()
	if err != nil {
		return fmt.Errorf("failed get azure creds [%v]", err)
	}
	urlStr := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, bucketName)
	logrus.Infof("Create container url %s", urlStr)
	// Create a ContainerURL object that wraps a soon-to-be-created container's URL and a default pipeline.
	u, _ := url.Parse(urlStr)
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return fmt.Errorf("failed to create shared key credential [%v]", err)
	}
	containerURL := azblob.NewContainerURL(*u, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
	ctx := context.Background() // This example uses a never-expiring context
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		return fmt.Errorf("failed to create container. Error: [%v]", err)
	}
	return nil
}

func deleteBucket(provider string, bucketName string) error {
	switch provider {
	case drivers.ProviderAws:
		return deleteS3Bucket(bucketName)
	case drivers.ProviderAzure:
		return deleteAzureBucket(bucketName)
	}
	return fmt.Errorf("unknown provider %s", provider)
}

func deleteS3Bucket(bucketName string) error {
	id, secret, endpoint, s3Region, disableSSLBool, err := getAWSDetailsFromEnv()
	if err != nil {
		return fmt.Errorf("failed get aws creds [%v]", err)
	}
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(id, secret, ""),
		Region:           aws.String(s3Region),
		DisableSSL:       aws.Bool(disableSSLBool),
		S3ForcePathStyle: aws.Bool(true),
	},
	)
	if err != nil {
		return fmt.Errorf("failed to get S3 session to create bucket. Error: [%v]", err)
	}

	S3Client := s3.New(sess)

	iter := s3manager.NewDeleteListIterator(S3Client, &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})

	err = s3manager.NewBatchDeleteWithClient(S3Client).Delete(aws.BackgroundContext(), iter)
	if err != nil {
		return fmt.Errorf("unable to delete objects from bucket %q, %v", bucketName, err)
	}

	_, err = S3Client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		return fmt.Errorf("failed to delete bucket [%v]. Error: [%v]", bucketName, err)
	}

	return nil
}

func deleteAzureBucket(bucketName string) error {
	// From the Azure portal, get your Storage account blob service URL endpoint.
	_, _, _, _, accountName, accountKey, err := getAzureCredsFromEnv()
	if err != nil {
		return fmt.Errorf("failed get azure creds [%v]", err)
	}
	urlStr := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, bucketName)
	logrus.Infof("Delete container url %s", urlStr)
	// Create a ContainerURL object that wraps a soon-to-be-created container's URL and a default pipeline.
	u, _ := url.Parse(urlStr)
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return fmt.Errorf("failed to create shared key credential [%v]", err)
	}
	containerURL := azblob.NewContainerURL(*u, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
	ctx := context.Background() // This example uses a never-expiring context

	_, err = containerURL.Delete(ctx, azblob.ContainerAccessConditions{})

	if err != nil {
		return fmt.Errorf("failed to delete container. Error: [%v]", err)
	}

	return nil
}

// createCloudCredential creates cloud credetials
func createCloudCredential(provider, name string, orgID string) error {
	logrus.Printf("Create credential name %s for org %s provider %s", name, orgID, provider)
	backupDriver := tests.Inst().Backup
	switch provider {
	case drivers.ProviderAws:
		log.Printf("Create creds for aws")
		id := os.Getenv("AWS_ACCESS_KEY_ID")
		if id == "" {
			return fmt.Errorf("empty AWS_ACCESS_KEY_ID")
		}
		secret := os.Getenv("AWS_SECRET_ACCESS_KEY")

		if secret == "" {
			return fmt.Errorf("empty AWS_SECRET_ACCESS_KEY")
		}

		credCreateRequest := &api.CloudCredentialCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  name,
				OrgId: orgID,
			},
			CloudCredential: &api.CloudCredentialInfo{
				Type: api.CloudCredentialInfo_AWS,
				Config: &api.CloudCredentialInfo_AwsConfig{
					AwsConfig: &api.AWSConfig{
						AccessKey: id,
						SecretKey: secret,
					},
				},
			},
		}
		_, err := backupDriver.CreateCloudCredential(credCreateRequest)
		if err != nil {
			return fmt.Errorf("failed to create cloud credential [%s] in org [%s]", name, orgID)
		}
	case drivers.ProviderAzure:
		logrus.Infof("Create creds for azure")
		tenantID, clientID, clientSecret, subscriptionID, accountName, accountKey, err := getAzureCredsFromEnv()
		if err != nil {
			return fmt.Errorf("failed get azure creds [%v]", err)
		}
		credCreateRequest := &api.CloudCredentialCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  name,
				OrgId: orgID,
			},
			CloudCredential: &api.CloudCredentialInfo{
				Type: api.CloudCredentialInfo_Azure,
				Config: &api.CloudCredentialInfo_AzureConfig{
					AzureConfig: &api.AzureConfig{
						TenantId:       tenantID,
						ClientId:       clientID,
						ClientSecret:   clientSecret,
						AccountName:    accountName,
						AccountKey:     accountKey,
						SubscriptionId: subscriptionID,
					},
				},
			},
		}
		_, err = backupDriver.CreateCloudCredential(credCreateRequest)
		if err != nil {
			return fmt.Errorf("failed to create cloud credential [%s] in org [%s]", name, orgID)
		}
	default:
		return fmt.Errorf("unknown provider")
	}
	return nil
}

func getAWSDetailsFromEnv() (id string, secret string, endpoint string,
	s3Region string, disableSSLBool bool, err error) {
	id = os.Getenv("AWS_ACCESS_KEY_ID")

	if id == "" {
		return "", "", "", "", false,
			fmt.Errorf("AWS_ACCESS_KEY_ID Environment variable should not be empty")
	}
	secret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secret == "" {
		return "", "", "", "", false,
			fmt.Errorf("AWS_SECRET_ACCESS_KEY Environment variable should not be empty")
	}
	endpoint = os.Getenv("S3_ENDPOINT")
	if endpoint != "" {
		return "", "", "", "", false,
			fmt.Errorf("S3_ENDPOINT Environment variable should not be empty")
	}
	s3Region = os.Getenv("S3_REGION")
	if s3Region == "" {
		return "", "", "", "", false,
			fmt.Errorf("S3_REGION Environment variable should not be empty")
	}
	disableSSL := os.Getenv("S3_DISABLE_SSL")
	disableSSLBool, err = strconv.ParseBool(disableSSL)
	if err != nil {
		return "", "", "", "", false,
			fmt.Errorf("S3_DISABLE_SSL=%s is not a valid boolean value %v", disableSSL, err)
	}
	return id, secret, endpoint, s3Region, disableSSLBool, nil
}

func getAzureCredsFromEnv() (tenantID, clientID, clientSecret, subscriptionID,
	accountName, accountKey string, err error) {
	accountName = os.Getenv("AZURE_ACCOUNT_NAME")
	if accountName == "" {
		return "", "", "", "", "",
			"", fmt.Errorf("AZURE_ACCOUNT_NAME Environment variable should not be empty")
	}
	accountKey = os.Getenv("AZURE_ACCOUNT_KEY")
	if accountKey == "" {
		return "", "", "", "", "",
			"", fmt.Errorf("AZURE_ACCOUNT_KEY Environment variable should not be empty")
	}

	tenantID = os.Getenv("AZURE_TENANT_ID")
	if tenantID == "" {
		return "", "", "", "", "",
			"", fmt.Errorf("AZURE_TENANT_ID Environment variable should not be empty")
	}

	clientID = os.Getenv("AZURE_CLIENT_ID")
	if clientID == "" {
		return "", "", "", "", "",
			"", fmt.Errorf("AZURE_CLIENT_ID Environment variable should not be empty")
	}

	clientSecret = os.Getenv("AZURE_CLIENT_SECRET")
	if clientSecret == "" {
		return "", "", "", "", "",
			"", fmt.Errorf("AZURE_CLIENT_SECRET Environment variable should not be empty")
	}

	subscriptionID = os.Getenv("AZURE_SUBSCRIPTION_ID")
	if subscriptionID == "" {
		return "", "", "", "", "",
			"", fmt.Errorf("AZURE_SUBSCRIPTION_ID Environment variable should not be empty")
	}
	return tenantID, clientID, clientSecret, subscriptionID, accountName, accountKey, nil
}

func createBackupLocation(provider, name, credName, bucketName, orgID string) error {
	switch provider {
	case drivers.ProviderAws:
		return createS3BackupLocation(name, credName, bucketName, orgID)
	case drivers.ProviderAzure:
		return createAzureBackupLocation(name, credName, bucketName, orgID)
	}
	return fmt.Errorf("unknown provider %s", provider)
}

func createS3BackupLocation(name string, cloudCred string, bucketName string, orgID string) error {
	backupDriver := tests.Inst().Backup
	_, _, endpoint, region, disableSSLBool, err := getAWSDetailsFromEnv()

	if err != nil {
		return fmt.Errorf("failed get aws credentials %v", err)
	}
	encryptionKey := "torpedo"
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
		},
		BackupLocation: &api.BackupLocationInfo{
			Path:            bucketName,
			EncryptionKey:   encryptionKey,
			CloudCredential: cloudCred,
			Type:            api.BackupLocationInfo_S3,
			Config: &api.BackupLocationInfo_S3Config{
				S3Config: &api.S3Config{
					Endpoint:   endpoint,
					Region:     region,
					DisableSsl: disableSSLBool,
				},
			},
		},
	}
	_, err = backupDriver.CreateBackupLocation(bLocationCreateReq)

	if err != nil {
		return fmt.Errorf("failed to create backuplocation [%s] in org [%s]", name, orgID)
	}

	return nil
}

func createAzureBackupLocation(name string, cloudCred string, bucketName string, orgID string) error {
	backupDriver := tests.Inst().Backup
	encryptionKey := "torpedo"
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
		},
		BackupLocation: &api.BackupLocationInfo{
			Path:            bucketName,
			EncryptionKey:   encryptionKey,
			CloudCredential: cloudCred,
			Type:            api.BackupLocationInfo_Azure,
		},
	}
	_, err := backupDriver.CreateBackupLocation(bLocationCreateReq)
	if err != nil {
		return fmt.Errorf("failed to create backuplocation [%s] in org [%s]", name, orgID)
	}
	return nil
}

func deleteBackupLocation(name string, orgID string) error {
	backupDriver := tests.Inst().Backup
	bLocationDeleteReq := &api.BackupLocationDeleteRequest{
		Name:  name,
		OrgId: orgID,
	}
	if _, err := backupDriver.DeleteBackupLocation(bLocationDeleteReq); err != nil {
		return fmt.Errorf("failed to delete backup location "+
			"%s in org %s error: [%v]", name, orgID, err)
	}

	return nil
}

func createSourceAndDestClusters(cloudCred, orgID, sourceClusterName, destinationClusterName string) error {
	// TODO: Add support for adding multiple clusters from
	// comma separated list of kubeconfig files
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs != "" {
		return fmt.Errorf("KUBECONFIGS Environment variable should not be empty")
	}
	kubeconfigList := strings.Split(kubeconfigs, ",")
	// Validate user has provided at least 2 kubeconfigs for source and destination cluster
	if len(kubeconfigList) < 2 {
		return fmt.Errorf("at least minimum two kubeconfigs required")
	}
	err := dumpKubeConfigs(configMapName, kubeconfigList)
	if err != nil {
		return fmt.Errorf("failed to get kubeconfigs [%v] from configmap [%s]", kubeconfigList, configMapName)
	}
	// Register source cluster with backup driver
	srcClusterConfigPath, err := getSourceClusterConfigPath()
	if err != nil {
		return fmt.Errorf("failed to get kubeconfig path for source cluster. Error: [%v]", err)
	}

	logrus.Debugf("Save cluster %s kubeconfig to %s", sourceClusterName, srcClusterConfigPath)
	if err := createCluster(sourceClusterName, cloudCred, srcClusterConfigPath, orgID); err != nil {
		return fmt.Errorf("failed create source cluster %s error: [%v]", sourceClusterName, err)
	}

	// Register destination cluster with backup driver
	dstClusterConfigPath, err := getDestinationClusterConfigPath()
	if err != nil {
		return fmt.Errorf("failed to get kubeconfig path for destination cluster. Error: [%v]", err)
	}
	logrus.Debugf("Save cluster %s kubeconfig to %s", destinationClusterName, dstClusterConfigPath)
	if err := createCluster(destinationClusterName, cloudCred, dstClusterConfigPath, orgID); err != nil {
		return fmt.Errorf("failed create destination cluster %s error: [%v]", destinationClusterName, err)
	}

	return nil
}

func getSourceClusterConfigPath() (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return "", fmt.Errorf("empty KUBECONFIGS environment variable")
	}

	kubeconfigList := strings.Split(kubeconfigs, ",")

	if len(kubeconfigList) < 2 {
		return "", fmt.Errorf("at least minimum two kubeconfigs required")
	}
	return fmt.Sprintf("%s/%s", kubeconfigDirectory, kubeconfigList[0]), nil
}

func getDestinationClusterConfigPath() (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return "", fmt.Errorf("empty KUBECONFIGS environment variable")
	}

	kubeconfigList := strings.Split(kubeconfigs, ",")
	if len(kubeconfigList) < 2 {
		return "", fmt.Errorf("at least minimum two kubeconfigs required")
	}
	return fmt.Sprintf("%s/%s", kubeconfigDirectory, kubeconfigList[1]), nil
}

func dumpKubeConfigs(configObject string, kubeconfigList []string) error {
	logrus.Infof("dump kubeconfigs to file system")
	cm, err := core.Instance().GetConfigMap(configObject, "default")
	if err != nil {
		logrus.Errorf("Error reading config map: %v", err)
		return err
	}
	logrus.Infof("Get over kubeconfig list %v", kubeconfigList)
	for _, kubeconfig := range kubeconfigList {
		config := cm.Data[kubeconfig]
		if len(config) == 0 {
			configErr := fmt.Sprintf("Error reading kubeconfig: found empty %s in config map %s",
				kubeconfig, configObject)
			return fmt.Errorf(configErr)
		}
		filePath := fmt.Sprintf("%s/%s", kubeconfigDirectory, kubeconfig)
		logrus.Infof("Save kubeconfig to %s", filePath)
		err := ioutil.WriteFile(filePath, []byte(config), 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpKubeconfigList(kubeconfigList []string) error {
	err := dumpKubeConfigs(configMapName, kubeconfigList)

	if err != nil {
		return fmt.Errorf("failed to get kubeconfigs [%v] from configmap [%s]", kubeconfigList, configMapName)
	}
	return nil
}

func getProviders() []string {
	providersStr := os.Getenv("PROVIDERS")
	return strings.Split(providersStr, ",")
}

func getProviderClusterConfigPath(providers []string, kubeconfigs []string) map[string]string {
	pathMap := make(map[string]string)
	for _, provider := range providers {
		for _, kubeconfigPath := range kubeconfigs {
			if strings.Contains(provider, kubeconfigPath) {
				fullPath := path.Join(kubeconfigDirectory, kubeconfigPath)
				pathMap[provider] = fullPath
			}
		}
	}
	return pathMap
}

func deleteCluster(name string, orgID string) error {
	backupDriver := tests.Inst().Backup
	clusterDeleteReq := &api.ClusterDeleteRequest{
		OrgId: orgID,
		Name:  name,
	}
	if _, err := backupDriver.DeleteCluster(clusterDeleteReq); err != nil {
		return fmt.Errorf("error deleting cluster %s in org %s error: %v", name, orgID, err)
	}

	return nil
}

func createCluster(name string, cloudCred string, kubeconfigPath string, orgID string) error {
	backupDriver := tests.Inst().Backup
	kubeconfigRaw, err := ioutil.ReadFile(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("failed to read kubeconfig file from location [%s]. Error:[%v]",
			kubeconfigPath, err)
	}
	clusterCreateReq := &api.ClusterCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
		},
		Kubeconfig:      base64.StdEncoding.EncodeToString(kubeconfigRaw),
		CloudCredential: cloudCred,
	}

	_, err = backupDriver.CreateCluster(clusterCreateReq)
	if err != nil {
		return fmt.Errorf("failed to create cluster [%s] in org [%s]. Error : [%v]",
			name, orgID, err)
	}

	return nil
}

func createBackup(backupName string, clusterName string, bLocation string,
	namespaces []string, labelSelectors map[string]string, orgID string) error {
	backupDriver := tests.Inst().Backup
	bkpCreateRequest := &api.BackupCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  backupName,
			OrgId: orgID,
		},
		BackupLocation: bLocation,
		Cluster:        clusterName,
		Namespaces:     namespaces,
		LabelSelectors: labelSelectors,
	}
	_, err := backupDriver.CreateBackup(bkpCreateRequest)
	if err != nil {
		return fmt.Errorf("failed to create backup [%s] in org [%s]. Error: [%v]",
			backupName, orgID, err)
	}

	return nil
}

func getNodesForBackup(backupName string, bkpNamespace string,
	orgID string, clusterName string, triggerOpts *driver_api.TriggerOptions) ([]node.Node, error) {

	var nodes []node.Node
	backupDriver := tests.Inst().Backup

	backupInspectReq := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
	}
	err := tests.Inst().Backup.WaitForBackupRunning(context.Background(),
		backupInspectReq, defaultRetryInterval, defaultRetryInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for backup [%s] to start. Error: [%v]",
			backupName, err)
	}
	clusterInspectReq := &api.ClusterInspectRequest{
		OrgId: orgID,
		Name:  clusterName,
	}
	clusterInspectRes, err := backupDriver.InspectCluster(clusterInspectReq)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect cluster [%s] in org [%s]. Error: [%v]",
			clusterName, orgID, err)
	}
	if clusterInspectRes == nil {
		return nil, fmt.Errorf("got an empty response while inspecting cluster [%s] in org [%s]",
			clusterName, orgID)
	}
	cluster := clusterInspectRes.GetCluster()
	volumeBackupIDs, err := backupDriver.GetVolumeBackupIDs(context.Background(),
		backupName, bkpNamespace, cluster, orgID)
	if err != nil {
		return nil, fmt.Errorf("failed to get volume backup IDs for backup [%s] in org [%s]. Error: [%v]",
			backupName, orgID, err)
	}
	if len(volumeBackupIDs) == 0 {
		return nil, fmt.Errorf("got empty list of volumeBackup IDs from backup driver")
	}
	for _, backupID := range volumeBackupIDs {
		n, err := tests.Inst().V.GetNodeForBackup(backupID)
		if err != nil {
			return nil, fmt.Errorf("failed to get node on which backup [%s] in running. Error: [%v]",
				backupName, err)
		}
		logrus.Debugf("Volume backup [%s] is running on node [%s], node id: [%s]\n",
			backupID, n.GetHostname(), n.GetId())
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// createRestore creates restore
func createRestore(restoreName string, backupName string,
	namespaceMapping map[string]string, clusterName string, orgID string) error {
	backupDriver := tests.Inst().Backup
	createRestoreReq := &api.RestoreCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  restoreName,
			OrgId: orgID,
		},
		Backup:           backupName,
		Cluster:          clusterName,
		NamespaceMapping: namespaceMapping,
	}
	_, err := backupDriver.CreateRestore(createRestoreReq)

	if err != nil {
		return fmt.Errorf("failed to create restore [%s] in org [%s] "+
			"on cluster [%s]. Error: [%v]",
			restoreName, orgID, clusterName, err)
	}
	return nil
}

// deleteBackup deletes backup
func deleteBackup(backupName string, orgID string) error {
	backupDriver := tests.Inst().Backup
	bkpDeleteRequest := &api.BackupDeleteRequest{
		Name:  backupName,
		OrgId: orgID,
	}
	_, err := backupDriver.DeleteBackup(bkpDeleteRequest)
	if err != nil {
		return fmt.Errorf("failed delete backup %s in org %s error [%v]",
			backupName, orgID, err)
	}

	ctx, _ := context.WithTimeout(context.Background(),
		backupRestoreCompletionTimeoutMin*time.Minute)
	if err := backupDriver.WaitForBackupDeletion(ctx, backupName, orgID,
		backupRestoreCompletionTimeoutMin*time.Minute,
		retrySeconds*time.Second); err != nil {
		return err
	}
	return nil
}

func deleteRestore(restoreName string, orgID string) error {
	backupDriver := tests.Inst().Backup
	if backupDriver == nil {
		return fmt.Errorf("backup driver is not initialized")
	}
	deleteRestoreReq := &api.RestoreDeleteRequest{
		OrgId: orgID,
		Name:  restoreName,
	}
	_, err := backupDriver.DeleteRestore(deleteRestoreReq)
	if err != nil {
		return fmt.Errorf("failed to delete restore [%s] in org [%s]. Error: [%v]",
			restoreName, orgID, err)
	}
	ctx, _ := context.WithTimeout(context.Background(),
		backupRestoreCompletionTimeoutMin*time.Minute)
	logrus.Infof("Wait for restore %s is deleted", restoreName)
	if err := backupDriver.WaitForRestoreDeletion(ctx, restoreName, orgID,
		backupRestoreCompletionTimeoutMin*time.Minute,
		retrySeconds*time.Second); err != nil {
		return err
	}
	return nil
}
