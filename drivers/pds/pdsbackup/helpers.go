package pdsbackup

import (
	"context"
	"fmt"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/pkg/log"
)

type awsStorageClient struct {
	accessKey string
	secretKey string
	region    string
}

type azureStorageClient struct {
	accountName string
	accountKey  string
}

type gcpStorageClient struct {
	projectId string
}

func (awsObj *awsStorageClient) createBucket(bucketName string) error {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:      aws.String(awsObj.region),
			Credentials: credentials.NewStaticCredentials(awsObj.accessKey, awsObj.secretKey, ""),
		},
	})

	if err != nil {
		return fmt.Errorf("failed to initialize new session: %v", err)
	}

	client := s3.New(sess)
	bucketObj, err := client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if (aerr.Code() == s3.ErrCodeBucketAlreadyOwnedByYou) || (aerr.Code() == s3.ErrCodeBucketAlreadyExists) {
				log.Infof("Bucket: %v ,already exist.", bucketName)
				return nil
			} else {
				return fmt.Errorf("couldn't create bucket: %v", err)
			}

		}
	}

	log.Infof("[AWS]Successfully created the bucket. Info: %v", bucketObj)
	return nil
}

func (awsObj *awsStorageClient) deleteBucket(bucketName string) error {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:      aws.String(awsObj.region),
			Credentials: credentials.NewStaticCredentials(awsObj.accessKey, awsObj.secretKey, ""),
		},
	})

	if err != nil {
		return fmt.Errorf("failed to initialize new session: %v", err)
	}

	client := s3.New(sess)
	_, err = client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchBucket {
				log.Infof("[AWS] Bucket: %v doesn't exists.!!", bucketName)
				return nil
			} else {
				return fmt.Errorf("couldn't delete bucket: %v", err)
			}

		}

	}
	log.Infof("[AWS] Successfully deleted the bucket: %v", bucketName)
	return nil
}

func (azObj *azureStorageClient) createBucket(containerName string) error {
	cred, err := azblob.NewSharedKeyCredential(azObj.accountName, azObj.accountKey)
	if err != nil {
		return err
	}
	client, err := azblob.NewClientWithSharedKeyCredential(fmt.Sprintf("https://%s.blob.core.windows.net/", azObj.accountName), cred, nil)
	if err != nil {
		return fmt.Errorf("error -> %v", err.Error())
	}

	_, err = client.CreateContainer(context.TODO(), containerName, nil)
	if err != nil && strings.Contains(err.Error(), "ContainerAlreadyExists") {
		log.Infof("Container: %s, already exists.", containerName)
	} else if err != nil && !strings.Contains(err.Error(), "ContainerAlreadyExists") {
		return fmt.Errorf("error while creating azure container. Error - %v", err)
	} else {
		log.Infof("[Azure]Successfully created the container: %s", containerName)
	}
	return nil
}

func (azObj *azureStorageClient) deleteBucket(containerName string) error {
	cred, err := azblob.NewSharedKeyCredential(azObj.accountName, azObj.accountKey)
	if err != nil {
		return fmt.Errorf("error -> %v", err.Error())
	}
	client, err := azblob.NewClientWithSharedKeyCredential(fmt.Sprintf("https://%s.blob.core.windows.net/", azObj.accountName), cred, nil)
	if err != nil {
		return fmt.Errorf("error -> %v", err.Error())
	}
	_, err = client.DeleteContainer(context.TODO(), containerName, nil)
	if err != nil && strings.Contains(err.Error(), "not found") {
		log.Infof("[Azure]Container: %s not found!!", containerName)
	} else if err != nil && !strings.Contains(err.Error(), "not found") {
		return fmt.Errorf("error while creating azure container. Error - %v", err)
	} else {
		log.Infof("[Azure]Container: %s deleted successfully!!", containerName)
	}
	return nil
}

func (gcpObj *gcpStorageClient) createBucket(bucketName string) error {
	err := gcpObj.setGcpJsonPath()
	if err != nil {
		return err
	}
	err = gcpObj.createGcpJsonFile("/tmp/json")
	if err != nil {
		return err
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile("/tmp/json"))
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}

	if _, err := client.Bucket(bucketName).Attrs(ctx); err != nil {
		if err == storage.ErrBucketNotExist {
			if err := client.Bucket(bucketName).Create(ctx, gcpObj.projectId, nil); err != nil {
				return fmt.Errorf("failed to create bucket: %v", err)
			}
			log.Infof("Bucket created: gs://%s\n", bucketName)
		} else {
			apiErr, ok := err.(*googleapi.Error)
			if ok && apiErr.Code == 403 {
				return fmt.Errorf("access denied to bucket: %v", err)
			} else {
				return fmt.Errorf("failed to get bucket: %v", err)
			}
		}
	} else {
		log.Infof("Bucket already exists: gs://%s\n", bucketName)
	}

	return nil
}

func (gcpObj *gcpStorageClient) setGcpJsonPath() error {
	cm, err := core.Instance().GetConfigMap("custom-pds-qa-gcp-json-path", "default")
	if err != nil {
		return err
	}
	if _, ok := cm.Data["custom-pds-qa-gcp-json-path"]; ok {
		gcpJsonData := cm.Data["custom-pds-qa-gcp-json-path"]
		os.Setenv("GCP_JSON_PATH", gcpJsonData)
		return nil
	}
	return fmt.Errorf("key: custom-pds-qa-gcp-json-path doesn't exists in the gcp configmap")
}

func (gcpObj *gcpStorageClient) createGcpJsonFile(path string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error while creating the file -> %v", err)
	}
	defer f.Close()
	err = f.Truncate(0)
	if err != nil {
		return fmt.Errorf("error truncating file. Err: %v", err)
	}
	_, err = f.WriteString(os.Getenv("GCP_JSON_PATH"))
	if err != nil {
		return fmt.Errorf("error while writing the data to file -> %v", err)
	}
	return nil
}

func (gcpObj *gcpStorageClient) deleteBucket(bucketName string) error {
	ctx := context.Background()
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/json")
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	bucketClient := client.Bucket(bucketName)
	exist, err := bucketClient.Attrs(ctx)
	if err != nil {
		return fmt.Errorf("unexpected error occured: %v", err)
	}
	if exist != nil {
		err := bucketClient.Delete(ctx)
		if err != nil {
			return fmt.Errorf("Bucket(%v).Delete: %v", bucketName, err)
		}
		log.Infof("[GCP]Successfully deleted the Bucket: %v", bucketName)
	} else {
		log.Infof("[GCP]Bucket: %v doesn't exist.", bucketName)
	}
	return nil
}
