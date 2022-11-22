package backuputils

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
)

// backupStoreClient interface
type backupStoreClient interface {
	createBucket() error
	deleteBucket() error
}

// awsStorageClient struct
type awsStorageClient struct {
	accessKey string
	secretKey string
	region    string
}

// azureStorageClient struct
type azureStorageClient struct {
	accountName string
	accountKey  string
}

// gcpStorageClient struct
type gcpStorageClient struct {
	projectId string
	jsongPath string
}

// s3CompatibleStorageClient struct
type s3CompatibleStorageClient struct {
	accessKey string
	secretKey string
	region    string
	endpoint  string
}

// createBucket creates aws storage.
func (awsObj *awsStorageClient) createBucket(bucketName string) error {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:      aws.String(awsObj.region),
			Credentials: credentials.NewStaticCredentials(awsObj.accessKey, awsObj.secretKey, ""),
		},
	})

	if err != nil {
		logrus.Errorf("Failed to initialize new session: %v", err)
		return err
	}

	client := s3.New(sess)
	bucketObj, err := client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if (aerr.Code() == s3.ErrCodeBucketAlreadyOwnedByYou) || (aerr.Code() == s3.ErrCodeBucketAlreadyExists) {
				logrus.Infof("Bucket: %v ,already exist.", bucketName)
				return nil
			} else {
				logrus.Errorf("Couldn't create bucket: %v", err)
				return err
			}

		}

	}

	logrus.Infof("[AWS]Successfully created the bucket. Info: %v", bucketObj)
	return nil
}

// deleteBucket deletes aws storage.
func (awsObj *awsStorageClient) deleteBucket(bucketName string) error {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:      aws.String(awsObj.region),
			Credentials: credentials.NewStaticCredentials(awsObj.accessKey, awsObj.secretKey, ""),
		},
	})

	if err != nil {
		logrus.Infof("Failed to initialize new session: %v", err)
		return err
	}

	client := s3.New(sess)
	_, err = client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchBucket {
				logrus.Infof("[AWS] Bucket: %v doesn't exists.!!", bucketName)
				return nil
			} else {
				logrus.Errorf("Couldn't delete bucket: %v", err)
				return err
			}

		}

	}
	logrus.Infof("[AWS] Successfully deleted the bucket: %v", bucketName)
	return nil
}

// Implement Azure storage creation/ delete with sdk having go support < 1.16

// createBucket creates google storage.
func (gcpObj *gcpStorageClient) createBucket(bucketName string) error {
	ctx := context.Background()
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", gcpObj.jsongPath)
	client, err := storage.NewClient(context.Background())
	if err != nil {
		logrus.Fatalf("Failed to create client: %v", err)
	}

	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	bucketClient := client.Bucket(bucketName)
	exist, err := bucketClient.Attrs(ctx)

	if exist != nil {
		if err != nil {
			logrus.Errorf("Unexpected error occured: %v", err)
			return err
		} else {
			logrus.Infof("[GCP] Bucket: %v already exists.!!", bucketName)
		}
	} else {
		err := bucketClient.Create(ctx, gcpObj.projectId, nil)
		if err != nil {
			logrus.Errorf("Bucket(%v).Create: %v", bucketName, err)
			return err
		}
		logrus.Infof("[GCP] Successfully create the Bucket: %v", bucketName)
	}
	return nil

}

// deleteBucket deletes google storage.
func (gcpObj *gcpStorageClient) deleteBucket(bucketName string) error {
	ctx := context.Background()
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", gcpObj.jsongPath)
	client, err := storage.NewClient(context.Background())
	if err != nil {
		logrus.Fatalf("Failed to create client: %v", err)
	}

	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	bucketClient := client.Bucket(bucketName)
	exist, err := bucketClient.Attrs(ctx)

	if exist != nil {
		if err != nil {
			logrus.Errorf("Unexpected error occured: %v", err)
			return err
		} else {
			err := bucketClient.Delete(ctx)
			if err != nil {
				logrus.Errorf("Bucket(%v).Delete: %v", bucketName, err)
				return err
			}
			logrus.Infof("[GCP]Successfully deleted the Bucket: %v", bucketName)
		}
	} else {
		logrus.Infof("[GCP]Bucket: %v doesn't exist.", bucketName)
	}
	return nil

}

// createBucket creates S3 compatible bucket.
func (minioObj *s3CompatibleStorageClient) createBucket(bucketName string) error {
	minioClient, err := minio.New(minioObj.endpoint, &minio.Options{
		Creds:  minioCred.NewStaticV4(minioObj.accessKey, minioObj.secretKey, ""),
		Secure: true,
	})
	if err != nil {
		logrus.Error(err)
	}
	found, err := minioClient.BucketExists(context.Background(), bucketName)
	if err != nil {
		logrus.Error(err)
		return err
	}
	if !found {
		err = minioClient.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{Region: minioObj.region})
		if err != nil {
			fmt.Println(err)
			return err
		}
		logrus.Infof("[MINIO] Successfully created bucket:%v.", bucketName)
	} else {
		logrus.Infof("[MINIO] Bucket:%v already exists.", bucketName)
	}
	return nil

}

// deleteBucket deletes S3 compatible bucket.
func (minioObj *s3CompatibleStorageClient) deleteBucket(bucketName string) error {
	minioClient, err := minio.New(minioObj.endpoint, &minio.Options{
		Creds:  minioCred.NewStaticV4(minioObj.accessKey, minioObj.secretKey, ""),
		Secure: true,
	})
	if err != nil {
		logrus.Error(err)
	}
	found, err := minioClient.BucketExists(context.Background(), bucketName)
	if err != nil {
		logrus.Error(err)
		return err
	}
	if found {
		err = minioClient.RemoveBucket(context.Background(), bucketName)
		if err != nil {
			fmt.Println(err)
			return err
		}
		logrus.Infof("[MINIO] Successfully deleted the bucket: %v", bucketName)
	} else {
		logrus.Infof("[MINIO] Bucket:%v doesn't exist.", bucketName)
	}
	return nil

}
