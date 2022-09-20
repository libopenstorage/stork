package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/libopenstorage/secrets/aws/credentials"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/objectstore/common"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

func getSession(backupLocation *stork_api.BackupLocation) (*session.Session, error) {
	// AWS SDK fetches the correct endpoint based on region provided if endpoint is passed empty
	var endpoint string
	if backupLocation.Location.S3Config.Endpoint == "s3.amazonaws.com" {
		endpoint = ""
	} else {
		endpoint = backupLocation.Location.S3Config.Endpoint
	}
	awsCreds, err := credentials.NewAWSCredentials(
		backupLocation.Location.S3Config.AccessKeyID,
		backupLocation.Location.S3Config.SecretAccessKey,
		"",
		backupLocation.Location.S3Config.UseIam, // runningOnEc2 when set ec2 role credentials will be used
	)
	if err != nil {
		return nil, err
	}
	creds, err := awsCreds.Get()
	if err != nil {
		return nil, err
	}

	return session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      creds,
		Region:           aws.String(backupLocation.Location.S3Config.Region),
		DisableSSL:       aws.Bool(backupLocation.Location.S3Config.DisableSSL),
		S3ForcePathStyle: aws.Bool(true),
	})
}

// GetBucket gets a reference to the bucket for that backup location
func GetBucket(backupLocation *stork_api.BackupLocation) (*blob.Bucket, error) {
	sess, err := getSession(backupLocation)
	if err != nil {
		return nil, err
	}
	return s3blob.OpenBucket(context.Background(), sess, backupLocation.Location.Path, nil)
}

// CreateBucket creates a bucket for the bucket location
func CreateBucket(backupLocation *stork_api.BackupLocation) error {
	sess, err := getSession(backupLocation)
	if err != nil {
		return err
	}

	input := &s3.CreateBucketInput{
		Bucket: &backupLocation.Location.Path,
	}
	_, err = s3.New(sess).CreateBucket(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == s3.ErrCodeBucketAlreadyOwnedByYou {
				return nil
			}
		}
	}
	return err
}

// GetObjLockInfo fetches the object lock configuration of a S3 bucket
func GetObjLockInfo(backupLocation *stork_api.BackupLocation) (*common.ObjLockInfo, error) {
	sess, err := getSession(backupLocation)
	if err != nil {
		return nil, err
	}

	input := &s3.GetObjectLockConfigurationInput{
		Bucket: &backupLocation.Location.Path,
	}

	objLockInfo := &common.ObjLockInfo{}
	out, err := s3.New(sess).GetObjectLockConfiguration(input)
	if err != nil {
		logrus.Warnf("GetObjLockInfo: GetObjectLockConfiguration failed with: %v", err)
		if awsErr, ok := err.(awserr.Error); ok {
			logrus.Warnf("GetObjLockInfo: GetObjectLockConfiguration awsErr.Code %v", awsErr.Code())
			// When a Minio server doesn't have object-lock implemented then above API
			// throws following error codes depending on version it runs for normal buckets
			//	1. "ObjectLockConfigurationNotFoundError"
			//      2. "MethodNotAllowed"
			// Similarly in case of AWS, we need to ignore "NoSuchBucket" so that
			// px-backup/stork can create the bucket on behalf user when validation flag is not set.
			// With cloudian objectstore, we saw the error as "ObjectLockConfigurationNotFound"
			// With Netapp Trident, we saw the error as "NotImplemented"
			if awsErr.Code() == "ObjectLockConfigurationNotFoundError" ||
				awsErr.Code() == "MethodNotAllowed" ||
				awsErr.Code() == "ObjectLockConfigurationNotFound" ||
				awsErr.Code() == "NotImplemented" ||
				awsErr.Code() == "NoSuchBucket" {
				// for a non-objectlocked bucket we needn't throw error
				return objLockInfo, nil
			}
		}
		return nil, err
	}
	if (out != nil) && (out.ObjectLockConfiguration != nil) {
		logrus.Warnf("GetObjLockInfo: out.ObjectLockConfiguration.ObjectLockEnabled: %v", aws.StringValue(out.ObjectLockConfiguration.ObjectLockEnabled))
		if aws.StringValue(out.ObjectLockConfiguration.ObjectLockEnabled) == "Enabled" {
			objLockInfo.LockEnabled = true
		} else {
			logrus.Infof("GetObjLockInfo ObjectLockConfiguration is empty: %v", out.ObjectLockConfiguration)
			// For some of the objectstore like FB and dell ECS, GetObjectLockConfiguration
			// will return empty objectlockconfiguration instead of nil or error
			return objLockInfo, nil
		}
		if out.ObjectLockConfiguration.Rule != nil &&
			out.ObjectLockConfiguration.Rule.DefaultRetention != nil {
			objLockInfo.LockMode = aws.StringValue(out.ObjectLockConfiguration.Rule.DefaultRetention.Mode)
			objLockInfo.RetentionPeriodYears = aws.Int64Value(out.ObjectLockConfiguration.Rule.DefaultRetention.Years)
			objLockInfo.RetentionPeriodDays = aws.Int64Value(out.ObjectLockConfiguration.Rule.DefaultRetention.Days)
		} else {
			//This is an invalid object-lock config, no default-retention but object-loc enabled
			logrus.Errorf("GetObjLockInfo: invalid config: object lock is enabled but default retention period is not set on the bucket")
			objLockInfo.LockEnabled = false
			return nil, fmt.Errorf("invalid config: object lock is enabled but default retention period is not set on the bucket")
		}
	}
	// This debug statement will not be executed as both err and out can not be nil at the same time.
	// Adding it, just in case, we hit it.
	logrus.Infof("GetObjLockInfo: returning objLockInfo: %v - err %v", objLockInfo, err)
	return objLockInfo, err
}
