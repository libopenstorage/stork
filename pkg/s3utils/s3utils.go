package s3utils

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var (
	expect       = gomega.Expect
	haveOccurred = gomega.HaveOccurred
	equal        = gomega.Equal
)

// Object S3 bucket object
type Object struct {
	Key          string
	Size         uint64
	LastModified time.Time
}

// S3Client client information
type S3Client struct {
	mu               *sync.Mutex
	renewalInrogress bool
	client           *s3.S3
	bucket           string
	endPoints        []string
	accessKeyID      string
	secretAccessKey  string
	region           string
	secure           bool
	disablePathStyle bool
	useProxy         bool
	proxy            string
}

// GetAWSDetailsFromEnv returns AWS details
func GetAWSDetailsFromEnv() (id string, secret string, endpoint string,
	s3Region string, disableSSLBool bool) {

	// TODO: add separate function to return cred object based on type
	id = os.Getenv("S3_AWS_ACCESS_KEY_ID")
	if id == "" {
		id = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	expect(id).NotTo(equal(""),
		"S3_AWS_ACCESS_KEY_ID Environment variable should not be empty")

	secret = os.Getenv("S3_AWS_SECRET_ACCESS_KEY")
	if secret == "" {
		secret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	expect(secret).NotTo(equal(""),
		"S3_AWS_SECRET_ACCESS_KEY Environment variable should not be empty")

	endpoint = os.Getenv("S3_ENDPOINT")
	expect(endpoint).NotTo(equal(""),
		"S3_ENDPOINT Environment variable should not be empty")

	s3Region = os.Getenv("S3_REGION")
	expect(s3Region).NotTo(equal(""),
		"S3_REGION Environment variable should not be empty")

	disableSSL := os.Getenv("S3_DISABLE_SSL")
	expect(disableSSL).NotTo(equal(""),
		"S3_DISABLE_SSL Environment variable should not be empty")

	disableSSLBool, err := strconv.ParseBool(disableSSL)
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("S3_DISABLE_SSL=%s is not a valid boolean value", disableSSL))

	return id, secret, endpoint, s3Region, disableSSLBool
}

// GetTimeStamp date/time path
func GetTimeStamp(getPreviousFolder bool) string {
	tnow := time.Now()
	if getPreviousFolder {
		tnow = tnow.Add(-1 * time.Hour)
	}
	return fmt.Sprintf("%d_%02d_%02d/%02d_00_00", tnow.Year(), tnow.Month(), tnow.Day(), tnow.Hour())
}

// GetS3Objects lists the objects in S3
func GetS3Objects(clusterID string, nodeName string, getPreviousFolder bool) ([]Object, error) {
	id, secret, endpoint, s3Region, disableSSLBool := GetAWSDetailsFromEnv()
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(id, secret, ""),
		Region:           aws.String(s3Region),
		DisableSSL:       aws.Bool(disableSSLBool),
		S3ForcePathStyle: aws.Bool(true),
	},
	)
	expect(err).NotTo(haveOccurred(),
		fmt.Sprintf("Failed to get S3 session to create bucket. Error: [%v]", err))

	S3Client := s3.New(sess)
	bucket := os.Getenv("DIAGS_BUCKET")
	prefix := fmt.Sprintf("%s/%s/%s", clusterID, nodeName, GetTimeStamp(getPreviousFolder))
	logrus.Debugf("Looking for files under folder %s", prefix)
	input := &s3.ListObjectsInput{
		Bucket: &bucket,
		Prefix: &prefix,
	}
	objs, err := S3Client.ListObjects(input)
	if err != nil {
		return nil, fmt.Errorf("Error in getting details from S3 %v", err)
	}
	var objects []Object
	for _, obj := range objs.Contents {
		object := Object{
			Key:          aws.StringValue(obj.Key),
			Size:         uint64(aws.Int64Value(obj.Size)),
			LastModified: aws.TimeValue(obj.LastModified),
		}
		objects = append(objects, object)
	}
	return objects, nil
}
