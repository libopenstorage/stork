package preflight

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/opsworks"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/libopenstorage/cloudops"
	awsops "github.com/libopenstorage/cloudops/aws"
	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/libopenstorage/operator/pkg/util/k8s"
)

const (
	eksDistribution = "eks"
	volumePrefix    = "px-preflight-"
	labelClusterUID = "PX_PREFLIGHT_CLUSTER_UID"
	labelVolumeName = "Name"

	// dryRunErrMsg dry run error response if succeeded, otherwise will be UnauthorizedOperation
	dryRunErrMsg = "DryRunOperation"

	awsAccessKeyEnvName = "AWS_ACCESS_KEY_ID"
	awsSecretKeyEnvName = "AWS_SECRET_ACCESS_KEY"
)

var (
	dryRunOption = map[string]string{cloudops.DryRunOption: ""}
)

type aws struct {
	checker
	ops            cloudops.Ops
	zone           string
	credentialHash string
}

func (a *aws) initCloudOps(cluster *corev1.StorageCluster) error {
	// Set env vars for user provided credentials: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
	// only set env vars when client init is needed
	if err := a.setAWSCredentialEnvVars(cluster); err != nil {
		unsetAWSCredentialEnvVars()
		return err
	}

	if a.ops != nil {
		return nil
	}

	// Initialize aws client for cloud drive permission checks
	client, err := awsops.NewClient()
	unsetAWSCredentialEnvVars()
	if err != nil {
		return err
	}
	a.ops = client

	instance, err := client.InspectInstance(client.InstanceID())
	if err != nil {
		return err
	}
	a.zone = instance.Zone
	return nil
}

// setAWSCredentialEnvVars sets credential env vars to init aws client if provided in StorageCluster
// env vars are only set if client creation is needed:
// 1. they are provided but client is not created yet
// 2. they got updated
func (a *aws) setAWSCredentialEnvVars(cluster *corev1.StorageCluster) error {
	// Check if user provided credentials exist
	var accessKeyEnv, secretKeyEnv *v1.EnvVar
	for _, env := range cluster.Spec.Env {
		if env.Name == awsAccessKeyEnvName {
			accessKeyEnv = env.DeepCopy()
		} else if env.Name == awsSecretKeyEnvName {
			secretKeyEnv = env.DeepCopy()
		}
	}
	if accessKeyEnv == nil && secretKeyEnv == nil {
		// No credential provided or credentials are removed
		logrus.Debugf("no aws credentials provided, will use instance privileges instead")
		if a.credentialHash != "" {
			a.credentialHash = ""
			a.ops = nil
			unsetAWSCredentialEnvVars()
		}
		return nil
	} else if accessKeyEnv == nil || secretKeyEnv == nil {
		return fmt.Errorf("both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY need to be provided")
	}

	// Get the credentials from secrets or values
	accessKey, err := k8s.GetValueFromEnvVar(context.TODO(), a.k8sClient, accessKeyEnv, cluster.Namespace)
	if err != nil {
		return err
	}
	secretKey, err := k8s.GetValueFromEnvVar(context.TODO(), a.k8sClient, secretKeyEnv, cluster.Namespace)
	if err != nil {
		return err
	}
	if accessKey == "" || secretKey == "" {
		return fmt.Errorf("both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY need to be provided")
	}

	// Check whether to set the env vars for aws client creation
	credentialHash := fmt.Sprintf("%x", md5.Sum([]byte(accessKey+secretKey)))
	if a.ops != nil &&
		a.credentialHash == credentialHash {
		return nil
	}

	// Client init required, set environment variables using credentials above
	if err := os.Setenv(awsAccessKeyEnvName, accessKey); err != nil {
		return err
	}
	if err := os.Setenv(awsSecretKeyEnvName, secretKey); err != nil {
		return err
	}
	a.credentialHash = credentialHash
	a.ops = nil

	logrus.Infof("environment variables set using AWS credentials provided")
	return nil
}

func unsetAWSCredentialEnvVars() {
	if err := os.Unsetenv(awsAccessKeyEnvName); err != nil {
		logrus.WithError(err).Warningf("failed to reset env var %s", awsAccessKeyEnvName)
	}
	if err := os.Unsetenv(awsSecretKeyEnvName); err != nil {
		logrus.WithError(err).Warningf("failed to reset env var %s", awsSecretKeyEnvName)
	}
}

func (a *aws) getEC2VolumeTemplate() *ec2.Volume {
	volTypeGp2 := opsworks.VolumeTypeGp2
	volSize := int64(1)
	return &ec2.Volume{
		Size:             &volSize,
		VolumeType:       &volTypeGp2,
		AvailabilityZone: &a.zone,
	}
}

func (a *aws) CheckCloudDrivePermission(cluster *corev1.StorageCluster) error {
	// Only init the aws client when needed
	if err := a.initCloudOps(cluster); err != nil {
		return err
	}
	// check the permission here by doing dummy drive operations
	logrus.Info("preflight starting eks cloud permission check")

	// List volume on cluster UID first, Describe permission checked here first
	result, err := a.ops.Enumerate(nil, map[string]string{labelClusterUID: string(cluster.UID)}, "")
	if err != nil {
		logrus.Errorf("preflight failed to enumerate volumes: %v", err)
		return err
	}
	volumes := result[cloudops.SetIdentifierNone]

	// Dry run requires an existing volume, so create one or get an old one, delete in the end
	var vol *ec2.Volume
	if len(volumes) > 0 {
		// Reuse old volume for permission checks
		logrus.Infof("preflight found %v volumes, using the first one for permission check", len(volumes))
		vol = volumes[0].(*ec2.Volume)
	} else {
		// Create a new volume
		volName := volumePrefix + cluster.Name + "-" + uuid.New()
		labels := map[string]string{
			labelVolumeName: volName,
			labelClusterUID: string(cluster.UID),
		}
		v, err := a.ops.Create(a.getEC2VolumeTemplate(), labels, nil)
		if err != nil {
			logrus.WithError(err).Errorf("preflight failed to create eks volume %s", volName)
			return err
		}
		volumes = append(volumes, v)
		vol = v.(*ec2.Volume)
	}

	// Dry run the rest operations
	// Attach volume
	// without dry run since container doesn't have access to /dev/ directory on host, it will fail to return dev path
	if _, err := a.ops.Attach(*vol.VolumeId, dryRunOption); !dryRunSucceeded(err) {
		return fmt.Errorf("preflight volume attach dry run failed: %v", err)
	}

	// Expand volume
	if _, err := a.ops.Expand(*vol.VolumeId, uint64(2), dryRunOption); !dryRunSucceeded(err) {
		return fmt.Errorf("preflight volume expansion dry run failed: %v", err)
	}

	// Apply and remove tags
	tags := map[string]string{
		"foo": "bar",
	}
	if err := a.ops.ApplyTags(*vol.VolumeId, tags, dryRunOption); !dryRunSucceeded(err) {
		return fmt.Errorf("preflight volume tag dry run failed: %v", err)
	}
	if err := a.ops.RemoveTags(*vol.VolumeId, tags, dryRunOption); !dryRunSucceeded(err) {
		return fmt.Errorf("preflight volume remove tag dry run failed: %v", err)
	}

	// Detach volume
	if err := a.ops.Detach(*vol.VolumeId, dryRunOption); !dryRunSucceeded(err) {
		return fmt.Errorf("preflight volume detach dry run failed: %v", err)
	}

	// Delete volume
	// Check permission first then do the actual deletion in cleanup phase
	if err := a.ops.Delete(*vol.VolumeId, dryRunOption); !dryRunSucceeded(err) {
		return fmt.Errorf("preflight volume delete dry run failed: %v", err)
	}

	// Do a cleanup when preflight passed, as it's guaranteed to have permissions for volume deletion
	// Will delete volumes created in previous attempts as well by cluster ID label
	a.cleanupPreflightVolumes(volumes)
	logrus.Infof("preflight check for eks cloud permission passed")
	return nil
}

func (a *aws) cleanupPreflightVolumes(volumes []interface{}) {
	logrus.Infof("preflight cleaning up volumes created in permission check, %v volumes to delete", len(volumes))
	for _, v := range volumes {
		vol := *v.(*ec2.Volume).VolumeId
		err := a.ops.Delete(vol, nil)
		if err != nil {
			logrus.Warnf("preflight failed to delete volume %s: %v", vol, err)
		}
	}
}

func dryRunSucceeded(err error) bool {
	return err != nil && strings.Contains(err.Error(), dryRunErrMsg)
}
