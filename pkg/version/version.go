package version

import (
	"context"
	"fmt"
	"regexp"

	version "github.com/hashicorp/go-version"
	kSnapshotClient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	coreops "github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// Version will be overridden with the current version at build time using the -X linker flag
var Version string

var (
	kbVerRegex = regexp.MustCompile(`^(v\d+\.\d+\.\d+)(.*)`)
)

const (
	k8sMinVersionCSIDriverV1      = "1.22"
	k8sMinVersionVolumeSnapshotV1 = "1.20"
	K8sMinVersionWebhookv1        = "1.22"
)

// RequiresV1Registration returns true if crd needs to be registered as apiVersion V1
func RequiresV1Registration() (bool, error) {
	k8sVersion, _, err := GetFullVersion()
	if err != nil {
		return false, err
	}
	k8sVer1_16, err := version.NewVersion("1.16")
	if err != nil {
		return false, err

	}
	if k8sVersion.GreaterThanOrEqual(k8sVer1_16) {
		return true, nil
	}
	return false, nil
}

// RequiresV1Webhooks returns true if V1 version of webhook object is needed
func RequiresV1Webhooks() (bool, error) {
	clusterK8sVersion, _, err := GetFullVersion()
	if err != nil {
		return false, err
	}
	requiredK8sVer, err := version.NewVersion(K8sMinVersionWebhookv1)
	if err != nil {
		return false, err

	}
	if clusterK8sVersion.GreaterThanOrEqual(requiredK8sVer) {
		return true, nil
	}
	return false, nil
}

// RequiresV1CSIdriver returns true if V1 version of CSIdriver APIs need to be called
func RequiresV1CSIdriver() (bool, error) {
	clusterK8sVersion, _, err := GetFullVersion()
	if err != nil {
		return false, err
	}
	requiredK8sVer, err := version.NewVersion(k8sMinVersionCSIDriverV1)
	if err != nil {
		return false, err

	}
	if clusterK8sVersion.GreaterThanOrEqual(requiredK8sVer) {
		return true, nil
	}
	return false, nil
}

// RequiresV1VolumeSnapshot returns true if V1 version of VolumeSnapshot APIs need to be called
func RequiresV1VolumeSnapshot() (bool, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return false, err
	}
	cs, err := kSnapshotClient.NewForConfig(config)
	if err != nil {
		return false, err
	}
	_, err = cs.SnapshotV1().VolumeSnapshots("").List(context.TODO(), metav1.ListOptions{})
	if err != nil && !k8s_errors.IsNotFound(err) {
		logrus.Errorf("Failed to get VolumeSnapshot v1 version error: %s", err)
		return false, err
	} else if k8s_errors.IsNotFound(err) {
		// Try for v1beta1
		_, err := cs.SnapshotV1beta1().VolumeSnapshots("").List(context.TODO(), metav1.ListOptions{})
		if err != nil && !k8s_errors.IsNotFound(err) {
			logrus.Errorf("Failed to get VolumeSnapshot v1beta1 version CRD error: %s", err)
			return false, err
		} else if k8s_errors.IsNotFound(err) {
			logrus.Warnf("VolumeSnapshot CRDs are not installed in the cluster, Please install appropriate version of volumesnapshot CRDs: %s", err)
			// Not attempting for v1alpha1 CRD search, it is too old to adopt
			// Returning error as nil to keep the previous behavior unchanged, which doesn't bail out in the absence of CRDs
			return false, nil
		}
		//Found v1beta1 CRD
		return false, nil
	}
	//Found the v1 CRD
	return true, nil
}

// GetFullVersion returns the full kubernetes server version
func GetFullVersion() (*version.Version, string, error) {
	k8sVersion, err := coreops.Instance().GetVersion()
	if err != nil {
		return nil, "", fmt.Errorf("unable to get kubernetes version: %v", err)
	}
	matches := kbVerRegex.FindStringSubmatch(k8sVersion.GitVersion)
	if len(matches) < 2 {
		return nil, "", fmt.Errorf("invalid kubernetes version received: %v", k8sVersion.GitVersion)
	}

	ver, err := version.NewVersion(matches[1])
	if len(matches) == 3 {
		return ver, matches[2], err
	}
	return ver, "", err
}

// GetVersion returns the kubernetes server version
func GetVersion() (*version.Version, error) {
	ver, _, err := GetFullVersion()
	return ver, err
}
