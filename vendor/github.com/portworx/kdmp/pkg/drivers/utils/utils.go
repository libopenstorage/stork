package utils

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/aquilax/truncate"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/version"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	defaultPXNamespace = "kube-system"
	kdmpConfig         = "kdmp-config"
)

var (
	// ErrOutOfJobResources - out of job resource error
	ErrOutOfJobResources = errors.New("out of job resources")
)

// NamespacedName returns a name in form "<namespace>/<name>".
func NamespacedName(namespace, name string) string {
	v := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	return v.String()
}

// ParseJobID parses input string as namespaced name ("<namespace>/<name>").
func ParseJobID(id string) (namespace, name string, err error) {
	v := strings.SplitN(id, string(types.Separator), 2)
	if len(v) != 2 {
		return "", "", fmt.Errorf("invalid job id")
	}
	return v[0], v[1], nil
}

// IsJobCompleted checks if a kubernetes job is completed.
func IsJobCompleted(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// IsJobFailed checks if a kubernetes job is failed.
func IsJobFailed(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// IsJobPending checks if a kubernetes job is in pending state
func IsJobPending(j *batchv1.Job) bool {
	// Check if the pod is in running state
	pods, err := core.Instance().GetPods(
		j.Namespace,
		map[string]string{
			"job-name": j.Name,
		},
	)
	if err != nil {
		// Cannot determine job state
		return false
	} else if len(pods.Items) == 0 {
		return true
	}
	for _, c := range pods.Items[0].Status.ContainerStatuses {
		if c.State.Waiting != nil {
			// container is in waiting state
			return true
		}
	}
	return false
}

// ToJobStatus returns a job status for provided parameters.
func ToJobStatus(progress float64, errMsg string) *drivers.JobStatus {
	if len(errMsg) > 0 {
		return &drivers.JobStatus{
			State:  drivers.JobStateFailed,
			Reason: errMsg,
		}
	}

	if drivers.IsTransferCompleted(progress) {
		return &drivers.JobStatus{
			State:            drivers.JobStateCompleted,
			ProgressPercents: progress,
		}
	}

	return &drivers.JobStatus{
		State:            drivers.JobStateInProgress,
		ProgressPercents: progress,
	}
}

// GetConfigValue read configmap and return the value of the requested parameter
// If error in reading from configmap, we try reading from env variable
func GetConfigValue(key string) string {
	configMap, err := core.Instance().GetConfigMap(
		kdmpConfig,
		defaultPXNamespace,
	)
	if err != nil {
		logrus.Warnf("Failed in getting value for key [%v] from configmap[%v]", key, kdmpConfig)
		// try reading from the Env variable
		return os.Getenv(key)
	}
	return configMap.Data[key]
}

// ResticExecutorImage returns a docker image that contains resticexecutor binary.
func ResticExecutorImage() string {
	if customImage := strings.TrimSpace(os.Getenv(drivers.ResticExecutorImageKey)); customImage != "" {
		return customImage
	}
	// use a versioned docker image
	return strings.Join([]string{drivers.ResticExecutorImage, version.Get().GitVersion}, ":")
}

// ResticExecutorImageSecret returns an image pull secret for the resticexecutor image.
func ResticExecutorImageSecret() string {
	return strings.TrimSpace(os.Getenv(drivers.ResticExecutorImageSecretKey))
}

// KopiaExecutorImage returns a docker image that contains kopiaexecutor binary.
func KopiaExecutorImage() string {
	if customImage := strings.TrimSpace(GetConfigValue(drivers.KopiaExecutorImageKey)); customImage != "" {
		return customImage
	}
	// use a versioned docker image
	return strings.Join([]string{drivers.KopiaExecutorImage, version.Get().GitVersion}, ":")
}

// KopiaExecutorImageSecret returns an image pull secret for the resticexecutor image.
func KopiaExecutorImageSecret() string {
	return strings.TrimSpace(GetConfigValue(drivers.KopiaExecutorImageSecretKey))
}

// RsyncImage returns a docker image that contains rsync binary.
func RsyncImage() string {
	if customImage := strings.TrimSpace(os.Getenv(drivers.RsyncImageKey)); customImage != "" {
		return customImage
	}
	return drivers.RsyncImage
}

// RsyncImageSecret returns an image pull secret for the rsync image.
func RsyncImageSecret() string {
	return strings.TrimSpace(os.Getenv(drivers.RsyncImageSecretKey))
}

// RsyncCommandFlags allows to change rsync command flags.
func RsyncCommandFlags() string {
	return strings.TrimSpace(os.Getenv(drivers.RsyncFlags))
}

// RsyncOpenshiftSCC is used to set a custom openshift security context constraints for a rsync deployment.
func RsyncOpenshiftSCC() string {
	return strings.TrimSpace(os.Getenv(drivers.RsyncOpenshiftSCC))
}

// ToImagePullSecret converts a secret name to the ImagePullSecret struct.
func ToImagePullSecret(name string) []corev1.LocalObjectReference {
	if name == "" {
		return nil
	}
	return []corev1.LocalObjectReference{
		{
			Name: name,
		},
	}

}

// KopiaResourceRequirements returns ResourceRequirements for the kopiaexecutor container.
func KopiaResourceRequirements() (corev1.ResourceRequirements, error) {
	requestCPU := drivers.DefaultKopiaExecutorRequestCPU
	if customRequestCPU := os.Getenv(drivers.KopiaExecutorRequestCPU); customRequestCPU != "" {
		requestCPU = customRequestCPU
	}
	requestMem := drivers.DefaultKopiaExecutorRequestMemory
	if customRequestMemory := os.Getenv(drivers.KopiaExecutorRequestMemory); customRequestMemory != "" {
		requestMem = customRequestMemory
	}
	limitCPU := drivers.DefaultKopiaExecutorLimitCPU
	if customLimitCPU := os.Getenv(drivers.KopiaExecutorLimitCPU); customLimitCPU != "" {
		limitCPU = customLimitCPU
	}
	limitMem := drivers.DefaultKopiaExecutorLimitMemory
	if customLimitMemory := os.Getenv(drivers.KopiaExecutorLimitMemory); customLimitMemory != "" {
		limitMem = customLimitMemory
	}
	return toResourceRequirements(requestCPU, requestMem, limitCPU, limitMem)
}

// ResticResourceRequirements returns JobResourceRequirements for the executor container.
func ResticResourceRequirements() (corev1.ResourceRequirements, error) {
	requestCPU := drivers.DefaultResticExecutorRequestCPU
	if customRequestCPU := os.Getenv(drivers.ResticExecutorRequestCPU); customRequestCPU != "" {
		requestCPU = customRequestCPU
	}
	requestMem := drivers.DefaultResticExecutorRequestMemory
	if customRequestMemory := os.Getenv(drivers.ResticExecutorRequestMemory); customRequestMemory != "" {
		requestMem = customRequestMemory
	}
	limitCPU := drivers.DefaultResticExecutorLimitCPU
	if customLimitCPU := os.Getenv(drivers.ResticExecutorLimitCPU); customLimitCPU != "" {
		limitCPU = customLimitCPU
	}
	limitMem := drivers.DefaultResticExecutorLimitMemory
	if customLimitMemory := os.Getenv(drivers.ResticExecutorLimitMemory); customLimitMemory != "" {
		limitMem = customLimitMemory
	}
	return toResourceRequirements(requestCPU, requestMem, limitCPU, limitMem)
}

// RsyncResourceRequirements returns ResourceRequirements for the rsync container.
func RsyncResourceRequirements() (corev1.ResourceRequirements, error) {
	requestCPU := drivers.DefaultRsyncRequestCPU
	if customRequestCPU := os.Getenv(drivers.RsyncRequestCPU); customRequestCPU != "" {
		requestCPU = customRequestCPU
	}
	requestMem := drivers.DefaultRsyncRequestMemory
	if customRequestMemory := os.Getenv(drivers.RsyncRequestMemory); customRequestMemory != "" {
		requestMem = customRequestMemory
	}
	limitCPU := drivers.DefaultRsyncLimitCPU
	if customLimitCPU := os.Getenv(drivers.RsyncLimitCPU); customLimitCPU != "" {
		limitCPU = customLimitCPU
	}
	limitMem := drivers.DefaultRsyncLimitMemory
	if customLimitMemory := os.Getenv(drivers.RsyncLimitMemory); customLimitMemory != "" {
		limitMem = customLimitMemory
	}
	return toResourceRequirements(requestCPU, requestMem, limitCPU, limitMem)
}

func toResourceRequirements(requestCPU, requestMem, limitCPU, limitMem string) (corev1.ResourceRequirements, error) {
	requestCPUQ, err := resource.ParseQuantity(requestCPU)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("failed to parse %q requestCPU: %s", requestCPU, err)
	}
	requestMemQ, err := resource.ParseQuantity(requestMem)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("failed to parse %q requestMemory: %s", requestMem, err)
	}
	limitCPUQ, err := resource.ParseQuantity(limitCPU)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("failed to parse %q limitCPU: %s", limitCPU, err)
	}
	limitMemQ, err := resource.ParseQuantity(limitMem)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("failed to parse %q limitMemory: %s", limitMem, err)
	}
	return corev1.ResourceRequirements{
		Requests: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    requestCPUQ,
			corev1.ResourceMemory: requestMemQ,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    limitCPUQ,
			corev1.ResourceMemory: limitMemQ,
		},
	}, nil
}

// GetValidLabel - will validate the label to make sure the length is less than 63 and contains valid label format.
// If the length is greater then 63, it will truncate to 63 character.
func GetValidLabel(labelVal string) string {
	if len(labelVal) > validation.LabelValueMaxLength {
		labelVal = truncate.Truncate(labelVal, validation.LabelValueMaxLength, "", truncate.PositionEnd)
		// make sure the truncated value does not end with the hyphen.
		labelVal = strings.Trim(labelVal, "-")
		// make sure the truncated value does not end with the dot.
		labelVal = strings.Trim(labelVal, ".")
	}
	return labelVal
}
