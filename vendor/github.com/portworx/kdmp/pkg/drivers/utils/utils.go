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
	// ErrJobAlreadyRunning - Already a job is running for the given instance of PVC
	ErrJobAlreadyRunning = errors.New("job Already Running")
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

// IsJobOrNodeFailed checks if a kubernetes job is failed, also check the node status, where the job is schedule
func IsJobOrNodeFailed(j *batchv1.Job) (bool, bool) {
	// Get the node name from job spec.
	nodeName := j.Spec.Template.Spec.NodeName
	if nodeName != "" {
		err := core.Instance().IsNodeReady(nodeName)
		if err != nil {
			return false, true
		}
	}
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true, false
		}
	}
	return false, false
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
	// some time even though the pod is created, initially ContainerStatuses is not available for sometime
	if len(pods.Items[0].Status.ContainerStatuses) == 0 {
		return true
	}
	for _, c := range pods.Items[0].Status.ContainerStatuses {
		if c.State.Waiting != nil || c.State.Running != nil {
			return true
		}
	}
	return false
}

// JobNodeExists checks if the node associated with the job spec exists in the cluster
func JobNodeExists(j *batchv1.Job) error {
	// Get the node name from job spec.
	nodeName := j.Spec.Template.Spec.NodeName
	if len(nodeName) > 0 {
		nodes, err := core.Instance().GetNodes()
		if err != nil {
			logrus.Errorf("getting all nodes failed with error: %v", err)
			// Not returning the error, so that the job does not get marked as failed in case of flaky errors
			return nil
		}
		found := false
		for _, node := range nodes.Items {
			if node.Name == nodeName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("node %s does not exist anymore", nodeName)
		}
	}
	return nil
}

// FetchJobContainerRestartCount fetches job pod restart count
func FetchJobContainerRestartCount(j *batchv1.Job) (int32, error) {
	// Check if the pod is in running state
	pods, err := core.Instance().GetPods(
		j.Namespace,
		map[string]string{
			"job-name": j.Name,
		},
	)
	if err != nil {
		// Cannot determine job state
		errMsg := fmt.Sprintf("cannot determine job state for: %v/%v", j.Namespace, j.Name)
		logrus.Errorf("%v", errMsg)
		return 0, fmt.Errorf(errMsg)
	} else if len(pods.Items) == 0 {
		logrus.Debugf("no pods in job spec: %v/%v", j.Namespace, j.Name)
		return 0, nil
	}
	if len(pods.Items[0].Status.ContainerStatuses) == 0 {
		logrus.Debugf("container status not present in pod - job: %v/%v", j.Namespace, j.Name)
		return 0, nil
	}

	return (pods.Items[0].Status.ContainerStatuses[0].RestartCount), nil
}

// ToJobStatus returns a job status for provided parameters.
func ToJobStatus(progress float64, errMsg string, jobStatus batchv1.JobConditionType) *drivers.JobStatus {
	if len(errMsg) > 0 {
		return &drivers.JobStatus{
			State:  drivers.JobStateFailed,
			Reason: errMsg,
			Status: jobStatus,
		}
	}

	if drivers.IsTransferCompleted(progress) {
		return &drivers.JobStatus{
			State:            drivers.JobStateCompleted,
			ProgressPercents: progress,
			Status:           jobStatus,
		}
	}

	return &drivers.JobStatus{
		State:            drivers.JobStateInProgress,
		ProgressPercents: progress,
		Status:           jobStatus,
	}
}

// GetConfigValue read configmap and return the value of the requested parameter
// If error in reading from configmap, we try reading from env variable
func GetConfigValue(cm, ns, key string) string {
	configMap, err := core.Instance().GetConfigMap(
		cm,
		ns,
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
func KopiaExecutorImage(configMap, ns string) string {
	if customImage := strings.TrimSpace(GetConfigValue(configMap, ns, drivers.KopiaExecutorImageKey)); customImage != "" {
		return customImage
	}
	// use a versioned docker image
	return strings.Join([]string{drivers.KopiaExecutorImage, version.Get().GitVersion}, ":")
}

// KopiaExecutorImageSecret returns an image pull secret for the resticexecutor image.
func KopiaExecutorImageSecret(configMap, ns string) string {
	return strings.TrimSpace(GetConfigValue(configMap, ns, drivers.KopiaExecutorImageSecretKey))
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
func KopiaResourceRequirements(configMap, ns string) (corev1.ResourceRequirements, error) {
	requestCPU := strings.TrimSpace(GetConfigValue(configMap, ns, drivers.KopiaExecutorRequestCPU))
	if requestCPU == "" {
		requestCPU = drivers.DefaultKopiaExecutorRequestCPU
	}

	requestMem := strings.TrimSpace(GetConfigValue(configMap, ns, drivers.KopiaExecutorRequestMemory))
	if requestMem == "" {
		requestMem = drivers.DefaultKopiaExecutorRequestMemory
	}

	limitCPU := strings.TrimSpace(GetConfigValue(configMap, ns, drivers.KopiaExecutorLimitCPU))
	if limitCPU == "" {
		limitCPU = drivers.DefaultKopiaExecutorLimitCPU
	}

	limitMem := strings.TrimSpace(GetConfigValue(configMap, ns, drivers.KopiaExecutorLimitMemory))
	if limitMem == "" {
		limitMem = drivers.DefaultKopiaExecutorLimitMemory
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
