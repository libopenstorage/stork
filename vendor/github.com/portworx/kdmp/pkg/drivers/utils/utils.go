package utils

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/aquilax/truncate"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/version"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	defaultPXNamespace = "kube-system"
	kdmpConfig         = "kdmp-config"
	// TriggeredFromStork - denotes the kopia job is triggered from stork module
	TriggeredFromStork = "stork"
	// TriggeredFromPxBackup - denotes the kopia job is triggered from px-backup module
	TriggeredFromPxBackup                  = "px-backup"
	kopiaExecutorImageRegistryEnvVar       = "KOPIA-EXECUTOR-IMAGE-REGISTRY"
	kopiaExecutorImageRegistrySecretEnvVar = "KOPIA-EXECUTOR-IMAGE-REGISTRY-SECRET"
	// AdminNamespace - kube-system namespace, where privilige pods will be deployed for live kopiabackup.
	AdminNamespace    = "kube-system"
	imageSecretPrefix = "image-secret"
	// CredSecret - credential secret prefix
	CredSecret = "cred-secret"
	// ImageSecret - image secret prefix
	ImageSecret = "image-secret"
	// CertSecret - cert secret prefix
	CertSecret = "cert-secret"
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

// GetImageRegistryFromDeployment - extract image registry and image registry secret from deployment spec
func GetImageRegistryFromDeployment(name, namespace string) (string, string, error) {
	deploy, err := apps.Instance().GetDeployment(name, namespace)
	if err != nil {
		return "", "", err
	}
	imageFields := strings.Split(deploy.Spec.Template.Spec.Containers[0].Image, "/")
	// Here the assumption is that the image format will be <registry-name>/<extra-dir-name>/<repo-name>/image:tag
	// or <repo-name>/image:tag or <registry-name>/<repo-name>/<extra-dir-name>/image:tag).
	// Customer might have extra dirs before the repo-name as mentioned above
	registryFields := imageFields[0 : len(imageFields)-1]
	registry := strings.Join(registryFields, "/")
	imageSecret := deploy.Spec.Template.Spec.ImagePullSecrets
	if imageSecret != nil {
		return registry, imageSecret[0].Name, nil
	}
	return registry, "", nil
}

// GetKopiaExecutorImageRegistryAndSecret - will return the kopia image registry and image secret
func GetKopiaExecutorImageRegistryAndSecret(source, sourceNs string) (string, string, error) {
	var registry, registrySecret string
	var err error
	if len(os.Getenv(kopiaExecutorImageRegistryEnvVar)) == 0 {
		registry, registrySecret, err = GetImageRegistryFromDeployment(source, sourceNs)
		if err != nil {
			logrus.Errorf("GetKopiaExecutorImageRegistryAndSecret: error in getting image registory from %v:%v deployment", sourceNs, source)
			return "", "", err
		}
		return registry, registrySecret, nil
	}
	registry = os.Getenv(kopiaExecutorImageRegistryEnvVar)
	registrySecret = os.Getenv(kopiaExecutorImageRegistrySecretEnvVar)
	return registry, registrySecret, nil

}

// GetKopiaExecutorImageName - will return the default kopia executor image
func GetKopiaExecutorImageName() string {
	return strings.Join([]string{drivers.KopiaExecutorImage, version.Get().GitVersion}, ":")
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

// CreateImageRegistrySecret - will create the image registry secret in the given job pod namespace
func CreateImageRegistrySecret(sourceName, destName, sourceNamespace, destNamespace string) error {
	// Read the image secret from the stork deployment namespace
	// and create one in the current job's namespace
	secret, err := core.Instance().GetSecret(sourceName, sourceNamespace)
	if err != nil {
		logrus.Errorf("failed in getting secret [%v/%v]: %v", sourceNamespace, sourceName, err)
		return err
	}
	// Re-create it in the pvc namespace, where the job pod will run
	newSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        GetImageSecretName(destName),
			Namespace:   destNamespace,
			Labels:      secret.Labels,
			Annotations: secret.Annotations,
		},
		Data: secret.Data,
		Type: secret.Type,
	}
	_, err = core.Instance().CreateSecret(newSecret)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		logrus.Errorf("failed in creating secret [%v/%v]: %v", destNamespace, destName, err)
		return err
	}
	return nil
}

//GetCredSecretName - get credential secret name
func GetCredSecretName(name string) string {
	return CredSecret + "-" + name
}

//GetImageSecretName - get image secret name
func GetImageSecretName(name string) string {
	return ImageSecret + "-" + name
}

//GetCertSecretName - get cert secret name
func GetCertSecretName(name string) string {
	return CertSecret + "-" + name
}
