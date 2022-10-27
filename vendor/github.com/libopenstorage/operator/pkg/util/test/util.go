package test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/hashicorp/go-version"
	"github.com/libopenstorage/openstorage/api"

	ocp_configv1 "github.com/openshift/api/config/v1"
	appops "github.com/portworx/sched-ops/k8s/apps"
	coreops "github.com/portworx/sched-ops/k8s/core"
	k8serrors "github.com/portworx/sched-ops/k8s/errors"
	operatorops "github.com/portworx/sched-ops/k8s/operator"
	prometheusops "github.com/portworx/sched-ops/k8s/prometheus"
	rbacops "github.com/portworx/sched-ops/k8s/rbac"
	"github.com/portworx/sched-ops/task"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	fakeextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	pluginhelper "k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
	cluster_v1alpha1 "sigs.k8s.io/cluster-api/pkg/apis/deprecated/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/libopenstorage/operator/pkg/mock"
	"github.com/libopenstorage/operator/pkg/util"
	ocp_secv1 "github.com/openshift/api/security/v1"
)

const (
	// PxReleaseManifestURLEnvVarName is a release manifest URL Env variable name
	PxReleaseManifestURLEnvVarName = "PX_RELEASE_MANIFEST_URL"

	// AnnotationPXVersion annotation indicating the portworx semantic version
	AnnotationPXVersion = pxAnnotationPrefix + "/px-version"

	// PxRegistryUserEnvVarName is a Docker username Env variable name
	PxRegistryUserEnvVarName = "REGISTRY_USER"

	// PxRegistryPasswordEnvVarName is a Docker password Env variable name
	PxRegistryPasswordEnvVarName = "REGISTRY_PASS"

	// PxImageEnvVarName is the env variable to specify a specific Portworx image to install
	PxImageEnvVarName = "PX_IMAGE"

	// StorkNamespaceEnvVarName is the namespace where stork is deployed
	StorkNamespaceEnvVarName = "STORK-NAMESPACE"

	// StorkPxJwtIssuerEnvVarName is a PX JWT issuer Env variable name for stork
	StorkPxJwtIssuerEnvVarName = "PX_JWT_ISSUER"

	// DefaultStorkPxJwtIssuerEnvVarValue is a defeault value for PX JWT issuer for stork
	DefaultStorkPxJwtIssuerEnvVarValue = "apps.portworx.io"

	// StorkPxSharedSecretEnvVarName is a PX shared secret Env variable name for stork
	StorkPxSharedSecretEnvVarName = "PX_SHARED_SECRET"

	// DefaultStorkPxSharedSecretEnvVarValue is a default value for PX shared secret for stork
	DefaultStorkPxSharedSecretEnvVarValue = "px-system-secrets"

	// DefaultPxVsphereSecretName is a default name for PX vSphere credentials secret
	DefaultPxVsphereSecretName = "px-vsphere-secret"

	// PxMasterVersion is a tag for Portworx master version
	PxMasterVersion = "3.0.0.0"

	// PxOperatorMasterVersion is a tag for PX Operator master version
	PxOperatorMasterVersion = "9.9.9.9"

	// AksPVCControllerSecurePort is the PVC controller secure port.
	AksPVCControllerSecurePort = "10261"

	pxAnnotationPrefix = "portworx.io"

	defaultTelemetrySecretValidationTimeout  = 30 * time.Second
	defaultTelemetrySecretValidationInterval = time.Second

	defaultTelemetryInPxctlValidationTimeout  = 5 * time.Minute
	defaultTelemetryInPxctlValidationInterval = 30 * time.Second

	defaultPxAuthValidationTimeout  = 20 * time.Minute
	defaultPxAuthValidationInterval = 30 * time.Second

	defaultRunCmdInPxPodTimeout  = 25 * time.Second
	defaultRunCmdInPxPodInterval = 5 * time.Second
)

// TestSpecPath is the path for all test specs. Due to currently functional test and
// unit test use different path, this needs to be set accordingly.
var TestSpecPath = "testspec"

var (
	pxVer2_12, _  = version.NewVersion("2.12.0-")
	opVer1_10, _  = version.NewVersion("1.10.0-")
	opVer1_9_1, _ = version.NewVersion("1.9.1-")
)

// MockDriver creates a mock storage driver
func MockDriver(mockCtrl *gomock.Controller) *mock.MockDriver {
	return mock.NewMockDriver(mockCtrl)
}

// FakeK8sClient creates a fake controller-runtime Kubernetes client. Also
// adds the CRDs defined in this repository to the scheme
func FakeK8sClient(initObjects ...runtime.Object) client.Client {
	s := scheme.Scheme
	corev1.AddToScheme(s)
	monitoringv1.AddToScheme(s)
	cluster_v1alpha1.AddToScheme(s)
	ocp_configv1.AddToScheme(s)
	return fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(initObjects...).Build()
}

// List returns a list of objects using the given Kubernetes client
func List(k8sClient client.Client, obj client.ObjectList) error {
	return k8sClient.List(context.TODO(), obj, &client.ListOptions{})
}

// Get returns an object using the given Kubernetes client
func Get(k8sClient client.Client, obj client.Object, name, namespace string) error {
	return k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		},
		obj,
	)
}

// Delete deletes an object using the given Kubernetes client
func Delete(k8sClient client.Client, obj client.Object) error {
	return k8sClient.Delete(context.TODO(), obj)
}

// Update changes an object using the given Kubernetes client and updates the resource version
func Update(k8sClient client.Client, obj client.Object) error {
	return k8sClient.Update(
		context.TODO(),
		obj,
	)
}

// GetExpectedClusterRole returns the ClusterRole object from given yaml spec file
func GetExpectedClusterRole(t *testing.T, fileName string) *rbacv1.ClusterRole {
	obj := getKubernetesObject(t, fileName)
	clusterRole, ok := obj.(*rbacv1.ClusterRole)
	assert.True(t, ok, "Expected ClusterRole object")
	return clusterRole
}

// GetExpectedClusterRoleBinding returns the ClusterRoleBinding object from given
// yaml spec file
func GetExpectedClusterRoleBinding(t *testing.T, fileName string) *rbacv1.ClusterRoleBinding {
	obj := getKubernetesObject(t, fileName)
	crb, ok := obj.(*rbacv1.ClusterRoleBinding)
	assert.True(t, ok, "Expected ClusterRoleBinding object")
	return crb
}

// GetExpectedRole returns the Role object from given yaml spec file
func GetExpectedRole(t *testing.T, fileName string) *rbacv1.Role {
	obj := getKubernetesObject(t, fileName)
	role, ok := obj.(*rbacv1.Role)
	assert.True(t, ok, "Expected Role object")
	return role
}

// GetExpectedRoleBinding returns the RoleBinding object from given yaml spec file
func GetExpectedRoleBinding(t *testing.T, fileName string) *rbacv1.RoleBinding {
	obj := getKubernetesObject(t, fileName)
	roleBinding, ok := obj.(*rbacv1.RoleBinding)
	assert.True(t, ok, "Expected RoleBinding object")
	return roleBinding
}

// GetExpectedStorageClass returns the StorageClass object from given yaml spec file
func GetExpectedStorageClass(t *testing.T, fileName string) *storagev1.StorageClass {
	obj := getKubernetesObject(t, fileName)
	storageClass, ok := obj.(*storagev1.StorageClass)
	assert.True(t, ok, "Expected StorageClass object")
	return storageClass
}

// GetExpectedConfigMap returns the ConfigMap object from given yaml spec file
func GetExpectedConfigMap(t *testing.T, fileName string) *v1.ConfigMap {
	obj := getKubernetesObject(t, fileName)
	configMap, ok := obj.(*v1.ConfigMap)
	assert.True(t, ok, "Expected ConfigMap object")
	return configMap
}

// GetExpectedSecret returns the Secret object from given yaml spec file
func GetExpectedSecret(t *testing.T, fileName string) *v1.Secret {
	obj := getKubernetesObject(t, fileName)
	secret, ok := obj.(*v1.Secret)
	assert.True(t, ok, "Expected Secret object")
	return secret
}

// GetExpectedService returns the Service object from given yaml spec file
func GetExpectedService(t *testing.T, fileName string) *v1.Service {
	obj := getKubernetesObject(t, fileName)
	service, ok := obj.(*v1.Service)
	assert.True(t, ok, "Expected Service object")
	return service
}

// GetExpectedDeployment returns the Deployment object from given yaml spec file
func GetExpectedDeployment(t *testing.T, fileName string) *appsv1.Deployment {
	obj := getKubernetesObject(t, fileName)
	deployment, ok := obj.(*appsv1.Deployment)
	assert.True(t, ok, "Expected Deployment object")
	return deployment
}

// GetExpectedStatefulSet returns the StatefulSet object from given yaml spec file
func GetExpectedStatefulSet(t *testing.T, fileName string) *appsv1.StatefulSet {
	obj := getKubernetesObject(t, fileName)
	statefulSet, ok := obj.(*appsv1.StatefulSet)
	assert.True(t, ok, "Expected StatefulSet object")
	return statefulSet
}

// GetExpectedDaemonSet returns the DaemonSet object from given yaml spec file
func GetExpectedDaemonSet(t *testing.T, fileName string) *appsv1.DaemonSet {
	obj := getKubernetesObject(t, fileName)
	daemonSet, ok := obj.(*appsv1.DaemonSet)
	assert.True(t, ok, "Expected DaemonSet object")
	return daemonSet
}

// GetExpectedCRD returns the CustomResourceDefinition object from given yaml spec file
func GetExpectedCRD(t *testing.T, fileName string) *apiextensionsv1beta1.CustomResourceDefinition {
	obj := getKubernetesObject(t, fileName)
	crd, ok := obj.(*apiextensionsv1beta1.CustomResourceDefinition)
	assert.True(t, ok, "Expected CustomResourceDefinition object")
	return crd
}

// GetExpectedCRDV1 returns the CustomResourceDefinition object from given yaml spec file
func GetExpectedCRDV1(t *testing.T, fileName string) *apiextensionsv1.CustomResourceDefinition {
	obj := getKubernetesObject(t, fileName)
	crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
	assert.True(t, ok, "Expected CustomResourceDefinition object")
	return crd
}

// GetExpectedPrometheus returns the Prometheus object from given yaml spec file
func GetExpectedPrometheus(t *testing.T, fileName string) *monitoringv1.Prometheus {
	obj := getKubernetesObject(t, fileName)
	prometheus, ok := obj.(*monitoringv1.Prometheus)
	assert.True(t, ok, "Expected Prometheus object")
	return prometheus
}

// GetExpectedServiceMonitor returns the ServiceMonitor object from given yaml spec file
func GetExpectedServiceMonitor(t *testing.T, fileName string) *monitoringv1.ServiceMonitor {
	obj := getKubernetesObject(t, fileName)
	serviceMonitor, ok := obj.(*monitoringv1.ServiceMonitor)
	assert.True(t, ok, "Expected ServiceMonitor object")
	return serviceMonitor
}

// GetExpectedPrometheusRule returns the PrometheusRule object from given yaml spec file
func GetExpectedPrometheusRule(t *testing.T, fileName string) *monitoringv1.PrometheusRule {
	obj := getKubernetesObject(t, fileName)
	prometheusRule, ok := obj.(*monitoringv1.PrometheusRule)
	assert.True(t, ok, "Expected PrometheusRule object")
	return prometheusRule
}

// GetExpectedAlertManager returns the AlertManager object from given yaml spec file
func GetExpectedAlertManager(t *testing.T, fileName string) *monitoringv1.Alertmanager {
	obj := getKubernetesObject(t, fileName)
	alertManager, ok := obj.(*monitoringv1.Alertmanager)
	assert.True(t, ok, "Expected Alertmanager object")
	return alertManager

}

// GetExpectedPSP returns the PodSecurityPolicy object from given yaml spec file
func GetExpectedPSP(t *testing.T, fileName string) *policyv1beta1.PodSecurityPolicy {
	obj := getKubernetesObject(t, fileName)
	psp, ok := obj.(*policyv1beta1.PodSecurityPolicy)
	assert.True(t, ok, "Expected PodSecurityPolicy object")
	return psp
}

// GetExpectedSCC returns the SecurityContextConstraints object from given yaml spec file
func GetExpectedSCC(t *testing.T, fileName string) *ocp_secv1.SecurityContextConstraints {
	obj := getKubernetesObject(t, fileName)
	scc, ok := obj.(*ocp_secv1.SecurityContextConstraints)
	assert.True(t, ok, "Expected SecurityContextConstraints object")
	return scc
}

// getKubernetesObject returns a generic Kubernetes object from given yaml file
func getKubernetesObject(t *testing.T, fileName string) runtime.Object {
	json, err := ioutil.ReadFile(path.Join(TestSpecPath, fileName))
	assert.NoError(t, err)
	s := scheme.Scheme
	apiextensionsv1beta1.AddToScheme(s)
	apiextensionsv1.AddToScheme(s)
	monitoringv1.AddToScheme(s)
	ocp_secv1.Install(s)
	codecs := serializer.NewCodecFactory(s)
	obj, _, err := codecs.UniversalDeserializer().Decode([]byte(json), nil, nil)
	assert.NoError(t, err)
	return obj
}

// GetPullPolicyForContainer returns the image pull policy for given deployment
// and container name
func GetPullPolicyForContainer(
	deployment *appsv1.Deployment,
	containerName string,
) v1.PullPolicy {
	for _, c := range deployment.Spec.Template.Spec.Containers {
		if c.Name == containerName {
			return c.ImagePullPolicy
		}
	}
	return ""
}

// ActivateCRDWhenCreated activates the given CRD by updating it's status. It waits for
// CRD to be created for 1 minute before returning an error
func ActivateCRDWhenCreated(fakeClient *fakeextclient.Clientset, crdName string) error {
	return wait.Poll(1*time.Second, 1*time.Minute, func() (bool, error) {
		crd, err := fakeClient.ApiextensionsV1().
			CustomResourceDefinitions().
			Get(context.TODO(), crdName, metav1.GetOptions{})
		if err == nil {
			crd.Status.Conditions = []apiextensionsv1.CustomResourceDefinitionCondition{{
				Type:   apiextensionsv1.Established,
				Status: apiextensionsv1.ConditionTrue,
			}}
			fakeClient.ApiextensionsV1().
				CustomResourceDefinitions().
				UpdateStatus(context.TODO(), crd, metav1.UpdateOptions{})
			return true, nil
		} else if !errors.IsNotFound(err) {
			return false, err
		}
		return false, nil
	})
}

// ActivateV1beta1CRDWhenCreated activates the given CRD by updating it's status. It waits for
// CRD to be created for 1 minute before returning an error
func ActivateV1beta1CRDWhenCreated(fakeClient *fakeextclient.Clientset, crdName string) error {
	return wait.Poll(1*time.Second, 1*time.Minute, func() (bool, error) {
		crd, err := fakeClient.ApiextensionsV1beta1().
			CustomResourceDefinitions().
			Get(context.TODO(), crdName, metav1.GetOptions{})
		if err == nil {
			crd.Status.Conditions = []apiextensionsv1beta1.CustomResourceDefinitionCondition{{
				Type:   apiextensionsv1beta1.Established,
				Status: apiextensionsv1beta1.ConditionTrue,
			}}
			fakeClient.ApiextensionsV1beta1().
				CustomResourceDefinitions().
				UpdateStatus(context.TODO(), crd, metav1.UpdateOptions{})
			return true, nil
		} else if !errors.IsNotFound(err) {
			return false, err
		}
		return false, nil
	})
}

// UninstallStorageCluster uninstalls and wipe storagecluster from k8s
func UninstallStorageCluster(cluster *corev1.StorageCluster, kubeconfig ...string) error {
	var err error
	if len(kubeconfig) != 0 && kubeconfig[0] != "" {
		os.Setenv("KUBECONFIG", kubeconfig[0])
	}
	cluster, err = operatorops.Instance().GetStorageCluster(cluster.Name, cluster.Namespace)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if cluster.Spec.DeleteStrategy == nil ||
		(cluster.Spec.DeleteStrategy.Type != corev1.UninstallAndWipeStorageClusterStrategyType &&
			cluster.Spec.DeleteStrategy.Type != corev1.UninstallStorageClusterStrategyType) {
		cluster.Spec.DeleteStrategy = &corev1.StorageClusterDeleteStrategy{
			Type: corev1.UninstallAndWipeStorageClusterStrategyType,
		}
		if _, err := operatorops.Instance().UpdateStorageCluster(cluster); err != nil {
			return err
		}

		if err := validateTelemetrySecret(cluster, defaultTelemetrySecretValidationTimeout, defaultTelemetrySecretValidationInterval, false); err != nil {
			return err
		}
	}

	return operatorops.Instance().DeleteStorageCluster(cluster.Name, cluster.Namespace)
}

func validateTelemetrySecret(cluster *corev1.StorageCluster, timeout, interval time.Duration, force bool) error {
	t := func() (interface{}, bool, error) {
		secret, err := coreops.Instance().GetSecret("pure-telemetry-certs", cluster.Namespace)
		if err != nil {
			if errors.IsNotFound(err) && !force {
				// skip secret existence validation
				return nil, false, nil
			}
			return nil, true, fmt.Errorf("failed to get secret pure-telemetry-certs: %v", err)
		}
		logrus.Debugf("Found secret %s", secret.Name)

		// validate secret owner reference if telemetry enabled
		if cluster.Spec.Monitoring != nil && cluster.Spec.Monitoring.Telemetry != nil && cluster.Spec.Monitoring.Telemetry.Enabled {
			ownerRef := metav1.NewControllerRef(cluster, corev1.SchemeGroupVersion.WithKind("StorageCluster"))
			if cluster.Spec.DeleteStrategy != nil && cluster.Spec.DeleteStrategy.Type == corev1.UninstallAndWipeStorageClusterStrategyType {
				// validate secret should have owner reference
				for _, reference := range secret.OwnerReferences {
					if reference.UID == ownerRef.UID {
						logrus.Debugf("Found ownerReference for StorageCluster %s in secret %s", ownerRef.Name, secret.Name)
						return nil, false, nil
					}
				}
				return nil, true, fmt.Errorf("waiting for ownerReference to be set to StorageCluster %s in secret %s", cluster.Name, secret.Name)
			}
			// validate secret owner should not have owner reference
			for _, reference := range secret.OwnerReferences {
				if reference.UID == ownerRef.UID {
					return nil, true, fmt.Errorf("secret %s should not have ownerReference to StorageCluster %s", secret.Name, cluster.Name)
				}
			}
			return nil, false, nil
		}
		logrus.Debugf("telemetry is not enabled, skipping secret validation")
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// FindAndCopyVsphereSecretToCustomNamespace attempt to find and copy PX vSphere secret to a given namespace
func FindAndCopyVsphereSecretToCustomNamespace(customNamespace string) error {
	var pxVsphereSecret *v1.Secret

	// Get all secrets
	secrets, err := coreops.Instance().ListSecret("", metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list secrets, err %v", err)
	}

	// Find PX vSphere secret
	for _, secret := range secrets.Items {
		if secret.Name == DefaultPxVsphereSecretName {
			logrus.Debugf("Found %s in the %s namespace", secret.Name, secret.Namespace)
			pxVsphereSecret = &secret
			break
		}
	}

	if pxVsphereSecret == nil {
		logrus.Warnf("Failed to find secret %s", DefaultPxVsphereSecretName)
		return nil
	}

	// Construct new PX vSpheresecret in the new namespace
	newPxVsphereSecret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pxVsphereSecret.Name,
			Namespace: customNamespace,
		},
		Data: pxVsphereSecret.Data,
		Type: pxVsphereSecret.Type,
	}

	logrus.Debugf("Attempting to copy %s from %s to %s", DefaultPxVsphereSecretName, pxVsphereSecret.Namespace, customNamespace)
	_, err = coreops.Instance().CreateSecret(newPxVsphereSecret)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			logrus.Warnf("Secret %s already exists in %s namespace", DefaultPxVsphereSecretName, customNamespace)
			return nil
		}
		return err
	}

	return nil
}

// CreateVsphereCredentialEnvVarsFromSecret check if px-vsphere-secret exists and returns vSphere crendentials Env vars
func CreateVsphereCredentialEnvVarsFromSecret(namespace string) ([]v1.EnvVar, error) {
	var envVars []v1.EnvVar

	// Get PX vSphere secret
	_, err := coreops.Instance().GetSecret(DefaultPxVsphereSecretName, namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			logrus.Warnf("PX vSphere secret %s in not found in %s namespace, unable to get credentials from secret, please make sure you have specified them in the Env vars", DefaultPxVsphereSecretName, namespace)
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get secret %s in %s namespace, err %v", DefaultPxVsphereSecretName, namespace, err)
	}

	envVars = []v1.EnvVar{
		{
			Name: "VSPHERE_USER",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: DefaultPxVsphereSecretName,
					},
					Key: "VSPHERE_USER",
				},
			},
		},
		{
			Name: "VSPHERE_PASSWORD",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: DefaultPxVsphereSecretName,
					},
					Key: "VSPHERE_PASSWORD",
				},
			},
		},
	}

	return envVars, nil
}

// ValidateStorageCluster validates a StorageCluster spec
func ValidateStorageCluster(
	pxImageList map[string]string,
	clusterSpec *corev1.StorageCluster,
	timeout, interval time.Duration,
	shouldStartSuccessfully bool,
	kubeconfig ...string,
) error {
	// Set kubeconfig
	if len(kubeconfig) != 0 && kubeconfig[0] != "" {
		os.Setenv("KUBECONFIG", kubeconfig[0])
	}

	// Validate StorageCluster
	var liveCluster *corev1.StorageCluster
	var err error
	if shouldStartSuccessfully {
		liveCluster, err = ValidateStorageClusterIsOnline(clusterSpec, timeout, interval)
		if err != nil {
			return err
		}
	} else {
		// If we shouldn't start successfully, this is all we need to check
		return validateStorageClusterIsFailed(clusterSpec, timeout, interval)
	}

	// Validate that spec matches live spec
	if err = validateDeployedSpec(clusterSpec, liveCluster); err != nil {
		return err
	}

	// Validate StorageNodes
	if err = validateStorageNodes(pxImageList, clusterSpec, timeout, interval); err != nil {
		return err
	}

	// Get list of expected Portworx node names
	expectedPxNodeNameList, err := GetExpectedPxNodeNameList(clusterSpec)
	if err != nil {
		return err
	}

	// Validate Portworx pods
	podTestFn := func(pod v1.Pod) bool {
		return coreops.Instance().IsPodReady(pod)
	}
	if err = validateStorageClusterPods(clusterSpec, expectedPxNodeNameList, timeout, interval, podTestFn); err != nil {
		return err
	}

	// Validate Portworx nodes
	if err = validatePortworxNodes(liveCluster, len(expectedPxNodeNameList)); err != nil {
		return err
	}

	// Validate Portworx Service
	if err = validatePortworxService(liveCluster.Namespace); err != nil {
		return err
	}

	// Validate Portworx API Service
	if err = validatePortworxAPIService(liveCluster, timeout, interval); err != nil {
		return err
	}

	if err = validateComponents(pxImageList, liveCluster, timeout, interval); err != nil {
		return err
	}

	return nil
}

func validateStorageNodes(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	var expectedPxVersion string

	imageOverride := ""

	// Check if we have a PX_IMAGE set
	for _, env := range cluster.Spec.Env {
		if env.Name == PxImageEnvVarName {
			imageOverride = env.Value
			break
		}
	}

	expectedPxVersion = getPxVersion(pxImageList, cluster)

	t := func() (interface{}, bool, error) {
		// Get all StorageNodes
		storageNodeList, err := operatorops.Instance().ListStorageNodes(cluster.Namespace)
		if err != nil {
			return nil, true, err
		}

		// Check StorageNodes status and PX version
		expectedStatus := "Online"
		var readyNodes int
		for _, storageNode := range storageNodeList.Items {
			logString := fmt.Sprintf("storagenode: %s Expected status: %s Got: %s, ", storageNode.Name, expectedStatus, storageNode.Status.Phase)
			if imageOverride != "" {
				logString += fmt.Sprintf("Running PX version: %s From image: %s", storageNode.Spec.Version, imageOverride)
			} else {
				logString += fmt.Sprintf("Expected PX version: %s Got: %s", expectedPxVersion, storageNode.Spec.Version)
			}
			logrus.Debug(logString)

			// Don't mark this node as ready if it's not in the expected phase
			if storageNode.Status.Phase != expectedStatus {
				continue
			}
			// If we didn't specify a custom image, make sure it's running the expected version
			if imageOverride == "" && !strings.Contains(storageNode.Spec.Version, expectedPxVersion) {
				continue
			}

			readyNodes++
		}

		if readyNodes != len(storageNodeList.Items) {
			return nil, true, fmt.Errorf("waiting for all storagenodes to be ready: %d/%d", readyNodes, len(storageNodeList.Items))
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// nodeSpecsToMaps takes the given node spec list and converts it to a map of node names to
// cloud storage specs. Note that this will not work for label selectors at the moment, only
// node names.
func nodeSpecsToMaps(nodes []corev1.NodeSpec) map[string]*corev1.CloudStorageNodeSpec {
	toReturn := map[string]*corev1.CloudStorageNodeSpec{}

	for _, node := range nodes {
		toReturn[node.Selector.NodeName] = node.CloudStorage
	}

	return toReturn
}

func validateDeployedSpec(expected, live *corev1.StorageCluster) error {
	// Validate cloudStorage
	if !reflect.DeepEqual(expected.Spec.CloudStorage, live.Spec.CloudStorage) {
		return fmt.Errorf("deployed CloudStorage spec doesn't match expected")
	}
	// Validate kvdb
	if !reflect.DeepEqual(expected.Spec.Kvdb, live.Spec.Kvdb) {
		return fmt.Errorf("deployed Kvdb spec doesn't match expected")
	}
	// Validate nodes
	if !reflect.DeepEqual(nodeSpecsToMaps(expected.Spec.Nodes), nodeSpecsToMaps(live.Spec.Nodes)) {
		return fmt.Errorf("deployed Nodes spec doesn't match expected")
	}

	// TODO: validate more parts of the spec as we test with them

	return nil
}

// NewResourceVersion creates a random 16 character string
// to simulate a k8s resource version
func NewResourceVersion() string {
	var randBytes = make([]byte, 32)
	_, err := rand.Read(randBytes)
	if err != nil {
		return ""
	}

	ver := make([]byte, base64.StdEncoding.EncodedLen(len(randBytes)))
	base64.StdEncoding.Encode(ver, randBytes)

	return string(ver[:16])
}

func getSdkConnection(cluster *corev1.StorageCluster) (*grpc.ClientConn, error) {
	pxEndpoint, err := coreops.Instance().GetServiceEndpoint("portworx-service", cluster.Namespace)
	if err != nil {
		return nil, err
	}

	svc, err := coreops.Instance().GetService("portworx-service", cluster.Namespace)
	if err != nil {
		return nil, err
	}

	servicePort := int32(0)
	nodePort := ""
	for _, port := range svc.Spec.Ports {
		if port.Name == "px-sdk" {
			servicePort = port.Port
			nodePort = port.TargetPort.StrVal
			break
		}
	}

	if servicePort == 0 {
		return nil, fmt.Errorf("px-sdk port not found in service")
	}

	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", pxEndpoint, servicePort), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	// try over the service endpoint
	cli := api.NewOpenStorageIdentityClient(conn)
	if _, err = cli.Version(context.Background(), &api.SdkIdentityVersionRequest{}); err == nil {
		return conn, nil
	}

	// if  service endpoint IP is not accessible, we pick one node IP
	if nodes, err := coreops.Instance().GetNodes(); err == nil {
		for _, node := range nodes.Items {
			for _, addr := range node.Status.Addresses {
				if addr.Type == v1.NodeInternalIP {
					conn, err := grpc.Dial(fmt.Sprintf("%s:%s", addr.Address, nodePort), grpc.WithInsecure())
					if err != nil {
						return nil, err
					}
					if _, err = cli.Version(context.Background(), &api.SdkIdentityVersionRequest{}); err == nil {
						return conn, nil
					}
				}
			}
		}
	}
	return nil, err
}

// ValidateUninstallStorageCluster validates if storagecluster and its related objects
// were properly uninstalled and cleaned
func ValidateUninstallStorageCluster(
	cluster *corev1.StorageCluster,
	timeout, interval time.Duration,
	kubeconfig ...string,
) error {
	if len(kubeconfig) != 0 && kubeconfig[0] != "" {
		os.Setenv("KUBECONFIG", kubeconfig[0])
	}
	t := func() (interface{}, bool, error) {
		cluster, err := operatorops.Instance().GetStorageCluster(cluster.Name, cluster.Namespace)
		if err != nil {
			if errors.IsNotFound(err) {
				return "", false, nil
			}
			return "", true, err
		}

		pods, err := coreops.Instance().GetPodsByOwner(cluster.UID, cluster.Namespace)
		if err != nil && err != k8serrors.ErrPodsNotFound {
			return "", true, fmt.Errorf("failed to get pods for StorageCluster %s/%s. Err: %v",
				cluster.Namespace, cluster.Name, err)
		}

		var podsToBeDeleted []string
		for _, pod := range pods {
			podsToBeDeleted = append(podsToBeDeleted, pod.Name)
		}

		if len(pods) > 0 {
			return "", true, fmt.Errorf("%d pods are still present, waiting for Portworx pods to be deleted: %s", len(pods), podsToBeDeleted)
		}

		return "", true, fmt.Errorf("pods are deleted, but StorageCluster %v/%v still present",
			cluster.Namespace, cluster.Name)
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	// Validate deletion of Portworx ConfigMaps
	if err := validatePortworxConfigMapsDeleted(cluster, timeout, interval); err != nil {
		return err
	}

	// Verify telemetry secret is deleted on UninstallAndWipe when telemetry is enabled
	if cluster.Spec.DeleteStrategy != nil && cluster.Spec.DeleteStrategy.Type == corev1.UninstallAndWipeStorageClusterStrategyType {
		secret, err := coreops.Instance().GetSecret("pure-telemetry-certs", cluster.Namespace)
		if err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("failed to get secret pure-telemetry-certs: %v", err)
		} else if err == nil {
			// Secret found, only do the validation when telemetry is enabled, since if it's disabled, there's a chance secret owner is not set yet
			if cluster.Spec.Monitoring != nil && cluster.Spec.Monitoring.Telemetry != nil && cluster.Spec.Monitoring.Telemetry.Enabled {
				return fmt.Errorf("telemetry secret pure-telemetry-certs was found when shouldn't have been")
			}
			// Delete stale telemetry secret
			if err := coreops.Instance().DeleteSecret(secret.Name, cluster.Namespace); err != nil {
				return fmt.Errorf("failed to delete secret %s: %v", secret.Name, err)
			}
		}
	}

	// Delete AlertManager secret, if found
	err := coreops.Instance().DeleteSecret("alertmanager-portworx", cluster.Namespace)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete alertmanager-portworx secret, err: %v", err)
	}

	return nil
}

func validatePortworxConfigMapsDeleted(cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	configMapList := []string{"px-attach-driveset-lock", fmt.Sprintf("px-bootstrap-%s", cluster.Name), "px-bringup-queue-lockdefault", fmt.Sprintf("px-cloud-drive-%s", cluster.Name)}

	t := func() (interface{}, bool, error) {
		var presentConfigMaps []string
		for _, configMapName := range configMapList {
			_, err := coreops.Instance().GetConfigMap(configMapName, "kube-system")
			if err != nil {
				if errors.IsNotFound(err) {
					continue
				}
				return "", true, err
			}
			presentConfigMaps = append(presentConfigMaps, configMapName)
		}
		if len(presentConfigMaps) > 0 {
			return "", true, fmt.Errorf("not all expected configMaps have been deleted, waiting for %s to be deleted", presentConfigMaps)
		}
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	logrus.Debug("Portworx ConfigMaps have been deleted successfully")
	return nil
}

type podTestFnType func(pod v1.Pod) bool

func validateStorageClusterPods(
	clusterSpec *corev1.StorageCluster,
	expectedPxNodeNameList []string,
	timeout, interval time.Duration,
	podTestFn podTestFnType,
) error {
	t := func() (interface{}, bool, error) {
		cluster, err := operatorops.Instance().GetStorageCluster(clusterSpec.Name, clusterSpec.Namespace)
		if err != nil {
			return "", true, err
		}

		pods, err := coreops.Instance().GetPodsByOwner(cluster.UID, cluster.Namespace)
		if err != nil || pods == nil {
			return "", true, fmt.Errorf("failed to get pods for StorageCluster %s/%s. Err: %v",
				cluster.Namespace, cluster.Name, err)
		}

		if len(pods) != len(expectedPxNodeNameList) {
			return "", true, fmt.Errorf("expected pods: %v. actual pods: %v", len(expectedPxNodeNameList), len(pods))
		}

		var pxNodeNameList []string
		var podsNotReady []string
		var podsReady []string
		for _, pod := range pods {
			// if test-function fails, POD is considered as "not ready"
			if !podTestFn(pod) {
				podsNotReady = append(podsNotReady, pod.Name)
			}
			pxNodeNameList = append(pxNodeNameList, pod.Spec.NodeName)
			podsReady = append(podsReady, pod.Name)
		}

		if len(podsNotReady) > 0 {
			return "", true, fmt.Errorf("waiting for Portworx pods to be ready: %s", podsNotReady)
		}

		if !assert.ElementsMatch(&testing.T{}, expectedPxNodeNameList, pxNodeNameList) {
			return "", false, fmt.Errorf("expected Portworx nodes: %+v, got %+v", expectedPxNodeNameList, pxNodeNameList)
		}

		logrus.Debugf("All Portworx pods are ready: %s", podsReady)
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// Set default Node Affinity rules as Portworx Operator would when deploying StorageCluster
func defaultPxNodeAffinityRules(runOnMaster bool) *v1.NodeAffinity {
	selectorRequirements := []v1.NodeSelectorRequirement{
		{
			Key:      "px/enabled",
			Operator: v1.NodeSelectorOpNotIn,
			Values:   []string{"false"},
		},
	}

	if !runOnMaster {
		selectorRequirements = append(
			selectorRequirements,
			v1.NodeSelectorRequirement{
				Key:      "node-role.kubernetes.io/master",
				Operator: v1.NodeSelectorOpDoesNotExist,
			},
			v1.NodeSelectorRequirement{
				Key:      "node-role.kubernetes.io/control-plane",
				Operator: v1.NodeSelectorOpDoesNotExist,
			},
		)
	}

	nodeAffinity := &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: selectorRequirements,
				},
			},
		},
	}

	return nodeAffinity
}

func validatePortworxNodes(cluster *corev1.StorageCluster, expectedNodes int) error {
	conn, err := getSdkConnection(cluster)
	if err != nil {
		// CHECKME -- shouldn't we return err ?
		return nil
	}

	nodeClient := api.NewOpenStorageNodeClient(conn)
	nodeEnumerateResp, err := nodeClient.Enumerate(context.Background(), &api.SdkNodeEnumerateRequest{})
	if err != nil {
		return err
	}

	actualNodes := len(nodeEnumerateResp.GetNodeIds())
	if actualNodes != expectedNodes {
		return fmt.Errorf("expected nodes: %v. actual nodes: %v", expectedNodes, actualNodes)
	}

	// TODO: Validate Portworx is started with correct params. Check individual options
	for _, n := range nodeEnumerateResp.GetNodeIds() {
		nodeResp, err := nodeClient.Inspect(context.Background(), &api.SdkNodeInspectRequest{NodeId: n})
		if err != nil {
			return err
		}
		if nodeResp.Node.Status != api.Status_STATUS_OK {
			return fmt.Errorf("node %s is not online. Current: %v", nodeResp.Node.SchedulerNodeName,
				nodeResp.Node.Status)
		}

	}
	return nil
}

func validatePortworxService(namespace string) error {
	pxServiceName := "portworx-service"
	_, err := coreops.Instance().GetService(pxServiceName, namespace)
	if err != nil {
		return fmt.Errorf("failed to validate Service %s/%s, Err: %v", namespace, pxServiceName, err)
	}
	return nil
}

func validatePortworxAPIService(cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	t := func() (interface{}, bool, error) {
		pxAPIServiceName := "portworx-api"
		service, err := coreops.Instance().GetService(pxAPIServiceName, cluster.Namespace)
		if err != nil {
			return nil, true, fmt.Errorf("failed to validate Service %s/%s, Err: %v", cluster.Namespace, pxAPIServiceName, err)
		}
		if cluster.Spec.Metadata != nil && cluster.Spec.Metadata.Labels != nil {
			for k, expectedVal := range cluster.Spec.Metadata.Labels["service/portworx-api"] {
				if actualVal, ok := service.Labels[k]; !ok || actualVal != expectedVal {
					return nil, true, fmt.Errorf("failed to validate Service %s/%s custom labels, label %s doesn't exist or value doesn't match", cluster.Namespace, pxAPIServiceName, k)
				}
			}
			for k := range service.Labels {
				if k == "name" {
					continue
				}
				if labels, ok := cluster.Spec.Metadata.Labels["service/portworx-api"]; ok {
					if _, ok := labels[k]; !ok {
						return nil, true, fmt.Errorf("failed to validate Service %s/%s custom labels, found unexpected label %s", cluster.Namespace, pxAPIServiceName, k)
					}
				} else {
					return nil, true, fmt.Errorf("failed to validate Service %s/%s custom labels, found unexpected label %s", cluster.Namespace, pxAPIServiceName, k)
				}
			}
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// GetExpectedPxNodeNameList will get the list of node names that should be included
// in the given Portworx cluster, by seeing if each non-master node matches the given
// node selectors and affinities.
func GetExpectedPxNodeNameList(cluster *corev1.StorageCluster) ([]string, error) {
	var nodeNameListWithPxPods []string
	nodeList, err := coreops.Instance().GetNodes()
	if err != nil {
		return nodeNameListWithPxPods, err
	}

	dummyPod := &v1.Pod{}
	if cluster.Spec.Placement != nil && cluster.Spec.Placement.NodeAffinity != nil {
		dummyPod.Spec.Affinity = &v1.Affinity{
			NodeAffinity: cluster.Spec.Placement.NodeAffinity.DeepCopy(),
		}
	} else {
		dummyPod.Spec.Affinity = &v1.Affinity{
			NodeAffinity: defaultPxNodeAffinityRules(IsK3sCluster()),
		}

		if IsK3sCluster() {
			masterTaintFound := false
			for _, t := range dummyPod.Spec.Tolerations {
				if t.Key == "node-role.kubernetes.io/master" &&
					t.Effect == v1.TaintEffectNoSchedule {
					masterTaintFound = true
					break
				}
			}

			if !masterTaintFound {
				dummyPod.Spec.Tolerations = append(dummyPod.Spec.Tolerations,
					v1.Toleration{
						Key:    "node-role.kubernetes.io/master",
						Effect: v1.TaintEffectNoSchedule,
					},
				)
			}
		}
	}

	for _, node := range nodeList.Items {
		if coreops.Instance().IsNodeMaster(node) && !IsK3sCluster() {
			continue
		}

		if pluginhelper.PodMatchesNodeSelectorAndAffinityTerms(dummyPod, &node) {
			nodeNameListWithPxPods = append(nodeNameListWithPxPods, node.Name)
		}
	}

	return nodeNameListWithPxPods, nil
}

// GetFullVersion returns the full kubernetes server version
func GetFullVersion() (*version.Version, string, error) {
	k8sVersion, err := coreops.Instance().GetVersion()
	if err != nil {
		return nil, "", fmt.Errorf("unable to get kubernetes version: %v", err)
	}

	kbVerRegex := regexp.MustCompile(`^(v\d+\.\d+\.\d+)(.*)`)
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

// IsK3sCluster returns true or false, based on this kubernetes cluster is k3s or not
func IsK3sCluster() bool {
	// Get k8s version ext
	_, ext, _ := GetFullVersion()

	if len(ext) > 0 {
		return strings.HasPrefix(ext[1:], "k3s") || strings.HasPrefix(ext[1:], "rke2")
	}
	return false
}

func validateComponents(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	k8sVersion, err := GetK8SVersion()
	if err != nil {
		return err
	}

	// Validate PVC Controller components and images
	if err := ValidatePvcController(pxImageList, cluster, k8sVersion, timeout, interval); err != nil {
		return err
	}

	// Validate Stork components and images
	if err := ValidateStork(pxImageList, cluster, k8sVersion, timeout, interval); err != nil {
		return err
	}

	// Validate Autopilot components and images
	if err := ValidateAutopilot(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	// Validate CSI components and images
	if err := ValidateCSI(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	// Validate Monitoring
	if err = ValidateMonitoring(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	// Validate PortworxProxy
	if err = ValidatePortworxProxy(cluster, timeout, interval); err != nil {
		return err
	}

	// Validate Security
	previouslyEnabled := false // NOTE: This is set to false by default as we are not expecting Security to be previously enabled here
	if err = ValidateSecurity(cluster, previouslyEnabled, timeout, interval); err != nil {
		return err
	}

	// Validate KVDB
	if err = ValidateKvdb(cluster, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidateKvdb validates Portworx KVDB components
func ValidateKvdb(cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Internal KVDB components")
	if cluster.Spec.Kvdb.Internal {
		logrus.Debug("Internal KVDB is enabled in StorageCluster")
		return ValidateInternalKvdbEnabled(cluster, timeout, interval)
	}
	logrus.Debug("Internal KVDB is disabled in StorageCluster")
	return ValidateInternalKvdbDisabled(cluster, timeout, interval)
}

// ValidateInternalKvdbEnabled validates that all Internal KVDB components are enabled/created
func ValidateInternalKvdbEnabled(cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Debug("Validate Internal KVDB components are enabled")

	t := func() (interface{}, bool, error) {
		// Validate KVDB pods
		listOptions := map[string]string{"kvdb": "true"}
		podList, err := coreops.Instance().GetPods(cluster.Namespace, listOptions)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get KVDB pods, Err: %v", err)
		}

		desiredKvdbPodCount := 3
		if len(podList.Items) != desiredKvdbPodCount {
			return nil, true, fmt.Errorf("failed to validate KVDB pod count, expected: %d, actual: %d", desiredKvdbPodCount, len(podList.Items))
		}
		logrus.Debugf("Found all %d/%d Internal KVDB pods", len(podList.Items), desiredKvdbPodCount)

		// Validate Portworx KVDB service
		portworxKvdbServiceName := "portworx-kvdb-service"
		_, err = coreops.Instance().GetService(portworxKvdbServiceName, cluster.Namespace)
		if err != nil {
			if errors.IsNotFound(err) {
				return nil, true, fmt.Errorf("failed to validate Portworx KVDB service %s, Err: %v", portworxKvdbServiceName, err)
			}
			return nil, true, fmt.Errorf("failed to get Portworx KVDB service %s, Err: %v", portworxKvdbServiceName, err)
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidateInternalKvdbDisabled validates that all Internal KVDB components are disabled/deleted
func ValidateInternalKvdbDisabled(cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Debug("Validate Internal KVDB components are disabled")

	t := func() (interface{}, bool, error) {
		// Validate KVDB pods
		listOptions := map[string]string{"kvdb": "true"}
		_, err := coreops.Instance().GetPods(cluster.Namespace, listOptions)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found KVDB pods, waiting for deletion")
		}

		// Validate Portworx KVDB service
		portworxKvdbServiceName := "portworx-kvdb-service"
		_, err = coreops.Instance().GetService(portworxKvdbServiceName, cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found %s service, waiting for deletion", portworxKvdbServiceName)
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidatePvcController validates PVC Controller components and images
func ValidatePvcController(pxImageList map[string]string, cluster *corev1.StorageCluster, k8sVersion string, timeout, interval time.Duration) error {
	logrus.Info("Validate PVC Controller components")

	pvcControllerDp := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "portworx-pvc-controller",
			Namespace: cluster.Namespace,
		},
	}

	// Check if PVC Controller is enabled or disabled
	if isPVCControllerEnabled(cluster) {
		return ValidatePvcControllerEnabled(pvcControllerDp, cluster, k8sVersion, timeout, interval)
	}
	return ValidatePvcControllerDisabled(pvcControllerDp, timeout, interval)
}

// ValidatePvcControllerEnabled validates that all PVC Controller components are enabled/created
func ValidatePvcControllerEnabled(pvcControllerDp *appsv1.Deployment, cluster *corev1.StorageCluster, k8sVersion string, timeout, interval time.Duration) error {
	logrus.Info("PVC Controller should be enabled")

	t := func() (interface{}, bool, error) {
		if err := appops.Instance().ValidateDeployment(pvcControllerDp, timeout, interval); err != nil {
			return nil, true, err
		}

		if err := validateImageTag(k8sVersion, pvcControllerDp.Namespace, map[string]string{"name": "portworx-pvc-controller"}); err != nil {
			return nil, true, err
		}

		// Validate PVC Controller custom port, if any were set
		if err := validatePvcControllerPorts(cluster, pvcControllerDp, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate PVC Controller ClusterRole
		_, err := rbacops.Instance().GetClusterRole(pvcControllerDp.Name)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ClusterRole %s, Err: %v", pvcControllerDp.Name, err)
		}

		// Validate PVC Controller ClusterRoleBinding
		_, err = rbacops.Instance().GetClusterRoleBinding(pvcControllerDp.Name)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ClusterRoleBinding %s, Err: %v", pvcControllerDp.Name, err)
		}

		// Validate PVC Controller ServiceAccount
		_, err = coreops.Instance().GetServiceAccount(pvcControllerDp.Name, pvcControllerDp.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ServiceAccount %s, Err: %v", pvcControllerDp.Name, err)
		}

		// Validate PVC controller deployment pod topology spread constraints
		if err := validatePodTopologySpreadConstraints(pvcControllerDp, timeout, interval); err != nil {
			return nil, true, err
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	logrus.Info("PVC Controller is enabled")
	return nil
}

// ValidatePvcControllerDisabled validates that all PVC Controller components are disabled/deleted
func ValidatePvcControllerDisabled(pvcControllerDp *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Info("PVC Controller should be disabled")

	t := func() (interface{}, bool, error) {
		// Validate portworx-pvc-controller deployment is terminated or doesn't exist
		if err := validateTerminatedDeployment(pvcControllerDp, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate VC Controller ClusterRole doesn't exist
		_, err := rbacops.Instance().GetClusterRole(pvcControllerDp.Name)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ClusterRole %s, is found when shouldn't be", pvcControllerDp.Name)
		}

		// Validate VC Controller ClusterRoleBinding doesn't exist
		_, err = rbacops.Instance().GetClusterRoleBinding(pvcControllerDp.Name)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ClusterRoleBinding %s, is found when shouldn't be", pvcControllerDp.Name)
		}

		// Validate VC Controller ServiceAccount doesn't exist
		_, err = coreops.Instance().GetServiceAccount(pvcControllerDp.Name, pvcControllerDp.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ServiceAccount %s, is found when shouldn't be", pvcControllerDp.Name)
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	logrus.Info("PVC Controller is disabled")
	return nil
}

// ValidateStork validates Stork components and images
func ValidateStork(pxImageList map[string]string, cluster *corev1.StorageCluster, k8sVersion string, timeout, interval time.Duration) error {
	logrus.Info("Validate Stork components")

	storkDp := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stork",
			Namespace: cluster.Namespace,
		},
	}

	storkSchedulerDp := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stork-scheduler",
			Namespace: cluster.Namespace,
		},
	}

	if cluster.Spec.Stork != nil && cluster.Spec.Stork.Enabled {
		logrus.Debug("Stork is enabled in StorageCluster")
		return ValidateStorkEnabled(pxImageList, cluster, storkDp, storkSchedulerDp, k8sVersion, timeout, interval)
	}
	logrus.Debug("Stork is disabled in StorageCluster")
	return ValidateStorkDisabled(cluster, storkDp, storkSchedulerDp, timeout, interval)
}

// ValidateStorkEnabled validates that all Stork components are enabled/created
func ValidateStorkEnabled(pxImageList map[string]string, cluster *corev1.StorageCluster, storkDp, storkSchedulerDp *appsv1.Deployment, k8sVersion string, timeout, interval time.Duration) error {
	logrus.Info("Validate Stork components are enabled")

	t := func() (interface{}, bool, error) {
		if err := validateDeployment(storkDp, timeout, interval); err != nil {
			return nil, true, err
		}

		var storkImage string
		if cluster.Spec.Stork.Image == "" {
			if value, ok := pxImageList["stork"]; ok {
				storkImage = value
			} else {
				return nil, true, fmt.Errorf("failed to find image for stork")
			}
		} else {
			storkImage = cluster.Spec.Stork.Image
		}

		pods, err := coreops.Instance().GetPods(cluster.Namespace, map[string]string{"name": "stork"})
		if err != nil {
			return nil, true, err
		}

		if err := validateContainerImageInsidePods(cluster, storkImage, "stork", pods); err != nil {
			return nil, true, err
		}

		// Validate stork namespace env var
		if err := validateStorkNamespaceEnvVar(cluster.Namespace, storkDp, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate stork-scheduler deployment and pods
		if err := validateDeployment(storkSchedulerDp, timeout, interval); err != nil {
			return nil, true, err
		}

		K8sVer1_22, _ := version.NewVersion("1.22")
		kubeVersion, _, err := GetFullVersion()
		if err != nil {
			return nil, true, err
		}

		if kubeVersion != nil && kubeVersion.GreaterThanOrEqual(K8sVer1_22) {
			// TODO Image tag for stork-scheduler is hardcoded to v1.21.4 for clusters 1.22 and up
			if err = validateImageTag("v1.21.4", cluster.Namespace, map[string]string{"name": "stork-scheduler"}); err != nil {
				return nil, true, err
			}
		} else {
			if err = validateImageTag(k8sVersion, cluster.Namespace, map[string]string{"name": "stork-scheduler"}); err != nil {
				return nil, true, err
			}
		}

		// Validate webhook-controller arguments
		if err := validateStorkWebhookController(cluster.Spec.Stork.Args, storkDp, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate hostNetwork parameter
		if err := validateStorkHostNetwork(cluster.Spec.Stork.HostNetwork, storkDp, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate stork deployment pod topology spread constraints
		if err := validatePodTopologySpreadConstraints(storkDp, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate stork scheduler deployment pod topology spread constraints
		if err := validatePodTopologySpreadConstraints(storkSchedulerDp, timeout, interval); err != nil {
			return nil, true, err
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidateStorkDisabled validates that all Stork components are disabled/deleted
func ValidateStorkDisabled(cluster *corev1.StorageCluster, storkDp, storkSchedulerDp *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Info("Validate Stork components are disabled")

	t := func() (interface{}, bool, error) {
		// Validate stork deployment is terminated or doesn't exist
		if err := validateTerminatedDeployment(storkDp, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate stork-scheduler deployment is terminated or doesn't exist
		if err := validateTerminatedDeployment(storkSchedulerDp, timeout, interval); err != nil {
			return nil, true, err
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil

}

// ValidateAutopilot validates Autopilot components and images
func ValidateAutopilot(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Autopilot components")

	autopilotDp := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "autopilot",
			Namespace: cluster.Namespace,
		},
	}

	if cluster.Spec.Autopilot != nil && cluster.Spec.Autopilot.Enabled {
		logrus.Debug("Autopilot is Enabled in StorageCluster")
		return ValidateAutopilotEnabled(pxImageList, cluster, autopilotDp, timeout, interval)
	}
	logrus.Debug("Autopilot is Disabled in StorageCluster")
	return ValidateAutopilotDisabled(cluster, autopilotDp, timeout, interval)
}

// ValidateAutopilotEnabled validates that all Autopilot components are enabled/created
func ValidateAutopilotEnabled(pxImageList map[string]string, cluster *corev1.StorageCluster, autopilotDp *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Info("Validate Autopilot components are enabled")

	t := func() (interface{}, bool, error) {
		// Validate autopilot deployment and pods
		if err := validateDeployment(autopilotDp, timeout, interval); err != nil {
			return nil, true, err
		}

		var autopilotImage string
		if cluster.Spec.Autopilot.Image == "" {
			if value, ok := pxImageList[autopilotDp.Name]; ok {
				autopilotImage = value
			} else {
				return nil, true, fmt.Errorf("failed to find image for %s", autopilotDp.Name)
			}
		} else {
			autopilotImage = cluster.Spec.Autopilot.Image
		}

		pods, err := coreops.Instance().GetPods(cluster.Namespace, map[string]string{"name": autopilotDp.Name})
		if err != nil {
			return nil, true, err
		}
		if err := validateContainerImageInsidePods(cluster, autopilotImage, autopilotDp.Name, pods); err != nil {
			return nil, true, err
		}

		// Validate Autopilot ClusterRole
		_, err = rbacops.Instance().GetClusterRole(autopilotDp.Name)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ClusterRole %s, Err: %v", autopilotDp.Name, err)
		}

		// Validate Autopilot ClusterRoleBinding
		_, err = rbacops.Instance().GetClusterRoleBinding(autopilotDp.Name)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ClusterRoleBinding %s, Err: %v", autopilotDp.Name, err)
		}

		// Validate Autopilot ConfigMap
		_, err = coreops.Instance().GetConfigMap("autopilot-config", autopilotDp.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate autopilot-config, Err: %v", err)
		}

		// Validate Autopilot ServiceAccount
		_, err = coreops.Instance().GetServiceAccount(autopilotDp.Name, autopilotDp.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ServiceAccount %s, Err: %v", autopilotDp.Name, err)
		}

		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidateAutopilotDisabled validates that all Autopilot components are disabled/deleted
func ValidateAutopilotDisabled(cluster *corev1.StorageCluster, autopilotDp *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Info("Validate Autopilot components are disabled")

	t := func() (interface{}, bool, error) {
		// Validate autopilot deployment is terminated or doesn't exist
		if err := validateTerminatedDeployment(autopilotDp, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate Autopilot ClusterRole doesn't exist
		_, err := rbacops.Instance().GetClusterRole(autopilotDp.Name)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ClusterRole %s, is found when shouldn't be", autopilotDp.Name)
		}

		// Validate Autopilot ClusterRoleBinding doesn't exist
		_, err = rbacops.Instance().GetClusterRoleBinding(autopilotDp.Name)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ClusterRoleBinding %s, is found when shouldn't be", autopilotDp.Name)
		}

		// Validate Autopilot ConfigMap doesn't exist
		_, err = coreops.Instance().GetConfigMap("autopilot-config", autopilotDp.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate autopilot-config, is found when shouldn't be")
		}

		// Validate Autopilot ServiceAccount doesn't exist
		_, err = coreops.Instance().GetServiceAccount(autopilotDp.Name, autopilotDp.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ServiceAccount %s, is found when shouldn't be", autopilotDp.Name)
		}

		return nil, true, nil
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidatePortworxProxy validates portworx proxy components
func ValidatePortworxProxy(cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Portworx Proxy components")

	proxyDs := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "portworx-proxy",
			Namespace: "kube-system",
		},
	}

	pxService := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "portworx-service",
			Namespace: "kube-system",
		},
	}

	if isPortworxProxyEnabled(cluster) {
		logrus.Debug("Portworx proxy is enabled in StorageCluster")
		return ValidatePortworxProxyEnabled(cluster, proxyDs, pxService, timeout, interval)
	}

	logrus.Debug("Portworx proxy is disabled in StorageCluster")
	return ValidatePortworxProxyDisabled(cluster, proxyDs, pxService, timeout, interval)
}

// ValidatePortworxProxyEnabled validates that all Portworx Proxy components are enabled/created
func ValidatePortworxProxyEnabled(cluster *corev1.StorageCluster, proxyDs *appsv1.DaemonSet, pxService *v1.Service, timeout, interval time.Duration) error {
	logrus.Info("Validate Portworx Proxy components are enabled")

	t := func() (interface{}, bool, error) {
		// Validate Portworx proxy DaemonSet
		if err := validateDaemonSet(proxyDs, timeout); err != nil {
			return nil, true, err
		}

		// Validate Portworx proxy ServiceAccount
		_, err := coreops.Instance().GetServiceAccount(proxyDs.Name, proxyDs.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ServiceAccount %s, Err: %v", proxyDs.Name, err)
		}

		// Validate Portworx proxy ClusterRoleBinding
		_, err = rbacops.Instance().GetClusterRoleBinding(proxyDs.Name)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ClusterRoleBinding %s, Err: %v", proxyDs.Name, err)
		}

		// Validate Portworx proxy Service in kube-system namespace
		if err := validatePortworxService("kube-system"); err != nil {
			return nil, true, err
		}
		_, err = coreops.Instance().GetService(pxService.Name, pxService.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate Service %s, Err: %v", pxService.Name, err)
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidatePortworxProxyDisabled validates that all Portworx Proxy components are disabled/deleted
func ValidatePortworxProxyDisabled(cluster *corev1.StorageCluster, proxyDs *appsv1.DaemonSet, pxService *v1.Service, timeout, interval time.Duration) error {
	logrus.Info("Validate Portworx Proxy components are disabled")

	t := func() (interface{}, bool, error) {
		// Validate Portworx proxy DaemonSet is terminated or doesn't exist
		if err := validateTerminatedDaemonSet(proxyDs, timeout); err != nil {
			return nil, true, err
		}

		// Validate Portworx proxy Service doesn't exist if cluster is not deployed in kube-system namespace
		if cluster.Namespace != "kube-system" {
			_, err := coreops.Instance().GetService(pxService.Name, pxService.Namespace)
			if !errors.IsNotFound(err) {
				return nil, true, fmt.Errorf("failed to validate Service %s, is found when shouldn't be", pxService.Name)
			}
		}

		// Validate Portworx proxy ClusterRoleBinding doesn't exist
		_, err := rbacops.Instance().GetClusterRoleBinding(proxyDs.Name)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ClusterRoleBinding %s, is found when shouldn't be", proxyDs.Name)
		}

		// Validate Portworx proxy ServiceAccount doesn't exist
		_, err = coreops.Instance().GetServiceAccount(proxyDs.Name, proxyDs.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ServiceAccount %s, is found when shouldn't be", proxyDs.Name)
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

func validateStorkWebhookController(webhookControllerArgs map[string]string, storkDeployment *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Debug("Validate Stork webhook-controller")

	t := func() (interface{}, bool, error) {
		pods, err := appops.Instance().GetDeploymentPods(storkDeployment)
		if err != nil {
			return nil, false, err
		}

		// Go through every Stork pod and look for --weebhook-controller command in every stork container and match it to the webhook-controller arg passed in spec
		for _, pod := range pods {
			webhookExist := false
			for _, container := range pod.Spec.Containers {
				if container.Name == "stork" {
					if len(container.Command) > 0 {
						for _, containerCommand := range container.Command {
							if strings.Contains(containerCommand, "--webhook-controller") {
								if len(webhookControllerArgs["webhook-controller"]) == 0 {
									return nil, true, fmt.Errorf("failed to validate webhook-controller, webhook-controller is missing from Stork args in the StorageCluster, but is found in the Stork pod [%s]", pod.Name)
								} else if webhookControllerArgs["webhook-controller"] != strings.Split(containerCommand, "=")[1] {
									return nil, true, fmt.Errorf("failed to validate webhook-controller, wrong --webhook-controller value in the command in Stork pod [%s]: expected: %s, got: %s", pod.Name, webhookControllerArgs["webhook-controller"], strings.Split(containerCommand, "=")[1])
								}
								logrus.Debugf("Value for webhook-controller inside Stork pod [%s] command args: expected %s, got %s", pod.Name, webhookControllerArgs["webhook-controller"], strings.Split(containerCommand, "=")[1])
								webhookExist = true
								continue
							}
						}
					}
					// Validate that if webhook-controller arg is missing from StorageCluster, it is also not found in pods
					if len(webhookControllerArgs["webhook-controller"]) != 0 && !webhookExist {
						return nil, true, fmt.Errorf("failed to validate webhook-controller, webhook-controller is found in Stork args in the StorageCluster, but missing from Stork pod [%s]", pod.Name)
					}
				}
			}
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

func validateStorkHostNetwork(hostNetwork *bool, storkDeployment *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Debug("Validate Stork hostNetwork")

	t := func() (interface{}, bool, error) {
		pods, err := appops.Instance().GetDeploymentPods(storkDeployment)
		if err != nil {
			return nil, false, err
		}

		// Setting hostNetworkValue to false if hostNetwork is nil, since its a *bool and we need to compare it to bool
		var hostNetworkValue bool
		if hostNetwork == nil {
			hostNetworkValue = false
		} else {
			hostNetworkValue = *hostNetwork
		}

		for _, pod := range pods {
			if pod.Spec.HostNetwork != hostNetworkValue {
				return nil, true, fmt.Errorf("failed to validate Stork hostNetwork inside Stork pod [%s]: expected: %v, actual: %v", pod.Name, hostNetworkValue, pod.Spec.HostNetwork)
			}
			logrus.Debugf("Value for hostNetwork inside Stork pod [%s]: expected: %v, actual: %v", pod.Name, hostNetworkValue, pod.Spec.HostNetwork)
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

func validateStorkNamespaceEnvVar(namespace string, storkDeployment *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Debug("Validate Stork STORK-NAMESPACE env")

	t := func() (interface{}, bool, error) {
		pods, err := appops.Instance().GetDeploymentPods(storkDeployment)
		if err != nil {
			return nil, false, err
		}

		for _, pod := range pods {
			namespaceEnvVar := ""
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == StorkNamespaceEnvVarName {
					if env.Value != namespace {
						return nil, true, fmt.Errorf("failed to validate Stork STORK-NAMESPACE env var inside Stork pod [%s]: expected: %s, actual: %s", pod.Name, namespace, env.Value)
					}
					namespaceEnvVar = env.Value
					break
				}
			}
			if namespaceEnvVar == "" {
				return nil, true, fmt.Errorf("failed to validate Stork STORK-NAMESPACE env var as it's not found")
			}
			logrus.Debugf("Value for STORK-NAMESPACE env var in Stork pod [%s]: expected: %v, actual: %v", pod.Name, namespace, namespaceEnvVar)
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidateCSI validates CSI components and images
func ValidateCSI(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate CSI components")

	pxCsiDp := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "px-csi-ext",
			Namespace: cluster.Namespace,
		},
	}

	if cluster.Spec.CSI.Enabled {
		logrus.Debug("CSI is enabled in StorageCluster")
		return ValidateCsiEnabled(pxImageList, cluster, pxCsiDp, timeout, interval)
	}
	logrus.Debug("CSI is disabled in StorageCluster")
	return ValidateCsiDisabled(cluster, pxCsiDp, timeout, interval)
}

// ValidateCsiEnabled validates that all CSI components are enabled/created
func ValidateCsiEnabled(pxImageList map[string]string, cluster *corev1.StorageCluster, pxCsiDp *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Info("Validate CSI components are enabled")

	t := func() (interface{}, bool, error) {
		logrus.Debug("CSI is enabled in StorageCluster")
		if err := validateCsiContainerInPxPods(cluster.Namespace, true, timeout, interval); err != nil {
			return nil, true, err
		}
		// Validate CSI container image inside Portworx OCI Monitor pods
		var csiNodeDriverRegistrarImage string
		if value, ok := pxImageList["csiNodeDriverRegistrar"]; ok {
			csiNodeDriverRegistrarImage = value
		} else {
			return nil, true, fmt.Errorf("failed to find image for csiNodeDriverRegistrar")
		}

		pods, err := coreops.Instance().GetPods(cluster.Namespace, map[string]string{"name": "portworx"})
		if err != nil {
			return nil, true, err
		}

		if err := validateContainerImageInsidePods(cluster, csiNodeDriverRegistrarImage, "csi-node-driver-registrar", pods); err != nil {
			return nil, true, err
		}

		// Validate px-csi-ext deployment and pods
		if err := validateDeployment(pxCsiDp, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate CSI container images inside px-csi-ext pods
		if err := validateCsiExtImages(cluster, pxImageList); err != nil {
			return nil, true, err
		}

		// Validate CSI deployment pod topology spread constraints
		if err := validatePodTopologySpreadConstraints(pxCsiDp, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate CSI topology specs
		if err := validateCSITopologySpecs(cluster.Namespace, cluster.Spec.CSI.Topology, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate CSI snapshot controller on non-ocp env, since ocp deploys its own snapshot controller
		if !isOpenshift(cluster) {
			if err := validateCSISnapshotController(cluster, pxImageList, timeout, interval); err != nil {
				return nil, true, err
			}
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidateCsiDisabled validates that all CSI components are disabled/deleted
func ValidateCsiDisabled(cluster *corev1.StorageCluster, pxCsiDp *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Info("Validate CSI components are disabled")

	t := func() (interface{}, bool, error) {
		logrus.Debug("CSI is disabled in StorageCluster")
		if err := validateCsiContainerInPxPods(cluster.Namespace, false, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate px-csi-ext deployment doesn't exist
		if err := validateTerminatedDeployment(pxCsiDp, timeout, interval); err != nil {
			return nil, true, err
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

func validateCsiContainerInPxPods(namespace string, csi bool, timeout, interval time.Duration) error {
	logrus.Debug("Validating CSI container inside Portworx OCI Monitor pods")
	listOptions := map[string]string{"name": "portworx"}

	t := func() (interface{}, bool, error) {
		var pxPodsWithCsiContainer []string

		// Get Portworx pods
		pods, err := coreops.Instance().GetPods(namespace, listOptions)
		if err != nil {
			return nil, false, err
		}

		podsReady := 0
		for _, pod := range pods.Items {
			for _, c := range pod.Status.InitContainerStatuses {
				if !c.Ready {
					continue
				}
			}
			containerReady := 0
			for _, c := range pod.Status.ContainerStatuses {
				if c.Ready {
					containerReady++
					continue
				}
			}

			if len(pod.Spec.Containers) == containerReady {
				podsReady++
			}

			for _, container := range pod.Spec.Containers {
				if container.Name == "csi-node-driver-registrar" {
					pxPodsWithCsiContainer = append(pxPodsWithCsiContainer, pod.Name)
					break
				}
			}
		}

		if csi {
			if len(pxPodsWithCsiContainer) != len(pods.Items) {
				return nil, true, fmt.Errorf("failed to validate CSI containers in PX pods: expected %d, got %d, %d/%d Ready pods", len(pods.Items), len(pxPodsWithCsiContainer), podsReady, len(pods.Items))
			}
		} else {
			if len(pxPodsWithCsiContainer) > 0 || len(pods.Items) != podsReady {
				return nil, true, fmt.Errorf("failed to validate CSI container in PX pods: expected: 0, got %d, %d/%d Ready pods", len(pxPodsWithCsiContainer), podsReady, len(pods.Items))
			}
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

func validatePvcControllerPorts(cluster *corev1.StorageCluster, pvcControllerDeployment *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Debug("Validate PVC Controller custom ports")
	annotations := cluster.Annotations
	if annotations == nil {
		return nil
	}

	pvcSecurePort := annotations["portworx.io/pvc-controller-secure-port"]

	t := func() (interface{}, bool, error) {
		pods, err := appops.Instance().GetDeploymentPods(pvcControllerDeployment)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get %s deployment pods, Err: %v", pvcControllerDeployment.Name, err)
		}

		numberOfPods := 0
		// Go through every PVC Controller pod and look for --port and --secure-port commands in portworx-pvc-controller-manager pods and match it to the pvc-controller-port and pvc-controller-secure-port passed in StorageCluster annotations
		for _, pod := range pods {
			securePortExist := false
			for _, container := range pod.Spec.Containers {
				if container.Name == "portworx-pvc-controller-manager" {
					if len(container.Command) > 0 {
						for _, containerCommand := range container.Command {
							if strings.Contains(containerCommand, "--secure-port") {
								if len(pvcSecurePort) == 0 {
									if isAKS(cluster) {
										if strings.Split(containerCommand, "=")[1] != AksPVCControllerSecurePort {
											return nil, true, fmt.Errorf("failed to validate secure-port, secure-port is missing in the PVC Controler pod %s", pod.Name)
										}
									} else {
										return nil, true, fmt.Errorf("failed to validate secure-port, secure-port is missing from annotations in the StorageCluster, but is found in the PVC Controler pod %s", pod.Name)
									}
								} else if pvcSecurePort != strings.Split(containerCommand, "=")[1] {
									return nil, true, fmt.Errorf("failed to validate secure-port, wrong --secure-port value in the command in PVC Controller pod [%s]: expected: %s, got: %s", pod.Name, pvcSecurePort, strings.Split(containerCommand, "=")[1])
								}
								logrus.Debugf("Value for secure-port inside PVC Controller pod [%s]: expected %s, got %s", pod.Name, pvcSecurePort, strings.Split(containerCommand, "=")[1])
								securePortExist = true
								continue
							}
						}
					}
					// Validate that if PVC Controller ports are missing from StorageCluster, it is also not found in pods
					if len(pvcSecurePort) != 0 && !securePortExist {
						return nil, true, fmt.Errorf("failed to validate secure-port, port is found in StorageCluster annotations, but is missing from PVC Controller pod [%s]", pod.Name)
					}
					numberOfPods++
				}
			}
		}

		// TODO: Hardcoding this to 3 instead of len(pods), because the previous ValidateDeloyment() step might have not validated the updated deloyment
		if numberOfPods != 3 {
			return nil, true, fmt.Errorf("waiting for all PVC Controller pods, expected: %d, got: %d", 3, numberOfPods)
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

func validateDaemonSet(daemonset *appsv1.DaemonSet, timeout time.Duration) error {
	logrus.Debugf("Validating DaemonSet %s/%s", daemonset.Namespace, daemonset.Name)
	return appops.Instance().ValidateDaemonSet(daemonset.Name, daemonset.Namespace, timeout)
}

func validateTerminatedDaemonSet(daemonset *appsv1.DaemonSet, timeout time.Duration) error {
	logrus.Debugf("Validating DaemonSet %s/%s is terminated or doesn't exist", daemonset.Namespace, daemonset.Name)
	return appops.Instance().ValidateDaemonSetIsTerminated(daemonset.Name, daemonset.Namespace, timeout)
}

func validateDeployment(deployment *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Debugf("Validating deployment %s", deployment.Name)
	return appops.Instance().ValidateDeployment(deployment, timeout, interval)
}

func validateTerminatedDeployment(deployment *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Debugf("Validating deployment %s is terminated or doesn't exist", deployment.Name)
	return appops.Instance().ValidateTerminatedDeployment(deployment, timeout, interval)
}

func validateCSITopologySpecs(namespace string, topologySpec *corev1.CSITopologySpec, timeout, interval time.Duration) error {
	logrus.Debug("Validating CSI topology specs inside px-csi-ext pods")
	topologyEnabled := false
	if topologySpec != nil {
		topologyEnabled = topologySpec.Enabled
	}

	t := func() (interface{}, bool, error) {
		deployment, err := appops.Instance().GetDeployment("px-csi-ext", namespace)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get deployment %s/px-csi-ext", namespace)
		}
		pods, err := appops.Instance().GetDeploymentPods(deployment)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get pods of deployment %s/px-csi-ext", namespace)
		}
		// Go through each pod and validate the csi specs
		for _, pod := range pods {
			if err := validateCSITopologyFeatureGate(pod, topologyEnabled); err != nil {
				return nil, true, fmt.Errorf("failed to validate csi topology feature gate")
			}
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}
	return nil
}

func validateCSITopologyFeatureGate(pod v1.Pod, topologyEnabled bool) error {
	for _, container := range pod.Spec.Containers {
		if container.Name == "csi-external-provisioner" {
			featureGateEnabled := false
			for _, arg := range container.Args {
				if strings.Contains(arg, "Topology=true") {
					featureGateEnabled = true
					if !topologyEnabled {
						return fmt.Errorf("csi topology is disabled but found the feature gate enabled in container args")
					}
				}
			}
			if topologyEnabled && !featureGateEnabled {
				return fmt.Errorf("csi topology is enabled but cannot find the enabled feature gate in container args")
			}
		}
	}
	return nil
}

func validateCSISnapshotController(cluster *corev1.StorageCluster, pxImageList map[string]string, timeout, interval time.Duration) error {
	deployment := &appsv1.Deployment{}
	deployment.Namespace = cluster.Namespace
	deployment.Name = "px-csi-ext"
	t := func() (interface{}, bool, error) {
		existingDeployment, err := appops.Instance().GetDeployment(deployment.Name, deployment.Namespace)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get deployment %s/%s", deployment.Namespace, deployment.Name)
		}
		pods, err := appops.Instance().GetDeploymentPods(existingDeployment)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get pods of deployment %s/%s", deployment.Namespace, deployment.Name)
		}
		if cluster.Spec.CSI.InstallSnapshotController != nil && *cluster.Spec.CSI.InstallSnapshotController {
			if image, ok := pxImageList["csiSnapshotController"]; ok {
				if err := validateContainerImageInsidePods(cluster, image, "csi-snapshot-controller", &v1.PodList{Items: pods}); err != nil {
					return nil, true, err
				}
			} else {
				return nil, false, fmt.Errorf("failed to find image for csiSnapshotController")
			}
		} else {
			for _, pod := range pods {
				for _, c := range pod.Spec.Containers {
					if c.Name == "csi-snapshot-controller" {
						return nil, true, fmt.Errorf("found unexpected csi-snapshot-controller container in pod %s/%s", pod.Namespace, pod.Name)
					}
				}
			}
		}

		return nil, false, nil
	}

	logrus.Info("validating csi snapshot controller")
	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}
	return nil
}

func getPxVersion(pxImageList map[string]string, cluster *corev1.StorageCluster) string {
	var pxVersion string

	// Construct PX Version string used to match to deployed expected PX version
	if strings.Contains(pxImageList["version"], "_") {
		if cluster.Spec.Env != nil {
			for _, env := range cluster.Spec.Env {
				if env.Name == PxReleaseManifestURLEnvVarName {
					// Looking for clear PX version before /version in the URL
					ver := regexp.MustCompile(`\S+\/(\d.\S+)\/version`).FindStringSubmatch(env.Value)
					if ver != nil {
						pxVersion = ver[1]
					} else {
						// If the above regex found nothing, assuming it was a master version URL
						pxVersion = PxMasterVersion
					}
					break
				}
			}
		}
	} else {
		pxVersion = strings.TrimSpace(regexp.MustCompile(`:(\S+)`).FindStringSubmatch(pxImageList["version"])[1])
	}

	if pxVersion == "" {
		logrus.Error("failed to get PX version")
		return ""
	}

	return pxVersion
}

func validateCsiExtImages(cluster *corev1.StorageCluster, pxImageList map[string]string) error {
	logrus.Debug("Validating CSI container images inside px-csi-ext pods")

	deployment, err := appops.Instance().GetDeployment("px-csi-ext", cluster.Namespace)
	if err != nil {
		return err
	}

	pods, err := appops.Instance().GetDeploymentPods(deployment)
	if err != nil {
		return err
	}

	// Validate images inside csi-external-provisioner containers
	if image, ok := pxImageList["csiProvisioner"]; ok {
		if err := validateContainerImageInsidePods(cluster, image, "csi-external-provisioner", &v1.PodList{Items: pods}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find image for csiProvisioner")
	}

	if image, ok := pxImageList["csiSnapshotter"]; ok {
		if err := validateContainerImageInsidePods(cluster, image, "csi-snapshotter", &v1.PodList{Items: pods}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find image for csiSnapshotter")
	}

	if image, ok := pxImageList["csiResizer"]; ok {
		if err := validateContainerImageInsidePods(cluster, image, "csi-resizer", &v1.PodList{Items: pods}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find image for csiResizer")
	}

	pxVer2_10, _ := version.NewVersion("2.10")
	pxVersion, _ := version.NewVersion(getPxVersion(pxImageList, cluster))
	if pxVersion.GreaterThanOrEqual(pxVer2_10) {
		if image, ok := pxImageList["csiHealthMonitorController"]; ok {
			if err := validateContainerImageInsidePods(cluster, image, "csi-external-health-monitor-controller", &v1.PodList{Items: pods}); err != nil {
				return err
			}
		} else {
			// CEE-452: csi-external-health-monitor-controller is removed from manifest, add back when resolved
			logrus.Warnf("failed to find image for csiHealthMonitorController")
		}
	}

	return nil
}

func validateContainerImageInsidePods(cluster *corev1.StorageCluster, expectedImage, containerName string, pods *v1.PodList) error {
	logrus.Infof("Validating image for %s container inside pod(s)", containerName)

	// Get PX Operator version
	opVersion, _ := GetPxOperatorVersion()
	if opVersion.GreaterThanOrEqual(opVer1_9_1) {
		expectedImage = util.GetImageURN(cluster, expectedImage)
	}

	for _, pod := range pods.Items {
		foundContainer := false
		foundImage := ""
		containerList := make([]v1.Container, 0)

		// Get list of Init Containers
		if pod.Spec.InitContainers != nil {
			containerList = append(containerList, pod.Spec.InitContainers...)
		}

		// Get list of Containers
		if pod.Spec.Containers != nil {
			containerList = append(containerList, pod.Spec.Containers...)
		}

		for _, container := range containerList {
			if container.Name == containerName {
				foundImage = container.Image
				if opVersion.GreaterThanOrEqual(opVer1_9_1) && foundImage == expectedImage {
					logrus.Infof("Image inside %s[%s] matches, expected: %s, actual: %s", pod.Name, containerName, expectedImage, foundImage)
					foundContainer = true
					break
				} else if strings.Contains(foundImage, expectedImage) {
					logrus.Infof("Image inside %s[%s] matches, expected: %s, actual: %s", pod.Name, containerName, expectedImage, foundImage)
					foundContainer = true
					break
				} else {
					return fmt.Errorf("failed to match container %s[%s] image, expected: %s, actual: %s",
						pod.Name, containerName, expectedImage, foundImage)
				}
			}
		}

		if !foundContainer {
			return fmt.Errorf("failed to match container %s[%s] image, expected: %s, actual: %s",
				pod.Name, containerName, expectedImage, foundImage)
		}
	}
	return nil
}

func validateImageTag(tag, namespace string, listOptions map[string]string) error {
	pods, err := coreops.Instance().GetPods(namespace, listOptions)
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		for _, container := range pod.Spec.Containers {
			imageSplit := strings.Split(container.Image, ":")
			imageTag := ""
			if len(imageSplit) == 2 {
				imageTag = imageSplit[1]
			}
			if imageTag != tag {
				return fmt.Errorf("failed to validate image tag on pod %s container %s, Expected: %s Got: %s",
					pod.Name, container.Name, tag, imageTag)
			}
		}
	}
	return nil
}

// ValidateSecurity validates all PX Security components
func ValidateSecurity(cluster *corev1.StorageCluster, previouslyEnabled bool, timeout, interval time.Duration) error {
	logrus.Info("Validate PX Security components")

	storkDp := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stork",
			Namespace: cluster.Namespace,
		},
	}

	if cluster.Spec.Security != nil &&
		cluster.Spec.Security.Enabled {
		logrus.Infof("PX Security is enabled in StorageCluster")
		return ValidateSecurityEnabled(cluster, storkDp, timeout, interval)
	}

	logrus.Infof("PX Security is disabled in StorageCluster")
	return ValidateSecurityDisabled(cluster, storkDp, previouslyEnabled, timeout, interval)
}

// ValidateSecurityEnabled validates PX Security components are enabled/running as expected
func ValidateSecurityEnabled(cluster *corev1.StorageCluster, storkDp *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Info("Validate PX Security components are enabled")

	t := func() (interface{}, bool, error) {
		// Validate Stork ENV vars, if Stork is enabled
		if cluster.Spec.Stork != nil && cluster.Spec.Stork.Enabled {
			// Validate stork deployment and pods
			if err := validateDeployment(storkDp, timeout, interval); err != nil {
				return nil, true, fmt.Errorf("failed to validate Stork deployment and pods, err %v", err)
			}

			// Validate Security ENv vars in Stork pods
			if err := validateStorkSecurityEnvVar(cluster, storkDp, timeout, interval); err != nil {
				return nil, true, fmt.Errorf("failed to validate Stork Security ENV vars, err %v", err)
			}
		}

		if _, err := coreops.Instance().GetSecret("px-admin-token", cluster.Namespace); err != nil {
			return nil, true, fmt.Errorf("failed to find secret px-admin-token, err %v", err)
		}

		if _, err := coreops.Instance().GetSecret("px-user-token", cluster.Namespace); err != nil {
			return nil, true, fmt.Errorf("failed to find secret px-user-token, err %v", err)
		}

		if _, err := coreops.Instance().GetSecret("px-shared-secret", cluster.Namespace); err != nil {
			return nil, true, fmt.Errorf("failed to find secret px-shared-secret, err %v", err)
		}

		if _, err := coreops.Instance().GetSecret("px-system-secrets", cluster.Namespace); err != nil {
			return nil, true, fmt.Errorf("failed to find secret px-system-secrets, err %v", err)
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	if err := validatePxAuthOnPxNodes(true, cluster); err != nil {
		return fmt.Errorf("failed to validate PX Auth is enabled on PX nodes, Err: %v", err)
	}

	return nil
}

// ValidateSecurityDisabled validates PX Security components are disabled/uninstalled as expected
func ValidateSecurityDisabled(cluster *corev1.StorageCluster, storkDp *appsv1.Deployment, previouslyEnabled bool, timeout, interval time.Duration) error {
	logrus.Info("Validate PX Security components are not disabled")

	t := func() (interface{}, bool, error) {
		// Validate Stork ENV vars, if Stork is enabled
		if cluster.Spec.Stork != nil && cluster.Spec.Stork.Enabled {
			// Validate Stork deployment and pods
			if err := validateDeployment(storkDp, timeout, interval); err != nil {
				return nil, true, fmt.Errorf("failed to validate Stork deployment and pods, err %v", err)
			}

			// Validate Security ENv vars in Stork pods
			if err := validateStorkSecurityEnvVar(cluster, storkDp, timeout, interval); err != nil {
				return nil, true, fmt.Errorf("failed to validate Stork Security ENV vars, err %v", err)
			}
		}

		// *-token secrets are always deleted regardless if security was previously enabled or not
		_, err := coreops.Instance().GetSecret("px-admin-token", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found secret px-admin-token, when should't have, err %v", err)
		}

		_, err = coreops.Instance().GetSecret("px-user-token", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found secret px-user-token, when shouldn't have, err %v", err)
		}

		if previouslyEnabled {
			if _, err := coreops.Instance().GetSecret("px-shared-secret", cluster.Namespace); err != nil {
				return nil, true, fmt.Errorf("failed to find secret px-shared-secret, err %v", err)
			}

			if _, err := coreops.Instance().GetSecret("px-system-secrets", cluster.Namespace); err != nil {
				return nil, true, fmt.Errorf("failed to find secret px-system-secrets, err %v", err)
			}
		} else {
			_, err := coreops.Instance().GetSecret("px-shared-secret", cluster.Namespace)
			if !errors.IsNotFound(err) {
				return nil, true, fmt.Errorf("found secret px-shared-secret, when shouldn't have, err %v", err)
			}

			_, err = coreops.Instance().GetSecret("px-system-secrets", cluster.Namespace)
			if !errors.IsNotFound(err) {
				return nil, true, fmt.Errorf("found secret px-system-secrets, when shouldn't have, err %v", err)
			}
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	if err := validatePxAuthOnPxNodes(false, cluster); err != nil {
		return fmt.Errorf("failed to validate PX Auth is disabled on PX nodes, Err: %v", err)
	}

	return nil
}

func validateStorkSecurityEnvVar(cluster *corev1.StorageCluster, storkDeployment *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Debug("Validate Stork Security ENV vars")
	var securityEnabled bool

	if cluster.Spec.Security != nil && cluster.Spec.Security.Enabled {
		securityEnabled = cluster.Spec.Security.Enabled
	}

	t := func() (interface{}, bool, error) {
		pods, err := appops.Instance().GetDeploymentPods(storkDeployment)
		if err != nil {
			return nil, false, err
		}

		numberOfPods := 0
		for _, pod := range pods {
			pxJwtIssuerEnvVar := ""
			pxSharedSecretEnvVar := ""
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == StorkPxJwtIssuerEnvVarName && securityEnabled {
					if env.Value != DefaultStorkPxJwtIssuerEnvVarValue {
						return nil, true, fmt.Errorf("failed to validate Stork %s env var inside Stork pod [%s]: expected: %s, actual: %s", StorkPxJwtIssuerEnvVarName, pod.Name, DefaultStorkPxJwtIssuerEnvVarValue, env.Value)
					}
					pxJwtIssuerEnvVar = env.Value
				} else if env.Name == StorkPxJwtIssuerEnvVarName && !securityEnabled {
					return nil, true, fmt.Errorf("found env var %s inside Stork pod [%s] when Security is disabled", StorkPxJwtIssuerEnvVarName, pod.Name)
				}

				if env.Name == StorkPxSharedSecretEnvVarName && securityEnabled {
					if env.ValueFrom != nil {
						if env.ValueFrom.SecretKeyRef != nil {
							if env.ValueFrom.SecretKeyRef.Key == "apps-secret" {
								keyValue := env.ValueFrom.SecretKeyRef.LocalObjectReference
								if keyValue.Name != DefaultStorkPxSharedSecretEnvVarValue {
									return nil, true, fmt.Errorf("failed to validate Stork %s env var inside Stork pod [%s]: expected: %s, actual: %s", StorkPxSharedSecretEnvVarName, pod.Name, DefaultStorkPxSharedSecretEnvVarValue, keyValue.Name)
								}
								pxSharedSecretEnvVar = keyValue.Name
							}
						}
					}
				} else if env.Name == StorkPxSharedSecretEnvVarName && !securityEnabled {
					return nil, true, fmt.Errorf("found env var %s inside Stork pod [%s] when Security is disabled", StorkPxSharedSecretEnvVarName, pod.Name)
				}

			}
			if pxJwtIssuerEnvVar == "" && securityEnabled {
				return nil, true, fmt.Errorf("failed to validate Stork %s env var inside Stork pod [%s], because it was not found", StorkPxJwtIssuerEnvVarName, pod.Name)
			} else if pxJwtIssuerEnvVar != "" && !securityEnabled {
				return nil, true, fmt.Errorf("failed to validate Stork %s env var inside Stork pod [%s], because it was was found, when shouldn't have, if security is disabled", StorkPxJwtIssuerEnvVarName, pod.Name)
			}

			if pxSharedSecretEnvVar == "" && securityEnabled {
				return nil, true, fmt.Errorf("failed to validate Stork %s env var inside Stork pod [%s], because it was not found", StorkPxSharedSecretEnvVarName, pod.Name)
			} else if pxSharedSecretEnvVar != "" && !securityEnabled {
				return nil, true, fmt.Errorf("failed to validate Stork %s env var inside Stork pod [%s], because it was was found, when shouldn't have, if security is disabledd", StorkPxSharedSecretEnvVarName, pod.Name)
			}

			if securityEnabled {
				logrus.Debugf("Value for %s env var in Stork pod [%s]: expected: %v, actual: %v", StorkPxJwtIssuerEnvVarName, pod.Name, DefaultStorkPxJwtIssuerEnvVarValue, pxJwtIssuerEnvVar)
				logrus.Debugf("Value for %s env var in Stork pod [%s]: expected: %v, actual: %v", StorkPxSharedSecretEnvVarName, pod.Name, DefaultStorkPxSharedSecretEnvVarValue, pxSharedSecretEnvVar)
			}
			numberOfPods++
		}

		// TODO: Hardcoding this to 3 instead of len(pods), because the previous ValidateDeloyment() step might have not validated the updated deployment
		if numberOfPods != 3 {
			return nil, true, fmt.Errorf("waiting for all Stork pods, expected: %d, got: %d", 3, numberOfPods)
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidateMonitoring validates all PX Monitoring components
func ValidateMonitoring(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	if err := ValidatePrometheus(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	// Increasing timeout for Telemetry components as they take quite long time to initialize
	defaultTelemetryRetryInterval := 30 * time.Second
	defaultTelemetryTimeout := 10 * time.Minute
	if err := ValidateTelemetry(pxImageList, cluster, defaultTelemetryTimeout, defaultTelemetryRetryInterval); err != nil {
		return err
	}

	if err := ValidateAlertManager(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidatePrometheus validates all Prometheus components
func ValidatePrometheus(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	if cluster.Spec.Monitoring != nil &&
		((cluster.Spec.Monitoring.EnableMetrics != nil && *cluster.Spec.Monitoring.EnableMetrics) ||
			(cluster.Spec.Monitoring.Prometheus != nil && cluster.Spec.Monitoring.Prometheus.ExportMetrics)) {
		if cluster.Spec.Monitoring.Prometheus != nil && cluster.Spec.Monitoring.Prometheus.Enabled {
			logrus.Info("Prometheus is enabled in the StorageCluster")
			dep := appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "px-prometheus-operator",
					Namespace: cluster.Namespace,
				},
			}
			if err := appops.Instance().ValidateDeployment(&dep, timeout, interval); err != nil {
				return err
			}

			st := appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "prometheus-px-prometheus",
					Namespace: cluster.Namespace,
				},
			}
			if err := appops.Instance().ValidateStatefulSet(&st, timeout); err != nil {
				return err
			}
			t := func() (interface{}, bool, error) {
				_, err := prometheusops.Instance().GetPrometheusRule("portworx", cluster.Namespace)
				if err != nil {
					return nil, true, err
				}
				return nil, false, nil
			}
			if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
				return err
			}

			t = func() (interface{}, bool, error) {
				_, err := prometheusops.Instance().GetServiceMonitor("portworx", cluster.Namespace)
				if err != nil {
					return nil, true, err
				}
				return nil, false, nil
			}
			if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
				return err
			}
		}
	}

	return nil
}

// ValidateTelemetryV1Disabled validates telemetry components are uninstalled as expected
func ValidateTelemetryV1Disabled(cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	t := func() (interface{}, bool, error) {
		_, err := appops.Instance().GetDeployment("px-metrics-collector", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("wait for deletion, err %v", err)
		}

		_, err = rbacops.Instance().GetRole("px-metrics-collector", cluster.Name)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("wait for deletion, err %v", err)
		}

		_, err = rbacops.Instance().GetRoleBinding("px-metrics-collector", cluster.Name)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("wait for deletion, err %v", err)
		}

		_, err = coreops.Instance().GetConfigMap("px-telemetry-config", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("wait for deletion, err %v", err)
		}

		_, err = coreops.Instance().GetConfigMap("px-collector-config", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("wait for deletion, err %v", err)
		}

		_, err = coreops.Instance().GetConfigMap("px-collector-proxy-config", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("wait for deletion, err %v", err)
		}

		_, err = coreops.Instance().GetServiceAccount("px-metrics-collector", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("wait for deletion, err %v", err)
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	// Validate Telemetry is Disabled in pxctl status
	if err := validateTelemetryStatusInPxctl(false, cluster); err != nil {
		return fmt.Errorf("failed to validate that Telemetry is Disabled in pxctl status, Err: %v", err)
	}

	logrus.Infof("Telemetry is disabled")
	return nil
}

// ValidateTelemetry validates telemetry component is installed/uninstalled as expected
func ValidateTelemetry(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Telemetry components")
	logrus.Info("Check PX and PX Operator versions to determine which telemetry to validate against..")
	pxVersion := GetPortworxVersion(cluster)
	fmt.Printf("PX Version: %v", pxVersion)
	opVersion, err := GetPxOperatorVersion()
	if err != nil {
		return err
	}
	logrus.Infof("PX Operator version: %v", opVersion)

	if cluster.Spec.Monitoring != nil &&
		cluster.Spec.Monitoring.Telemetry != nil &&
		cluster.Spec.Monitoring.Telemetry.Enabled {
		logrus.Info("Telemetry is enabled in StorageCluster")
		if pxVersion.GreaterThanOrEqual(pxVer2_12) && opVersion.GreaterThanOrEqual(opVer1_10) {
			if err := ValidateTelemetryV2Enabled(pxImageList, cluster, timeout, interval); err != nil {
				return err
			}
		} else if err := ValidateTelemetryV1Enabled(pxImageList, cluster, timeout, interval); err != nil {
			return err
		}
		if err := validateTelemetrySecret(cluster, defaultTelemetrySecretValidationTimeout, defaultTelemetrySecretValidationInterval, true); err != nil {
			return err
		}
		return nil
	}

	logrus.Info("Telemetry is disabled in StorageCluster")
	if pxVersion.GreaterThanOrEqual(pxVer2_12) && opVersion.GreaterThanOrEqual(opVer1_10) {
		return ValidateTelemetryV2Disabled(cluster, timeout, interval)
	}
	return ValidateTelemetryV1Disabled(cluster, timeout, interval)
}

func runCmdInsidePxPod(pxPod *v1.Pod, cmd string, namespace string, ignoreErr bool) (string, error) {
	t := func() (interface{}, bool, error) {
		// Execute command in PX pod
		cmds := []string{"nsenter", "--mount=/host_proc/1/ns/mnt", "/bin/bash", "-c", cmd}
		logrus.Debugf("[%s] Running command inside pod %s", pxPod.Name, cmds)
		output, err := coreops.Instance().RunCommandInPod(cmds, pxPod.Name, "portworx", pxPod.Namespace)
		if !ignoreErr && err != nil {
			return "", true, fmt.Errorf("[%s] failed to run command inside pod, command: %v, err: %v", pxPod.Name, cmds, err)
		}
		return output, false, err
	}

	output, err := task.DoRetryWithTimeout(t, defaultRunCmdInPxPodTimeout, defaultRunCmdInPxPodInterval)
	if err != nil {
		return "", err
	}

	return output.(string), nil
}

// GetPortworxVersion returns the Portworx version based on the image provided.
// We first look at spec.Image, if not valid image tag found, we check the PX_IMAGE
// env variable. If that is not present or invalid semvar, then we fallback to an
// annotation portworx.io/px-version; then we try to extract the version from PX_RELEASE_MANIFEST_URL
// env variable, else we return master version
func GetPortworxVersion(cluster *corev1.StorageCluster) *version.Version {
	var (
		err       error
		pxVersion *version.Version
	)

	pxImage := cluster.Spec.Image
	var manifestURL string
	for _, env := range cluster.Spec.Env {
		if env.Name == PxImageEnvVarName {
			pxImage = env.Value
		} else if env.Name == PxReleaseManifestURLEnvVarName {
			manifestURL = env.Value
		}
	}

	pxVersionStr := strings.Split(pxImage, ":")[len(strings.Split(pxImage, ":"))-1]
	pxVersion, err = version.NewSemver(pxVersionStr)
	if err != nil {
		logrus.WithError(err).Warnf("Invalid PX version %s extracted from image name", pxVersionStr)
		if pxVersionStr, exists := cluster.Annotations[AnnotationPXVersion]; exists {
			logrus.Infof("Checking version in annotations %s", AnnotationPXVersion)
			pxVersion, err = version.NewSemver(pxVersionStr)
			if err != nil {
				logrus.WithError(err).Warnf("Invalid PX version %s extracted from annotation", pxVersionStr)
			}
		} else {
			logrus.Infof("Checking version in %s", PxReleaseManifestURLEnvVarName)
			pxVersionStr = getPortworxVersionFromManifestURL(manifestURL)
			pxVersion, err = version.NewSemver(pxVersionStr)
			if err != nil {
				logrus.WithError(err).Warnf("Invalid PX version %s extracted from %s", pxVersionStr, PxReleaseManifestURLEnvVarName)
			}
		}
	}

	if pxVersion == nil {
		logrus.Warnf("Failed to determine PX version, assuming its latest and setting it to master: %s", PxMasterVersion)
		pxVersion, _ = version.NewVersion(PxMasterVersion)
	}
	return pxVersion
}

func getPortworxVersionFromManifestURL(url string) string {
	regex := regexp.MustCompile(`.*portworx\.com\/(.*)\/version`)
	version := regex.FindStringSubmatch(url)
	if len(version) >= 2 {
		return version[1]
	}
	return ""
}

// GetPxOperatorVersion returns PX Operator version
func GetPxOperatorVersion() (*version.Version, error) {
	image, err := getPxOperatorImage()
	if err != nil {
		return nil, err
	}

	// We may run the automation on operator installed using private images,
	// so assume we are testing the latest operator version if failed to parse the tag
	versionTag := strings.Split(image, ":")[len(strings.Split(image, ":"))-1]
	opVersion, err := version.NewVersion(versionTag)
	if err != nil {
		masterVersionTag := PxOperatorMasterVersion
		logrus.WithError(err).Warnf("Failed to parse portworx-operator tag to version, assuming its latest and setting it to %s", PxOperatorMasterVersion)
		opVersion, _ = version.NewVersion(masterVersionTag)
	}

	logrus.Infof("Testing portworx-operator version: %s", opVersion.String())
	return opVersion, nil
}

func getPxOperatorImage() (string, error) {
	labelSelector := map[string]string{}
	var image string

	NamespaceList, err := coreops.Instance().ListNamespaces(labelSelector)
	if err != nil {
		return "", err
	}

	for _, ns := range NamespaceList.Items {
		operatorDeployment, err := appops.Instance().GetDeployment("portworx-operator", ns.Name)
		if err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return "", err
		}

		logrus.Infof("Found deployment name: %s in namespace: %s", operatorDeployment.Name, operatorDeployment.Namespace)

		for _, container := range operatorDeployment.Spec.Template.Spec.Containers {
			if container.Name == "portworx-operator" {
				image = container.Image
				logrus.Infof("Get portworx-operator image installed: %s", image)
				break
			}
		}
	}
	return image, nil
}

// ValidateAlertManager validates alertManager components
func ValidateAlertManager(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	if cluster.Spec.Monitoring != nil && cluster.Spec.Monitoring.Prometheus != nil {
		if cluster.Spec.Monitoring.Prometheus.Enabled {
			logrus.Infof("Prometheus is enabled in StorageCluster")
			if cluster.Spec.Monitoring.Prometheus.AlertManager != nil && cluster.Spec.Monitoring.Prometheus.AlertManager.Enabled {
				logrus.Infof("AlertManager is enabled in StorageCluster")
				return ValidateAlertManagerEnabled(pxImageList, cluster, timeout, interval)
			}
			logrus.Infof("AlertManager is not enabled in StorageCluster")
			return ValidateAlertManagerDisabled(pxImageList, cluster, timeout, interval)
		}
	}

	logrus.Infof("AlertManager is disabled in StorageCluster")
	return ValidateAlertManagerDisabled(pxImageList, cluster, timeout, interval)
}

// ValidateAlertManagerEnabled validates alert manager components are enabled/installed as expected
func ValidateAlertManagerEnabled(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	// Validate Alert Manager statefulset, pods and images
	logrus.Info("Validating AlertManager components")
	alertManagerSset := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "alertmanager-portworx",
			Namespace: cluster.Namespace,
		},
	}
	if err := appops.Instance().ValidateStatefulSet(alertManagerSset, timeout); err != nil {
		return err
	}

	alertManagerSset, err := appops.Instance().GetStatefulSet(alertManagerSset.Name, alertManagerSset.Namespace)
	if err != nil {
		return err
	}

	pods, err := appops.Instance().GetStatefulSetPods(alertManagerSset)
	if err != nil {
		return err
	}

	if image, ok := pxImageList["alertManager"]; ok {
		if err := validateContainerImageInsidePods(cluster, image, "alertmanager", &v1.PodList{Items: pods}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find image for telemetry")
	}

	K8sVer1_22, _ := version.NewVersion("1.22")
	kubeVersion, _, err := GetFullVersion()
	if err != nil {
		return err
	}

	// NOTE: Prometheus uses different images for k8s 1.22 and up then for 1.21 and below
	var configReloaderImageName string
	if kubeVersion != nil && kubeVersion.GreaterThanOrEqual(K8sVer1_22) {
		value, ok := pxImageList["prometheusConfigReloader"]
		if !ok {
			return fmt.Errorf("failed to find image for prometheus config reloader")
		}
		configReloaderImageName = value
	} else {
		value, ok := pxImageList["prometheusConfigMapReload"]
		if !ok {
			return fmt.Errorf("failed to find image for prometheus configmap reloader")
		}
		configReloaderImageName = value
	}

	if err := validateContainerImageInsidePods(cluster, configReloaderImageName, "config-reloader", &v1.PodList{Items: pods}); err != nil {
		return err
	}

	// Verify alert manager services
	if _, err := coreops.Instance().GetService("alertmanager-portworx", cluster.Namespace); err != nil {
		return fmt.Errorf("failed to get service alertmanager-portworx")
	}

	if _, err := coreops.Instance().GetService("alertmanager-operated", cluster.Namespace); err != nil {
		return fmt.Errorf("failed to get service alertmanager-operated")
	}

	logrus.Infof("Alert manager is enabled and deployed")
	return nil
}

// ValidateAlertManagerDisabled validates alert manager components are disabled/uninstalled as expected
func ValidateAlertManagerDisabled(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	t := func() (interface{}, bool, error) {
		if _, err := appops.Instance().GetStatefulSet("alertmanager-portworx", cluster.Namespace); !errors.IsNotFound(err) {
			return "", true, fmt.Errorf("wait for stateful alertmanager-portworx deletion, err %v", err)
		}

		if cluster.Spec.Monitoring != nil && cluster.Spec.Monitoring.Prometheus != nil && cluster.Spec.Monitoring.Prometheus.AlertManager != nil {
			_, err := coreops.Instance().GetService("alertmanager-portworx", cluster.Namespace)
			if !cluster.Spec.Monitoring.Prometheus.AlertManager.Enabled && !errors.IsNotFound(err) {
				return "", true, fmt.Errorf("wait for service alertmanager-portworx deletion, err %v", err)
			}
		}

		if _, err := coreops.Instance().GetService("alertmanager-operated", cluster.Namespace); !errors.IsNotFound(err) {
			return "", true, fmt.Errorf("wait for service alertmanager-operated deletion, err %v", err)
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	logrus.Infof("Alert manager components do not exist")
	return nil
}

// ValidateTelemetryV2Enabled validates telemetry component is running as expected
func ValidateTelemetryV2Enabled(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Telemetry components are enabled")

	t := func() (interface{}, bool, error) {
		// Validate px-telemetry-registration deployment, pods and container images
		if err := validatePxTelemetryRegistrationV2(pxImageList, cluster, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate px-telemetry-metrics  deployment, pods and container images
		// Skipped because PWX-27401
		// if err := validatePxTelemetryMetricsCollectorV2(pxImageList, cluster, timeout, interval); err != nil {
		// 	return nil, true, err
		// }

		// Validate px-telemetry-phonehome daemonset, pods and container images
		if err := validatePxTelemetryPhonehomeV2(pxImageList, cluster, timeout, interval); err != nil {
			return nil, true, err
		}

		// Verify telemetry roles
		if _, err := rbacops.Instance().GetRole("px-telemetry", cluster.Namespace); err != nil {
			return nil, true, err
		}

		// Verify telemetry rolebindings
		if _, err := rbacops.Instance().GetRoleBinding("px-telemetry", cluster.Namespace); err != nil {
			return nil, true, err
		}

		// Verify telemetry clusterroles
		if _, err := rbacops.Instance().GetClusterRole("px-telemetry"); err != nil {
			return nil, true, err
		}

		// Verify telemetry clusterrolebindings
		if _, err := rbacops.Instance().GetClusterRoleBinding("px-telemetry"); err != nil {
			return nil, true, err
		}

		// Verify telemetry configmaps
		// if _, err := coreops.Instance().GetConfigMap("px-telemetry-collector", cluster.Namespace); err != nil {
		// 	return nil, true, err
		// }

		// if _, err := coreops.Instance().GetConfigMap("px-telemetry-collector-proxy", cluster.Namespace); err != nil {
		// 	return nil, true, err
		// }

		if _, err := coreops.Instance().GetConfigMap("px-telemetry-phonehome", cluster.Namespace); err != nil {
			return nil, true, err
		}

		if _, err := coreops.Instance().GetConfigMap("px-telemetry-phonehome-proxy", cluster.Namespace); err != nil {
			return nil, true, err
		}

		if _, err := coreops.Instance().GetConfigMap("px-telemetry-register", cluster.Namespace); err != nil {
			return nil, true, err
		}

		if _, err := coreops.Instance().GetConfigMap("px-telemetry-register-proxy", cluster.Namespace); err != nil {
			return nil, true, err
		}

		if _, err := coreops.Instance().GetConfigMap("px-telemetry-tls-certificate", cluster.Namespace); err != nil {
			return nil, true, err
		}

		// Verify telemetry serviceaccounts
		if _, err := coreops.Instance().GetServiceAccount("px-telemetry", cluster.Namespace); err != nil {
			return nil, true, err
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	// Validate Telemetry is Healthy in pxctl status
	if err := validateTelemetryStatusInPxctl(true, cluster); err != nil {
		return fmt.Errorf("failed to validate that Telemetry is Healthy in pxctl status, Err: %v", err)
	}

	logrus.Infof("All Telemetry components were successfully enabled/installed")
	return nil
}

func validatePxAuthOnPxNodes(pxAuthShouldBeEnabled bool, cluster *corev1.StorageCluster) error {
	listOptions := map[string]string{"name": "portworx"}
	cmd := "pxctl status"
	cmdWithPxAuthToken := ""

	logrus.Infof("Run pxctl status on all PX nodes to determine if PX Auth is enabled/disabled properly")
	t := func() (interface{}, bool, error) {
		if pxAuthShouldBeEnabled {
			token, err := getSecurityAdminToken(cluster.Namespace)
			if err != nil {
				return nil, true, err
			}
			cmdWithPxAuthToken = fmt.Sprintf("PXCTL_AUTH_TOKEN=%s %s", token, cmd)
		}

		// Get Portworx pods
		pxPods, err := coreops.Instance().GetPods(cluster.Namespace, listOptions)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get PX pods, Err: %v", err)
		}

		for _, pxPod := range pxPods.Items {
			// Validate PX pod is ready to run command on
			if !coreops.Instance().IsPodReady(pxPod) {
				return nil, true, fmt.Errorf("[%s] PX pod is not in Ready state", pxPod.Name)
			}

			if pxAuthShouldBeEnabled {
				// Should expect Auth errors as we are running command without Auth token
				_, err := runCmdInsidePxPod(&pxPod, cmd, cluster.Namespace, true)
				if err == nil {
					return nil, true, fmt.Errorf("[%s (%s)] got no errors trying to run [%s] command without an Auth token, when Security is enabled, expected this to fail", pxPod.Spec.NodeName, pxPod.Name, cmd)
				}
				logrus.Debugf("[%s (%s)] Got expected return when running [%s] command without an Auth token, since Security is enabled, expected errors: %v", pxPod.Spec.NodeName, pxPod.Name, cmd, err)

				// Should not expect Auth errors as we are running command with Auth token
				_, err = runCmdInsidePxPod(&pxPod, cmdWithPxAuthToken, cluster.Namespace, false)
				if err != nil {
					return nil, true, fmt.Errorf("[%s (%s)] got error trying to run [%s] command even with Auth token, Err: %v", pxPod.Spec.NodeName, pxPod.Name, cmdWithPxAuthToken, err)
				}
				logrus.Debugf("[%s (%s)] Got no errors when running [%s] command with Auth token, as expected", pxPod.Spec.NodeName, pxPod.Name, cmdWithPxAuthToken)
				logrus.Infof("[%s (%s)] PX Auth is enabled on node", pxPod.Spec.NodeName, pxPod.Name)
			} else {
				// Should not expect Auth errors as PX Auth should be disabled
				_, err := runCmdInsidePxPod(&pxPod, cmd, cluster.Namespace, false)
				if err != nil {
					return nil, true, fmt.Errorf("got error trying to get %s, Err: %v", cmd, err)
				}
				logrus.Debugf("[%s (%s)] Got no errors when running [%s] command without an Auth token, as expected", pxPod.Spec.NodeName, pxPod.Name, cmd)
				logrus.Infof("[%s (%s)] PX Auth is disabled on node", pxPod.Spec.NodeName, pxPod.Name)
			}
		}

		return nil, false, nil
	}

	_, err := task.DoRetryWithTimeout(t, defaultPxAuthValidationTimeout, defaultPxAuthValidationInterval)
	if err != nil {
		return err
	}
	return nil

}

func validateTelemetryStatusInPxctl(telemetryShouldBeEnabled bool, cluster *corev1.StorageCluster) error {
	listOptions := map[string]string{"name": "portworx"}
	cmd := "pxctl status | grep Telemetry:"

	logrus.Infof("Validate Telemetry pxctl status on all PX nodes")
	t := func() (interface{}, bool, error) {
		if cluster.Spec.Security != nil && cluster.Spec.Security.Enabled {
			token, err := getSecurityAdminToken(cluster.Namespace)
			if err != nil {
				return nil, true, err
			}
			cmd = fmt.Sprintf("PXCTL_AUTH_TOKEN=%s %s", token, cmd)
		}

		// Get Portworx pods
		pxPods, err := coreops.Instance().GetPods(cluster.Namespace, listOptions)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get PX pods, Err: %v", err)
		}

		for _, pxPod := range pxPods.Items {
			// Validate PX pod is ready to run command on
			if !coreops.Instance().IsPodReady(pxPod) {
				return nil, true, fmt.Errorf("[%s (%s)] PX pod is not in Ready state", pxPod.Spec.NodeName, pxPod.Name)
			}

			output, err := runCmdInsidePxPod(&pxPod, cmd, cluster.Namespace, false)
			if err != nil {
				return nil, true, fmt.Errorf("got error while trying to get Telemetry status from pxctl, Err: %v", err)
			}

			if telemetryShouldBeEnabled && !strings.Contains(output, "Healthy") {
				return nil, true, fmt.Errorf("[%s (%s)] Telemetry is enabled and should be Healthy in pxctl status on PX node, but got [%s]", pxPod.Spec.NodeName, pxPod.Name, strings.TrimSpace(output))
			} else if !telemetryShouldBeEnabled && !strings.Contains(output, "Disabled") {
				return nil, true, fmt.Errorf("[%s (%s)] Telemetry is not enabled and should be Disabled in pxctl status on PX node, but got [%s]", pxPod.Spec.NodeName, pxPod.Name, strings.TrimSpace(output))
			} else if !strings.Contains(output, "Disabled") && !strings.Contains(output, "Healthy") {
				return nil, true, fmt.Errorf("[%s (%s)] Telemetry is Enabled=%v, but pxctl on PX node returned unexpected status [%s]", pxPod.Spec.NodeName, pxPod.Name, telemetryShouldBeEnabled, output)
			}

			logrus.Infof("[%s(%s)] Telemetry is Enabled=%v and pxctl status on PX node reports [%s]", pxPod.Spec.NodeName, pxPod.Name, telemetryShouldBeEnabled, strings.TrimSpace(output))
		}
		return nil, false, nil
	}

	_, err := task.DoRetryWithTimeout(t, defaultTelemetryInPxctlValidationTimeout, defaultTelemetryInPxctlValidationInterval)
	if err != nil {
		return err
	}
	return nil
}

func getSecurityAdminToken(namespace string) (string, error) {
	logrus.Debug("PX Security is enabled, getting token from px-admin-token secret")

	secret, err := coreops.Instance().GetSecret("px-admin-token", namespace)
	if err != nil {
		return "", fmt.Errorf("failed to get px-admin-token secret, Err: %v", err)
	}
	token := string(secret.Data["auth-token"])

	return token, nil
}

func validatePxTelemetryPhonehomeV2(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	// Validate px-telemetry-phonehome daemonset, pods and container images
	logrus.Info("Validate px-telemetry-phonehome daemonset and images")
	if err := appops.Instance().ValidateDaemonSet("px-telemetry-phonehome", cluster.Namespace, timeout); err != nil {
		return err
	}

	telemetryPhonehomeDs, err := appops.Instance().GetDaemonSet("px-telemetry-phonehome", cluster.Namespace)
	if err != nil {
		return err
	}

	pods, err := appops.Instance().GetDaemonSetPods(telemetryPhonehomeDs)
	if err != nil {
		return err
	}

	// Verify init-cont image inside px-telemetry-phonehome[init-cont]
	if image, ok := pxImageList["telemetryProxy"]; ok {
		if err := validateContainerImageInsidePods(cluster, image, "init-cont", &v1.PodList{Items: pods}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find image for px-telemetry-phonehome[init-cont]")
	}

	// Verify init-cont image inside px-telemetry-phonehome[log-upload-service]
	if image, ok := pxImageList["logUploader"]; ok {
		if err := validateContainerImageInsidePods(cluster, image, "log-upload-service", &v1.PodList{Items: pods}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find image for px-telemetry-phonehome[log-upload-service]")
	}

	// Verify collector container image inside px-telemetry-phonehome[envoy]
	if image, ok := pxImageList["telemetryProxy"]; ok {
		if err := validateContainerImageInsidePods(cluster, image, "envoy", &v1.PodList{Items: pods}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find image for px-telemetry-phonehome[envoy]")
	}

	return nil
}

// func validatePxTelemetryMetricsCollectorV2(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
// 	// Validate px-telemetry-metrics-collector deployment, pods and container images
// 	logrus.Info("Validate px-telemetry-metrics-collector deployment and images")
// 	metricsCollectorDep := &appsv1.Deployment{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "px-telemetry-metrics-collector",
// 			Namespace: cluster.Namespace,
// 		},
// 	}
// 	if err := appops.Instance().ValidateDeployment(metricsCollectorDep, timeout, interval); err != nil {
// 		return err
// 	}
//
// 	pods, err := appops.Instance().GetDeploymentPods(metricsCollectorDep)
// 	if err != nil {
// 		return err
// 	}
//
// 	// Validate image inside px-metric-collector[init-cont]
// 	if image, ok := pxImageList["telemetryProxy"]; ok {
// 		if err := validateContainerImageInsidePods(cluster, image, "init-cont", &v1.PodList{Items: pods}); err != nil {
// 			return err
// 		}
// 	} else {
// 		return fmt.Errorf("failed to find image for px-telemetry-metrics-collector[init-cont]")
// 	}
//
// 	// Validate image inside px-metrics-collector[collector]
// 	if image, ok := pxImageList["metricsCollector"]; ok {
// 		if err := validateContainerImageInsidePods(cluster, image, "collector", &v1.PodList{Items: pods}); err != nil {
// 			return err
// 		}
// 	} else {
// 		return fmt.Errorf("failed to find image for px-telemetry-metrics-collector[collector]")
// 	}
//
// 	// Validate image inside px-metric-collector[envoy]
// 	if image, ok := pxImageList["telemetryProxy"]; ok {
// 		if err := validateContainerImageInsidePods(cluster, image, "envoy", &v1.PodList{Items: pods}); err != nil {
// 			return err
// 		}
// 	} else {
// 		return fmt.Errorf("failed to find image for px-telemetry-metrics-collector[envoy]")
// 	}
//
// 	return nil
// }

func validatePxTelemetryRegistrationV2(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	// Validate px-telemetry-registration deployment, pods and container images
	logrus.Info("Validate px-telemetry-registration deployment and images")
	registrationServiceDep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "px-telemetry-registration",
			Namespace: cluster.Namespace,
		},
	}
	if err := appops.Instance().ValidateDeployment(registrationServiceDep, timeout, interval); err != nil {
		return err
	}

	pods, err := appops.Instance().GetDeploymentPods(registrationServiceDep)
	if err != nil {
		return err
	}

	if image, ok := pxImageList["telemetry"]; ok {
		if err := validateContainerImageInsidePods(cluster, image, "registration", &v1.PodList{Items: pods}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find image for telemetry")
	}

	if image, ok := pxImageList["telemetryProxy"]; ok {
		if err := validateContainerImageInsidePods(cluster, image, "envoy", &v1.PodList{Items: pods}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find image for envoy")
	}

	return nil
}

// ValidateTelemetryV2Disabled validates telemetry component is running as expected
func ValidateTelemetryV2Disabled(cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Telemetry components are disabled")

	t := func() (interface{}, bool, error) {
		_, err := appops.Instance().GetDeployment("px-telemetry-metrics-collector", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry-metrics-collector deployment, waiting for deletion")
		}

		_, err = appops.Instance().GetDeployment("px-telemetry-registration", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry-registration deployment, waiting for deletion")
		}

		_, err = appops.Instance().GetDaemonSet("px-telemetry-phonehome", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry-phonehome daemonset, waiting for deletion")
		}

		// Verify telemetry roles
		_, err = rbacops.Instance().GetRole("px-telemetry", cluster.Name)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry role, waiting for deletion")
		}

		// Verify telemetry rolebindings
		_, err = rbacops.Instance().GetRoleBinding("px-telemetry", cluster.Name)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry rolebinding, waiting for deletion")
		}

		// Verify telemetry clusterroles
		_, err = rbacops.Instance().GetClusterRole("px-telemetry")
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry clusterrole, waiting for deletion")
		}

		// Verify telemetry clusterrolebindings
		_, err = rbacops.Instance().GetClusterRoleBinding("px-telemetry")
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry clusterrolebinding, waiting for deletion")
		}

		// Verify telemetry configmaps
		_, err = coreops.Instance().GetConfigMap("px-telemetry-collector", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry-collector configmap, waiting for deletion")
		}

		_, err = coreops.Instance().GetConfigMap("px-telemetry-collector-proxy", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry-collector-proxy configmap, waiting for deletion")
		}

		_, err = coreops.Instance().GetConfigMap("px-telemetry-phonehome", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry-phonehome configmap, waiting for deletion")
		}

		_, err = coreops.Instance().GetConfigMap("px-telemetry-phonehome-proxy", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry-phonehome-proxy configmap, waiting for deletion")
		}

		_, err = coreops.Instance().GetConfigMap("px-telemetry-register", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry-register configmap, waiting for deletion")
		}

		_, err = coreops.Instance().GetConfigMap("px-telemetry-register-proxy", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry-register-proxy configmap, wait for deletion")
		}

		_, err = coreops.Instance().GetConfigMap("px-telemetry-tls-certificate", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry-tls-certificate configmap, waiting for deletion")
		}

		// Verify telemetry serviceaccounts
		_, err = coreops.Instance().GetServiceAccount("px-telemetry", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("found px-telemetry serviceaccount, waiting for deletion")
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	// Validate Telemetry is Disabled in pxctl status
	if err := validateTelemetryStatusInPxctl(false, cluster); err != nil {
		return fmt.Errorf("failed to validate that Telemetry is Disabled in pxctl status, Err: %v", err)
	}

	logrus.Infof("All Telemetry components were successfully disabled/uninstalled")
	return nil
}

// ValidateTelemetryV1Enabled validates telemetry component is running as expected
func ValidateTelemetryV1Enabled(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Telemetry components are enabled")

	// Wait for the deployment to become online
	// TODO: Skipped because PWX-27401, revert later
	// dep := appsv1.Deployment{
	// 	ObjectMeta: metav1.ObjectMeta{
	// 		Name:      "px-metrics-collector",
	// 		Namespace: cluster.Namespace,
	// 	},
	// }

	t := func() (interface{}, bool, error) {
		// if err := appops.Instance().ValidateDeployment(&dep, timeout, interval); err != nil {
		// 	return nil, true, err
		// }

		/* TODO: We need to make this work for spawn
		expectedDeployment := GetExpectedDeployment(&testing.T{}, "metricsCollectorDeployment.yaml")
		*/

		// deployment, err := appops.Instance().GetDeployment(dep.Name, dep.Namespace)
		// if err != nil {
		// 	return nil, true, err
		// }

		/* TODO: We need to make this work for spawn
		if equal, err := util.DeploymentDeepEqual(expectedDeployment, deployment); !equal {
			return err
		}
		*/

		// _, err = rbacops.Instance().GetRole("px-metrics-collector", cluster.Namespace)
		// if err != nil {
		// 	return nil, true, err
		// }

		// _, err = rbacops.Instance().GetRoleBinding("px-metrics-collector", cluster.Namespace)
		// if err != nil {
		// 	return nil, true, err
		// }

		// Verify telemetry config map
		_, err := coreops.Instance().GetConfigMap("px-telemetry-config", cluster.Namespace)
		if err != nil {
			return nil, true, err
		}

		// Verify collector config map
		// _, err = coreops.Instance().GetConfigMap("px-collector-config", cluster.Namespace)
		// if err != nil {
		// 	return nil, true, err
		// }

		// Verify collector proxy config map
		// _, err = coreops.Instance().GetConfigMap("px-collector-proxy-config", cluster.Namespace)
		// if err != nil {
		// 	return nil, true, err
		// }

		// Verify collector service account
		// _, err = coreops.Instance().GetServiceAccount("px-metrics-collector", cluster.Namespace)
		// if err != nil {
		// 	return nil, true, err
		// }

		// Verify metrics collector image
		// imageName, ok := pxImageList["metricsCollector"]
		// if !ok {
		// 	return nil, true, fmt.Errorf("failed to find image for metrics collector")
		// }
		// imageName = util.GetImageURN(cluster, imageName)

		// if deployment.Spec.Template.Spec.Containers[0].Image != imageName {
		// 	return nil, true, fmt.Errorf("collector image mismatch, image: %s, expected: %s",
		// 		deployment.Spec.Template.Spec.Containers[0].Image,
		// 		imageName)
		// }

		// // Verify metrics collector proxy image
		// imageName, ok = pxImageList["metricsCollectorProxy"]
		// if !ok {
		// 	return nil, true, fmt.Errorf("failed to find image for metrics collector proxy")
		// }
		// imageName = util.GetImageURN(cluster, imageName)

		// if deployment.Spec.Template.Spec.Containers[1].Image != imageName {
		// 	return nil, true, fmt.Errorf("collector proxy image mismatch, image: %s, expected: %s",
		// 		deployment.Spec.Template.Spec.Containers[1].Image,
		// 		imageName)
		// }

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	// Validate Telemetry is Healthy in pxctl status
	if err := validateTelemetryStatusInPxctl(true, cluster); err != nil {
		return fmt.Errorf("failed to validate that Telemetry is Healthy in pxctl status, Err: %v", err)
	}

	logrus.Infof("Telemetry is enabled")
	return nil
}

// validatePodTopologySpreadConstraints validates pod topology spread constraints
func validatePodTopologySpreadConstraints(deployment *appsv1.Deployment, timeout, interval time.Duration) error {
	t := func() (interface{}, bool, error) {
		nodeList, err := coreops.Instance().GetNodes()
		if err != nil {
			return nil, true, fmt.Errorf("failed to get k8s nodes")
		}
		existingDeployment, err := appops.Instance().GetDeployment(deployment.Name, deployment.Namespace)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get deployment %s/%s", deployment.Namespace, deployment.Name)
		}
		expectedConstraints, err := util.GetTopologySpreadConstraintsFromNodes(nodeList, existingDeployment.Spec.Template.Labels)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get expected pod topology spread constraints from %s/%s deployment template",
				deployment.Namespace, deployment.Name)
		}
		existingConstraints := existingDeployment.Spec.Template.Spec.TopologySpreadConstraints
		if !reflect.DeepEqual(expectedConstraints, existingConstraints) {
			return nil, true, fmt.Errorf("failed to validate deployment pod topology spread constraints for %s/%s, expected constraints: %+v, actual constraints: %+v",
				deployment.Namespace, deployment.Name, expectedConstraints, existingConstraints)
		}
		return nil, false, nil
	}

	logrus.Infof("validating deployment %s/%s pod topology spread constraints", deployment.Namespace, deployment.Name)
	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}
	return nil
}

func isPVCControllerEnabled(cluster *corev1.StorageCluster) bool {
	enabled, err := strconv.ParseBool(cluster.Annotations["portworx.io/pvc-controller"])
	if err == nil {
		return enabled
	}

	// If portworx is disabled, then do not run pvc controller unless explicitly told to.
	if !isPortworxEnabled(cluster) {
		return false
	}

	// Enable PVC controller for managed kubernetes services. Also enable it for openshift,
	// only if Portworx service is not deployed in kube-system namespace.
	if isPKS(cluster) || isEKS(cluster) ||
		isGKE(cluster) || isAKS(cluster) ||
		cluster.Namespace != "kube-system" {
		return true
	}
	return false
}

func isPortworxEnabled(cluster *corev1.StorageCluster) bool {
	disabled, err := strconv.ParseBool(cluster.Annotations["operator.libopenstorage.org/disable-storage"])
	return err != nil || !disabled
}

func isPortworxProxyEnabled(cluster *corev1.StorageCluster) bool {
	enabled, err := strconv.ParseBool(cluster.Annotations["portworx.io/portworx-proxy"])
	// If annotation is not present or invalid, then portworx proxy is considered enabled
	return (err != nil || enabled) &&
		cluster.Namespace != "kube-system" &&
		startPort(cluster) != 9001 &&
		isPortworxEnabled(cluster)
}

func isPKS(cluster *corev1.StorageCluster) bool {
	enabled, err := strconv.ParseBool(cluster.Annotations["portworx.io/is-pks"])
	return err == nil && enabled
}

func isGKE(cluster *corev1.StorageCluster) bool {
	enabled, err := strconv.ParseBool(cluster.Annotations["portworx.io/is-gke"])
	return err == nil && enabled
}

func isAKS(cluster *corev1.StorageCluster) bool {
	enabled, err := strconv.ParseBool(cluster.Annotations["portworx.io/is-aks"])
	return err == nil && enabled
}

func isEKS(cluster *corev1.StorageCluster) bool {
	enabled, err := strconv.ParseBool(cluster.Annotations["portworx.io/is-eks"])
	return err == nil && enabled
}

func isOpenshift(cluster *corev1.StorageCluster) bool {
	enabled, err := strconv.ParseBool(cluster.Annotations["portworx.io/is-openshift"])
	return err == nil && enabled
}

func startPort(cluster *corev1.StorageCluster) int {
	startPort := 9001
	if cluster.Spec.StartPort != nil {
		startPort = int(*cluster.Spec.StartPort)
	} else if isOpenshift(cluster) {
		startPort = 17001
	}
	return startPort
}

// GetK8SVersion gets and return K8S server version
func GetK8SVersion() (string, error) {
	kbVerRegex := regexp.MustCompile(`^(v\d+\.\d+\.\d+).*`)
	k8sVersion, err := coreops.Instance().GetVersion()
	if err != nil {
		return "", fmt.Errorf("unable to get kubernetes version: %v", err)
	}
	matches := kbVerRegex.FindStringSubmatch(k8sVersion.GitVersion)
	if len(matches) < 2 {
		return "", fmt.Errorf("invalid kubernetes version received: %v", k8sVersion.GitVersion)
	}
	return matches[1], nil
}

// GetImagesFromVersionURL gets images from version URL
func GetImagesFromVersionURL(url, k8sVersion string) (map[string]string, error) {
	imageListMap := make(map[string]string)

	// Construct PX version URL
	pxVersionURL, err := ConstructVersionURL(url, k8sVersion)
	if err != nil {
		return nil, err
	}
	logrus.Infof("Get component images from version URL %s", pxVersionURL)

	resp, err := http.Get(pxVersionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to send GET request to %s, Err: %v", pxVersionURL, err)
	}

	htmlData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %+v", resp.Body)
	}

	for _, line := range strings.Split(string(htmlData), "\n") {
		if strings.Contains(line, "components") || line == "" {
			continue
		}

		imageNameSplit := strings.Split(strings.TrimSpace(line), ": ")

		if strings.Contains(line, "version") {
			imageListMap["version"] = fmt.Sprintf("portworx/oci-monitor:%s", imageNameSplit[1])
			continue
		}
		imageListMap[imageNameSplit[0]] = imageNameSplit[1]
	}

	return imageListMap, nil
}

// ConstructVersionURL constructs Portworx version URL that contains component images
func ConstructVersionURL(specGenURL, k8sVersion string) (string, error) {
	u, err := url.Parse(specGenURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL [%s], Err: %v", specGenURL, err)
	}
	q := u.Query()
	q.Set("kbver", k8sVersion)
	u.Path = path.Join(u.Path, "version")
	u.RawQuery = q.Encode()

	return u.String(), nil
}

// ConstructPxReleaseManifestURL constructs Portworx install URL
func ConstructPxReleaseManifestURL(specGenURL string) (string, error) {
	u, err := url.Parse(specGenURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL [%s], Err: %v", specGenURL, err)
	}

	u.Path = path.Join(u.Path, "version")
	return u.String(), nil
}

func validateStorageClusterInState(cluster *corev1.StorageCluster, status corev1.ClusterConditionStatus) func() (interface{}, bool, error) {
	return func() (interface{}, bool, error) {
		cluster, err := operatorops.Instance().GetStorageCluster(cluster.Name, cluster.Namespace)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get StorageCluster %s in %s, Err: %v", cluster.Name, cluster.Namespace, err)
		}
		if cluster.Status.Phase != string(status) {
			if cluster.Status.Phase == "" {
				return nil, true, fmt.Errorf("failed to get cluster status")
			}
			return nil, true, fmt.Errorf("cluster state: %s", cluster.Status.Phase)
		}
		return cluster, false, nil
	}
}

func validateAllStorageNodesInState(namespace string, status corev1.NodeConditionStatus) func() (interface{}, bool, error) {
	return func() (interface{}, bool, error) {
		// Get all StorageNodes
		storageNodeList, err := operatorops.Instance().ListStorageNodes(namespace)
		if err != nil {
			return nil, true, fmt.Errorf("failed to list StorageNodes in %s, Err: %v", namespace, err)
		}

		for _, node := range storageNodeList.Items {
			if node.Status.Phase != string(status) {
				return nil, true, fmt.Errorf("StorageNode %s in %s is in state %v", node.Name, namespace, node.Status.Phase)
			}
		}

		return nil, false, nil
	}
}

// ValidateStorageClusterIsOnline wait for storage cluster to become online.
func ValidateStorageClusterIsOnline(cluster *corev1.StorageCluster, timeout, interval time.Duration) (*corev1.StorageCluster, error) {
	out, err := task.DoRetryWithTimeout(validateStorageClusterInState(cluster, corev1.ClusterOnline), timeout, interval)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for StorageCluster to be ready, Err: %v", err)
	}
	cluster = out.(*corev1.StorageCluster)

	return cluster, nil
}

func validateStorageClusterIsFailed(cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	_, err := task.DoRetryWithTimeout(validateAllStorageNodesInState(cluster.Namespace, corev1.NodeFailedStatus), timeout, interval)
	if err != nil {
		return fmt.Errorf("failed to wait for StorageNodes to be failed, Err: %v", err)
	}
	return nil
}

func validateStorageClusterIsInitializing(cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	_, err := task.DoRetryWithTimeout(validateAllStorageNodesInState(cluster.Namespace, corev1.NodeInitStatus), timeout, interval)
	if err != nil {
		return fmt.Errorf("failed to wait for StorageNodes to be initializing, Err: %v", err)
	}
	return nil
}

// ValidateStorageClusterFailedEvents checks a StorageCluster installed, but has issues (logged events)
func ValidateStorageClusterFailedEvents(
	clusterSpec *corev1.StorageCluster,
	timeout, interval time.Duration,
	eventsFieldSelector string,
	eventsNewerThan time.Time,
	kubeconfig ...string,
) error {
	// Set kubeconfig
	if len(kubeconfig) != 0 && kubeconfig[0] != "" {
		os.Setenv("KUBECONFIG", kubeconfig[0])
	}

	// Validate StorageCluster started Initializing  (note, will be stuck in this phase...)
	err := validateStorageClusterIsInitializing(clusterSpec, timeout, interval)
	if err != nil {
		return err
	}

	// Get list of expected Portworx node names
	expectedPxNodeNameList, err := GetExpectedPxNodeNameList(clusterSpec)
	if err != nil {
		return err
	}

	// Validate Portworx pods
	// - we want POD in "Running" state, and have one container in READY=0/1 state
	podTestFn := func(pod v1.Pod) bool {
		if pod.Status.Phase != v1.PodRunning {
			return false
		}
		for _, c := range pod.Status.ContainerStatuses {
			if c.Started != nil && *c.Started && !c.Ready {
				return true
			}
		}
		return false
	}
	if err = validateStorageClusterPods(clusterSpec, expectedPxNodeNameList, timeout, interval, podTestFn); err != nil {
		return err
	}

	return validateK8Events(clusterSpec, timeout, interval, eventsFieldSelector, eventsNewerThan, "")
}

// ValidateStorageClusterInstallFailedWithEvents checks a StorageCluster installation failed with a logged event
func ValidateStorageClusterInstallFailedWithEvents(
	clusterSpec *corev1.StorageCluster,
	timeout, interval time.Duration,
	eventsFieldSelector string,
	eventsNewerThan time.Time,
	reason string,
) error {
	// Validate StorageCluster started Initializing  (note: will be stuck in this phase...)
	err := validateStorageClusterIsInitializing(clusterSpec, timeout, interval)
	if err != nil {
		return err
	}
	logrus.Debug("Validating K8 event for NodeStartFailure")
	return validateK8Events(clusterSpec, timeout, interval, eventsFieldSelector, eventsNewerThan, reason)
}

func validateK8Events(
	clusterSpec *corev1.StorageCluster,
	timeout, interval time.Duration,
	eventsFieldSelector string,
	eventsNewerThan time.Time,
	reason string) error {
	// List newer events -- ensure requested `eventsFieldSelector` is listed
	t := func() (interface{}, bool, error) {
		tmout := int64(timeout.Seconds() / 2)
		events, err := coreops.Instance().ListEvents(clusterSpec.Namespace, metav1.ListOptions{
			FieldSelector:  eventsFieldSelector,
			TimeoutSeconds: &tmout,
			Limit:          100,
		})
		if err != nil {
			return "", true, err
		}
		seen := make(map[string]bool)
		v1Time := metav1.NewTime(eventsNewerThan)
		for _, ev := range events.Items {
			// Checking for a specific failure reason from kubernetes
			if ev.LastTimestamp.Before(&v1Time) ||
				(len(reason) > 0 && ev.Reason != reason) {
				continue
			}
			seen[ev.Source.Host] = true
		}
		if len(seen) <= 0 {
			return "", true, fmt.Errorf("no %s events found", eventsFieldSelector)
		}
		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// CreateClusterWithTLS is a helper method
func CreateClusterWithTLS(caCertFileName, serverCertFileName, serverKeyFileName *string) *corev1.StorageCluster {
	var apicert *corev1.CertLocation = nil
	if caCertFileName != nil {
		apicert = &corev1.CertLocation{
			FileName: caCertFileName,
		}
	}
	var serverCert *corev1.CertLocation = nil
	if serverCertFileName != nil {
		serverCert = &corev1.CertLocation{
			FileName: serverCertFileName,
		}
	}
	var serverkey *corev1.CertLocation = nil
	if serverKeyFileName != nil {
		serverkey = &corev1.CertLocation{
			FileName: serverKeyFileName,
		}
	}

	cluster := &corev1.StorageCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "px-cluster",
			Namespace: "kube-system",
		},
		Spec: corev1.StorageClusterSpec{
			Security: &corev1.SecuritySpec{
				Enabled: true,
				Auth: &corev1.AuthSpec{
					Enabled: BoolPtr(false),
				},
				TLS: &corev1.TLSSpec{
					Enabled:    BoolPtr(true),
					RootCA:     apicert,
					ServerCert: serverCert,
					ServerKey:  serverkey,
				},
			},
		},
	}
	return cluster
}

// BoolPtr returns a pointer to provided bool value
func BoolPtr(val bool) *bool {
	return &val
}
