package test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	cryptoTls "crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
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
	"github.com/libopenstorage/operator/pkg/apis"
	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/libopenstorage/operator/pkg/mock"
	"github.com/libopenstorage/operator/pkg/util"
	ocp_configv1 "github.com/openshift/api/config/v1"
	consolev1 "github.com/openshift/api/console/v1"
	routev1 "github.com/openshift/api/route/v1"
	ocp_secv1 "github.com/openshift/api/security/v1"
	appops "github.com/portworx/sched-ops/k8s/apps"
	coreops "github.com/portworx/sched-ops/k8s/core"
	k8serrors "github.com/portworx/sched-ops/k8s/errors"
	openshiftops "github.com/portworx/sched-ops/k8s/openshift"
	operatorops "github.com/portworx/sched-ops/k8s/operator"
	prometheusops "github.com/portworx/sched-ops/k8s/prometheus"
	rbacops "github.com/portworx/sched-ops/k8s/rbac"
	"github.com/portworx/sched-ops/task"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	appsv1 "k8s.io/api/apps/v1"
	certv1 "k8s.io/api/certificates/v1"
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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	affinityhelper "k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	cluster_v1alpha1 "sigs.k8s.io/cluster-api/pkg/apis/deprecated/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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

	// DefaultPxAzureSecretName is a default name for PX vSphere credentials secret
	DefaultPxAzureSecretName = "px-azure"

	// PxMasterVersion is a tag for Portworx master version
	PxMasterVersion = "4.0.0.0"

	// PxOperatorMasterVersion is a tag for PX Operator master version
	PxOperatorMasterVersion = "99.9.9"

	// AksPVCControllerSecurePort is the PVC controller secure port.
	AksPVCControllerSecurePort = "10261"

	pxAnnotationPrefix = "portworx.io"

	// TelemetryCertName is name of the telemetry cert.
	TelemetryCertName = "pure-telemetry-certs"

	// EnvKeyPortworxHTTPProxy env var to use http proxy
	EnvKeyPortworxHTTPProxy = "PX_HTTP_PROXY"
	// EnvKeyPortworxHTTPSProxy env var to use https proxy
	EnvKeyPortworxHTTPSProxy = "PX_HTTPS_PROXY"
	// HttpProtocolPrefix is the prefix for HTTP protocol
	HttpProtocolPrefix = "http://"
	// HttpsProtocolPrefix is the prefix for HTTPS protocol
	HttpsProtocolPrefix = "https://"

	// AnnotationTelemetryArcusLocation annotation indicates the location (internal/external) of Arcus
	// that CCM should use
	AnnotationTelemetryArcusLocation = pxAnnotationPrefix + "/arcus-location"

	// AnnotationIsPKS annotation indicating whether it is a PKS cluster
	AnnotationIsPKS = pxAnnotationPrefix + "/is-pks"

	// Telemetry default params
	productionArcusLocation         = "external"
	productionArcusRestProxyURL     = "rest.cloud-support.purestorage.com"
	productionArcusRegisterProxyURL = "register.cloud-support.purestorage.com"
	stagingArcusLocation            = "internal"
	stagingArcusRestProxyURL        = "rest.staging-cloud-support.purestorage.com"
	stagingArcusRegisterProxyURL    = "register.staging-cloud-support.purestorage.com"
	arcusPingInterval               = 6 * time.Second
	arcusPingRetry                  = 5

	defaultOcpClusterCheckTimeout  = 15 * time.Minute
	defaultOcpClusterCheckInterval = 1 * time.Minute

	// OCP Plugin
	clusterOperatorKind    = "ClusterOperator"
	clusterOperatorVersion = "config.openshift.io/v1"

	defaultTelemetrySecretValidationTimeout  = 30 * time.Second
	defaultTelemetrySecretValidationInterval = time.Second

	defaultTelemetryInPxctlValidationTimeout  = 20 * time.Minute
	defaultTelemetryInPxctlValidationInterval = 30 * time.Second

	defaultStorageNodesValidationTimeout  = 30 * time.Minute
	defaultStorageNodesValidationInterval = 30 * time.Second

	defaultDmthinValidationTimeout  = 20 * time.Minute
	defaultDmthinValidationInterval = 10 * time.Second

	defaultPxAuthValidationTimeout  = 20 * time.Minute
	defaultPxAuthValidationInterval = 30 * time.Second

	defaultDeleteStorageClusterTimeout  = 3 * time.Minute
	defaultDeleteStorageClusterInterval = 10 * time.Second

	defaultRunCmdInPxPodTimeout  = 25 * time.Second
	defaultRunCmdInPxPodInterval = 5 * time.Second

	defaultCheckFreshInstallTimeout  = 120 * time.Second
	defaultCheckFreshInstallInterval = 5 * time.Second

	etcHostsFile       = "/etc/hosts"
	tempEtcHostsMarker = "### px-operator unit-test"
)

// TestSpecPath is the path for all test specs. Due to currently functional test and
// unit test use different path, this needs to be set accordingly.
var TestSpecPath = "testspec"

var (
	opVer1_9_1, _                     = version.NewVersion("1.9.1-")
	opVer1_10, _                      = version.NewVersion("1.10.0-")
	opVer23_3, _                      = version.NewVersion("23.3.0-")
	opVer23_8, _                      = version.NewVersion("23.8.0-")
	opVer23_5, _                      = version.NewVersion("23.5.0-")
	opVer23_5_1, _                    = version.NewVersion("23.5.1-")
	opVer23_7, _                      = version.NewVersion("23.7.0-")
	minOpVersionForKubeSchedConfig, _ = version.NewVersion("1.10.2-")
	OpVer23_10_3, _                   = version.NewVersion("23.10.3-")

	minimumPxVersionCO, _    = version.NewVersion("3.2")
	minimumCcmGoVersionCO, _ = version.NewVersion("1.2.3")

	// OCP Dynamic Plugin is only supported in starting with OCP 4.12+ which is k8s v1.25.0+
	minK8sVersionForDynamicPlugin, _ = version.NewVersion("1.25.0")

	pxVer3_0, _  = version.NewVersion("3.0")
	pxVer2_13, _ = version.NewVersion("2.13")

	// minimumPxVersionCCMJAVA minimum PX version to install ccm-java
	minimumPxVersionCCMJAVA, _ = version.NewVersion("2.8")
	// minimumPxVersionCCMGO minimum PX version to install ccm-go
	minimumPxVersionCCMGO, _ = version.NewVersion("2.12")
)

// MockDriver creates a mock storage driver
func MockDriver(mockCtrl *gomock.Controller) *mock.MockDriver {
	return mock.NewMockDriver(mockCtrl)
}

// FakeK8sClient creates a fake controller-runtime Kubernetes client. Also
// adds the CRDs defined in this repository to the scheme
func FakeK8sClient(initObjects ...runtime.Object) client.Client {
	s := scheme.Scheme
	if err := apis.AddToScheme(s); err != nil {
		logrus.Error(err)
	}
	if err := monitoringv1.AddToScheme(s); err != nil {
		logrus.Error(err)
	}
	if err := cluster_v1alpha1.AddToScheme(s); err != nil {
		logrus.Error(err)
	}
	if err := ocp_configv1.AddToScheme(s); err != nil {
		logrus.Error(err)
	}
	if err := consolev1.AddToScheme(s); err != nil {
		logrus.Error(err)
	}
	if err := routev1.AddToScheme(s); err != nil {
		logrus.Error(err)
	}
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

// GetExpectedCSR returns the CSR object from given yaml spec file
func GetExpectedCSR(t *testing.T, fileName string) *certv1.CertificateSigningRequest {
	obj := getKubernetesObject(t, fileName)
	csr, ok := obj.(*certv1.CertificateSigningRequest)
	assert.True(t, ok, "Expected CertificateSigningRequest object")
	return csr
}

// getKubernetesObject returns a generic Kubernetes object from given yaml file
func getKubernetesObject(t *testing.T, fileName string) runtime.Object {
	json, err := os.ReadFile(path.Join(TestSpecPath, fileName))
	assert.NoError(t, err)
	s := scheme.Scheme
	err = apiextensionsv1beta1.AddToScheme(s)
	assert.NoError(t, err)
	err = apiextensionsv1.AddToScheme(s)
	assert.NoError(t, err)
	err = monitoringv1.AddToScheme(s)
	assert.NoError(t, err)
	err = ocp_secv1.Install(s)
	assert.NoError(t, err)
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
			_, err = fakeClient.ApiextensionsV1().
				CustomResourceDefinitions().
				UpdateStatus(context.TODO(), crd, metav1.UpdateOptions{})
			if err != nil {
				return false, err
			}
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
			_, err = fakeClient.ApiextensionsV1beta1().
				CustomResourceDefinitions().
				UpdateStatus(context.TODO(), crd, metav1.UpdateOptions{})
			if err != nil {
				return false, err
			}
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

	t := func() (interface{}, bool, error) {
		cluster, err = operatorops.Instance().GetStorageCluster(cluster.Name, cluster.Namespace)
		if err != nil && !errors.IsNotFound(err) {
			return nil, true, err
		}

		if cluster.Spec.DeleteStrategy == nil ||
			(cluster.Spec.DeleteStrategy.Type != corev1.UninstallAndWipeStorageClusterStrategyType &&
				cluster.Spec.DeleteStrategy.Type != corev1.UninstallStorageClusterStrategyType) {
			cluster.Spec.DeleteStrategy = &corev1.StorageClusterDeleteStrategy{
				Type: corev1.UninstallAndWipeStorageClusterStrategyType,
			}

			if _, err := operatorops.Instance().UpdateStorageCluster(cluster); err != nil {
				return nil, true, err
			}

			if err := validateTelemetrySecret(cluster, defaultTelemetrySecretValidationTimeout, defaultTelemetrySecretValidationInterval, false); err != nil {
				return nil, true, err
			}
		}

		if err := operatorops.Instance().DeleteStorageCluster(cluster.Name, cluster.Namespace); err != nil {
			return nil, true, err
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, defaultDeleteStorageClusterTimeout, defaultDeleteStorageClusterInterval); err != nil {
		return err
	}

	return nil
}

func validateTelemetrySecret(cluster *corev1.StorageCluster, timeout, interval time.Duration, force bool) error {
	t := func() (interface{}, bool, error) {
		secret, err := coreops.Instance().GetSecret("pure-telemetry-certs", cluster.Namespace)
		if err != nil {
			if errors.IsNotFound(err) && !force {
				// Skip secret existence validation
				return nil, false, nil
			}
			return nil, true, fmt.Errorf("failed to get secret pure-telemetry-certs: %v", err)
		}
		logrus.Debugf("Found secret %s", secret.Name)

		// Validate secret owner reference if telemetry enabled
		if cluster.Spec.Monitoring != nil && cluster.Spec.Monitoring.Telemetry != nil && cluster.Spec.Monitoring.Telemetry.Enabled {
			ownerRef := metav1.NewControllerRef(cluster, corev1.SchemeGroupVersion.WithKind("StorageCluster"))
			if cluster.Spec.DeleteStrategy != nil && cluster.Spec.DeleteStrategy.Type == corev1.UninstallAndWipeStorageClusterStrategyType {
				// Validate secret should have owner reference
				for _, reference := range secret.OwnerReferences {
					if reference.UID == ownerRef.UID {
						logrus.Debugf("Found ownerReference for StorageCluster %s in secret %s", ownerRef.Name, secret.Name)
						return nil, false, nil
					}
				}
				return nil, true, fmt.Errorf("waiting for ownerReference to be set to StorageCluster %s in secret %s", cluster.Name, secret.Name)
			}
			// Validate secret owner should not have owner reference
			// Do not validate reference for PX Operators below 1.10, as this was introduced in 1.10+, see PWX-26326 for more info
			opVersion, _ := GetPxOperatorVersion()
			if opVersion.GreaterThanOrEqual(opVer1_10) {
				for _, reference := range secret.OwnerReferences {
					if reference.UID == ownerRef.UID {
						return nil, true, fmt.Errorf("secret %s should not have ownerReference to StorageCluster %s", secret.Name, cluster.Name)
					}
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

// CreateAzureCredentialEnvVarsFromSecret check if px-vsphere-secret exists and returns vSphere crendentials Env vars
func CreateAzureCredentialEnvVarsFromSecret(namespace string) ([]v1.EnvVar, error) {
	var envVars []v1.EnvVar

	// Get PX vSphere secret
	_, err := coreops.Instance().GetSecret(DefaultPxAzureSecretName, namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			logrus.Warnf("Azure secret %s in not found in %s namespace", DefaultPxAzureSecretName, namespace)
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get secret %s in %s namespace, err %v", DefaultPxVsphereSecretName, namespace, err)
	}

	envVars = []v1.EnvVar{
		{
			Name: "AZURE_CLIENT_ID",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: DefaultPxAzureSecretName,
					},
					Key: "AZURE_CLIENT_ID",
				},
			},
		},
		{
			Name: "AZURE_CLIENT_SECRET",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: DefaultPxAzureSecretName,
					},
					Key: "AZURE_CLIENT_SECRET",
				},
			},
		},
		{
			Name: "AZURE_TENANT_ID",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: DefaultPxAzureSecretName,
					},
					Key: "AZURE_TENANT_ID",
				},
			},
		},
	}

	return envVars, nil
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

	freshInstall, err := IsThisFreshInstall(clusterSpec, defaultCheckFreshInstallTimeout, defaultCheckFreshInstallInterval)
	if err != nil {
		return err
	}

	if freshInstall {
		logrus.Debug("This is fresh PX installation!")
	}

	// Validate StorageCluster
	var liveCluster *corev1.StorageCluster
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

	// Get list of expected Portworx node names
	expectedPxNodeList, err := GetExpectedPxNodeList(clusterSpec)
	if err != nil {
		return err
	}

	// Validate StorageNodes
	if err = validateStorageNodes(pxImageList, clusterSpec, len(expectedPxNodeList), timeout, interval); err != nil {
		return err
	}

	// Validate Portworx pods
	podTestFn := func(pod v1.Pod) bool {
		return coreops.Instance().IsPodReady(pod)
	}
	if err = validateStorageClusterPods(clusterSpec, expectedPxNodeList, timeout, interval, podTestFn); err != nil {
		return err
	}

	// Validate Portworx nodes
	if err = validatePortworxNodes(liveCluster, len(expectedPxNodeList)); err != nil {
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

	// Validate dmthin
	if err = validateDmthinOnPxNodes(liveCluster); err != nil {
		return err
	}

	// Validate components
	if err = validateComponents(pxImageList, clusterSpec, liveCluster, timeout, interval); err != nil {
		return err
	}

	// Validate cluster provider health
	if err = ValidateClusterProviderHealth(liveCluster); err != nil {
		return err
	}

	return nil
}

// IsThisFreshInstall checks if its fresh install or not and returns true or false
func IsThisFreshInstall(clusterSpec *corev1.StorageCluster, timeout, interval time.Duration) (bool, error) {
	var isFreshInstall bool
	t := func() (interface{}, bool, error) {
		cluster, err := operatorops.Instance().GetStorageCluster(clusterSpec.Name, clusterSpec.Namespace)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get StorageCluster [%s] in [%s], Err: %v", clusterSpec.Name, cluster.Namespace, err)
		}
		return cluster, false, nil
	}

	out, err := task.DoRetryWithTimeout(t, timeout, interval)
	if err != nil {
		return isFreshInstall, fmt.Errorf("failed to determine if this is fresh install or not, Err: %v", err)
	}
	cluster := out.(*corev1.StorageCluster)

	// Check if PX install if fresh
	isFreshInstall = isThisFreshInstall(cluster)
	if !isFreshInstall {
		logrus.Debug("This is not a fresh PX installation!")
	}
	return isFreshInstall, nil
}

// IsThisFreshInstall checks whether it's a fresh Portworx install, copied this function her due to import cycle not allowed
func isThisFreshInstall(cluster *corev1.StorageCluster) bool {
	// To handle failures during fresh install e.g. validation failures,
	// extra check for px runtime states is added here to avoid unexpected behaviors
	return cluster.Status.Phase == "" ||
		cluster.Status.Phase == string(corev1.ClusterStateInit) ||
		(cluster.Status.Phase == string(corev1.ClusterStateDegraded) &&
			util.GetStorageClusterCondition(cluster, "Portworx", corev1.ClusterConditionTypeRuntimeState) == nil)
}

// ValidateClusterProviderHealth validates health of the cluster provider environment
func ValidateClusterProviderHealth(cluster *corev1.StorageCluster) error {
	// NOTE: For now only checking this for Openshift, will add for other providers in future when its needed
	if isOpenshift(cluster) {
		t := func() (interface{}, bool, error) {
			logrus.Debug("This is Openshift cluster, checking ClusterVersion object status..")
			clusterVersion, err := openshiftops.Instance().GetClusterVersion("version")
			if err != nil {
				return nil, false, fmt.Errorf("failed to get Openshift ClusterVersion object, Err: %v", err)
			}

			var listOfBadConditions []ocp_configv1.ClusterOperatorStatusCondition
			for _, condition := range clusterVersion.Status.Conditions {
				logrus.Debugf("ClusterVersions condition [%v=%v], message [%s]", condition.Type, condition.Status, condition.Message)
				if condition.Type == "Available" && condition.Status == ocp_configv1.ConditionFalse {
					listOfBadConditions = append(listOfBadConditions, condition)
				} else if condition.Type == "Degraded" && condition.Status == ocp_configv1.ConditionTrue {
					listOfBadConditions = append(listOfBadConditions, condition)
				} else if condition.Type == "Progressing" && condition.Status == ocp_configv1.ConditionTrue {
					listOfBadConditions = append(listOfBadConditions, condition)
				}
			}

			if len(listOfBadConditions) > 0 {
				logrus.Debugf("ClusterVersion object list of all bad conditions:\n%s", listOfBadConditions)
			}

			lastCondition := clusterVersion.Status.Conditions[len(clusterVersion.Status.Conditions)-1]
			logrus.Debugf("ClusterVersion object last known condition [%v=%v], message [%s]", lastCondition.Type, lastCondition.Status, lastCondition.Message)
			if strings.Contains(strings.ToLower(lastCondition.Message), "cluster version is") { // This type of message is most likely an indication that cluster is healthy
				logrus.Debugf("ClusterVersion object indicates that cluster is healthy, see latest status message [%s]", lastCondition.Message)
				return nil, false, nil
			}
			return nil, true, fmt.Errorf("clusterVersion object returned message that does not indicate it is healthy, see [%s]", lastCondition.Message)
		}
		if _, err := task.DoRetryWithTimeout(t, defaultOcpClusterCheckTimeout, defaultOcpClusterCheckInterval); err != nil {
			logrus.Warnf("Cluster does not seem to be healthy, Err: %v", err)
			return nil // Will not be returning any errors here, this is just for observation of condition statuses
		}
	}
	return nil
}

// GetOpenshiftVersion gets Openshift version from ClusterVersion object and returns it as a string
func GetOpenshiftVersion() (string, error) {
	logrus.Debug("Getting Openshift version from ClusterVersion object..")
	clusterVersion, err := openshiftops.Instance().GetClusterVersion("version")
	if err != nil {
		return "", fmt.Errorf("failed to get Openshift ClusterVersion object, Err: %v", err)
	}

	if clusterVersion.Status.Desired.Version == "" {
		return "", fmt.Errorf("ClusterVersion object returned empty Openshift version string")
	}
	logrus.Debugf("Got Openshift version [%s]", clusterVersion.Status.Desired.Version)
	return clusterVersion.Status.Desired.Version, nil
}

// IsOpenshiftCluster checks if its Openshift cluster by seeing if ClusterVersion resource exists
func IsOpenshiftCluster() bool {
	clusterVersionKind := "ClusterVersion"
	clusterVersionApiVersion := "config.openshift.io/v1"

	gvk := schema.GroupVersionKind{
		Kind:    clusterVersionKind,
		Version: clusterVersionApiVersion,
	}
	exists, err := coreops.Instance().ResourceExists(gvk)
	if err != nil {
		logrus.Error(err)
		return false
	}
	return exists
}

// ValidatePxPodsAreReadyOnGivenNodes takes list of node and validates PX pods are present and ready on these nodes
func ValidatePxPodsAreReadyOnGivenNodes(clusterSpec *corev1.StorageCluster, nodeList []v1.Node, timeout, interval time.Duration) error {
	labelSelector := map[string]string{"name": "portworx"}
	expectedPxPodCount := len(nodeList)

	t := func() (interface{}, bool, error) {
		var podsReady []string
		var podsNotReady []string

		// Get StorageCluster
		cluster, err := operatorops.Instance().GetStorageCluster(clusterSpec.Name, clusterSpec.Namespace)
		if err != nil {
			return "", true, err
		}

		for _, node := range nodeList {
			// Get PX pods from node
			podList, err := coreops.Instance().GetPodsByNodeAndLabels(node.Name, clusterSpec.Namespace, labelSelector)
			if err != nil {
				return nil, true, err
			}

			// Validate PX pod is found on node and only 1
			if len(podList.Items) == 0 {
				return nil, true, fmt.Errorf("didn't find any Portworx pods on node [%s]", node.Name)
			} else if len(podList.Items) > 1 {
				return nil, true, fmt.Errorf("found [%d] Portworx pods on node [%s], should only have 1 per node", len(podList.Items), node.Name)
			}
			pxPod := podList.Items[0]

			// Validate PX pod has right owner
			for _, owner := range pxPod.OwnerReferences {
				if owner.UID != cluster.UID {
					return nil, true, fmt.Errorf("failed to match pod owner reference UID [%s] to StorageCluster UID [%s]", owner.UID, cluster.UID)
				}
			}

			// Get list of ready and not ready PX pods
			logrus.Infof("Found Portworx pod [%s] on node [%s]", pxPod.Name, node.Name)
			if coreops.Instance().IsPodReady(pxPod) {
				logrus.Infof("Portworx pod [%s] is ready on node [%s]", pxPod.Name, node.Name)
				podsReady = append(podsReady, pxPod.Name)
			} else {
				logrus.Infof("Portworx pod [%s] is not ready on node [%s]", pxPod.Name, node.Name)
				podsNotReady = append(podsNotReady, pxPod.Name)
			}
		}

		// Validate count of PX pods equals to number of expected nodes
		if len(podsReady) == expectedPxPodCount {
			logrus.Infof("All Portworx pods are ready: %s", podsReady)
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("some Portworx pods are still not ready: %s. Expected ready pods: %d, Got: %d", podsNotReady, expectedPxPodCount, len(podsReady))
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

func validateStorageNodes(pxImageList map[string]string, cluster *corev1.StorageCluster, expectedNumberOfStorageNodes int, timeout, interval time.Duration) error {
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

		if readyNodes != expectedNumberOfStorageNodes {
			return nil, true, fmt.Errorf("waiting for all storagenodes to be ready: %d/%d", readyNodes, expectedNumberOfStorageNodes)
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
	/* Will find a better way to do this in PTX-18637
	// Validate cloudStorage
	if !reflect.DeepEqual(expected.Spec.CloudStorage, live.Spec.CloudStorage) {
		return fmt.Errorf("deployed CloudStorage spec doesn't match expected")
	}
	*/
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
	expectedPxNodeList []v1.Node,
	timeout, interval time.Duration,
	podTestFn podTestFnType,
) error {
	expectedPxNodeNameList := ConvertNodeListToNodeNameList(expectedPxNodeList)
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

// validateDmthinOnPxNodes greps for dmthin in the config.json on each PX pod
// and makes sure its there, if dmthin misc-args annotation is found
func validateDmthinOnPxNodes(cluster *corev1.StorageCluster) error {
	listOptions := map[string]string{"name": "portworx"}

	// Check if px-storev2 exists in config.json
	cmd := "grep -i px-storev2 /etc/pwx/config.json"
	if IsPKS(cluster) {
		cmd = "grep -i px-storev2 /var/vcap/store/etc/pwx/config.json"
	}

	miscArgAnnotation := cluster.Annotations["portworx.io/misc-args"]

	if !strings.Contains(strings.ToLower(miscArgAnnotation), "-t px-storev2") {
		logrus.Debugf("PX-StoreV2 is not enabled on PX cluster [%s]", cluster.Name)
		return nil
	}
	logrus.Infof("PX-StoreV2 is enabled on PX cluster [%s]", cluster.Name)

	logrus.Infof("Will check that storage is type PX-StoreV2 on all PX pods")
	t := func() (interface{}, bool, error) {
		// Get Portworx pods
		pxPods, err := coreops.Instance().GetPods(cluster.Namespace, listOptions)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get PX pods, Err: %v", err)
		}

		for _, pxPod := range pxPods.Items {
			dmthinEnabled, err := validateDmthinViaPodCmd(&pxPod, cmd, cluster.Namespace)
			if err != nil {
				return nil, true, err
			}
			if dmthinEnabled {
				continue
			}
			return nil, true, fmt.Errorf("PX-StoreV2 is not enabled on PX pod [%s]", pxPod.Name)
		}
		return nil, false, nil
	}

	_, err := task.DoRetryWithTimeout(t, defaultDmthinValidationTimeout, defaultDmthinValidationInterval)
	if err != nil {
		return err
	}

	logrus.Info("Validated PX-StoreV2 is enabled on all PX pods")
	return nil
}

// IsPKS returns true if the annotation has a PKS annotation and is true value
func IsPKS(cluster *corev1.StorageCluster) bool {
	enabled, err := strconv.ParseBool(cluster.Annotations[AnnotationIsPKS])
	return err == nil && enabled
}

// validateDmthinViaPodCmd runs command on PX pod and returns true if dmthin is enabled on that PX node
func validateDmthinViaPodCmd(pxPod *v1.Pod, cmd string, namespace string) (bool, error) {
	output, err := runCmdInsidePxPod(pxPod, cmd, namespace, false)
	if err != nil {
		return false, err
	}

	if len(output) > 0 {
		logrus.Infof("Validated PX-StoreV2 is enabled on pod [%s], output from the command [%s] is [%s]", pxPod.Name, cmd, strings.TrimSpace(strings.TrimSuffix(output, "\n")))
		return true, nil
	}
	return false, fmt.Errorf("failed to find [PX-StoreV2] in the putput from [%s]", cmd)
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

	// Do not expect Portworx pods to be deployed on infra nodes
	selectorRequirements = append(
		selectorRequirements,
		v1.NodeSelectorRequirement{
			Key:      "node-role.kubernetes.io/infra",
			Operator: v1.NodeSelectorOpDoesNotExist,
		},
	)

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

// GetExpectedPxNodeList will get the list of nodes that should be included
// in the given Portworx cluster, by seeing if each non-master node matches the given
// node selectors and affinities.
func GetExpectedPxNodeList(cluster *corev1.StorageCluster) ([]v1.Node, error) {
	var nodeListWithPxPods []v1.Node
	var runOnMaster bool

	nodeList, err := coreops.Instance().GetNodes()
	if err != nil {
		return nodeListWithPxPods, err
	}

	dummyPod := &v1.Pod{}
	if cluster.Spec.Placement != nil && cluster.Spec.Placement.NodeAffinity != nil {
		dummyPod.Spec.Affinity = &v1.Affinity{
			NodeAffinity: cluster.Spec.Placement.NodeAffinity.DeepCopy(),
		}
	} else {
		if IsK3sCluster() || IsPxDeployedOnMaster(cluster) {
			runOnMaster = true
		}

		dummyPod.Spec.Affinity = &v1.Affinity{
			NodeAffinity: defaultPxNodeAffinityRules(runOnMaster),
		}

		if runOnMaster {
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
		if coreops.Instance().IsNodeMaster(node) && !runOnMaster {
			continue
		}

		matches, err := affinityhelper.GetRequiredNodeAffinity(dummyPod).Match(&node)
		if err == nil && matches {
			nodeListWithPxPods = append(nodeListWithPxPods, node)
		}
	}

	return nodeListWithPxPods, nil
}

// IsPxDeployedOnMaster look for PX StorageCluster annotation that tells PX Operator wether to deploy PX on master or not and return true or false
func IsPxDeployedOnMaster(cluster *corev1.StorageCluster) bool {
	deployOnMaster, err := strconv.ParseBool(cluster.Annotations["portworx.io/run-on-master"])
	return err == nil && deployOnMaster
}

// ConvertNodeListToNodeNameList takes list of nodes and return list of node names
func ConvertNodeListToNodeNameList(nodeList []v1.Node) []string {
	var nodeNameList []string

	for _, node := range nodeList {
		nodeNameList = append(nodeNameList, node.Name)
	}
	return nodeNameList
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

func validateComponents(pxImageList map[string]string, originalClusterSpec, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	// Validate PVC Controller components and images
	if err := ValidatePvcController(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	// Validate Stork components and images
	if err := ValidateStork(pxImageList, originalClusterSpec, cluster, timeout, interval); err != nil {
		return err
	}

	// Validate Autopilot components and images
	if err := ValidateAutopilot(pxImageList, originalClusterSpec, cluster, timeout, interval); err != nil {
		return err
	}

	// Validate CSI components and images
	if err := ValidateCSI(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	// Validate Monitoring
	if err := ValidateMonitoring(pxImageList, originalClusterSpec, cluster, timeout, interval); err != nil {
		return err
	}

	// Validate PortworxProxy
	if err := ValidatePortworxProxy(cluster, timeout, interval); err != nil {
		return err
	}

	// Validate Security
	previouslyEnabled := false // NOTE: This is set to false by default as we are not expecting Security to be previously enabled here
	if err := ValidateSecurity(cluster, previouslyEnabled, timeout, interval); err != nil {
		return err
	}

	// Validate OCP Dynamic Plugin
	if err := ValidateOpenshiftDynamicPlugin(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	// Validate KVDB
	if err := ValidateKvdb(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidateOpenshiftDynamicPlugin validates OCP Dynamic Plugin components
func ValidateOpenshiftDynamicPlugin(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Openshift Dynamic Plugin components")
	logrus.Info("Check OCP and PX Operator versions to determine whether Openshift Dynamic Plugin should be deployed..")
	pxVersion := GetPortworxVersion(cluster)
	logrus.Infof("PX Version: [%s]", pxVersion.String())
	opVersion, err := GetPxOperatorVersion()
	if err != nil {
		return err
	}
	logrus.Infof("PX Operator version: [%s]", opVersion.String())

	// Get k8s version
	kbVer, err := GetK8SVersion()
	if err != nil {
		return err
	}
	k8sVersion, _ := version.NewVersion(kbVer)

	// Validate Dynamic plugin only if PX Operator 23.7.0+ (due to image inconsistency bug which is fixed in 23.7.0+) and OCP 4.12+ (k8s v1.25+)
	if opVersion.GreaterThanOrEqual(opVer23_7) && isOpenshift(cluster) && k8sVersion.GreaterThanOrEqual(minK8sVersionForDynamicPlugin) && isPluginSupported() {
		logrus.Info("Openshift Dynamic Plugin should be deployed")
		if err := ValidateOpenshiftDynamicPluginEnabled(pxImageList, cluster, timeout, interval); err != nil {
			return fmt.Errorf("failed to validate Openshift Dynamic Plugin components, Err: %v", err)
		}
		logrus.Info("Successfully validated all Openshift Dynamic Plugin components")
		return nil
	}

	logrus.Info("Openshift Dynamic Plugin is not supported, will skip validation")
	return nil
}

// isPluginSupported checks if plugin resource exists and returns true or false
func isPluginSupported() bool {
	gvk := schema.GroupVersionKind{
		Kind:    clusterOperatorKind,
		Version: clusterOperatorVersion,
	}
	exists, err := coreops.Instance().ResourceExists(gvk)
	if err != nil {
		logrus.Error(err)
		return false
	}
	return exists
}

// ValidateOpenshiftDynamicPluginEnabled validates that all Openshift Dynamic Plugin components are enabled/created
func ValidateOpenshiftDynamicPluginEnabled(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	t := func() (interface{}, bool, error) {
		// Validate px-plugin Deployment and pods
		pxPluginDeployment, err := appops.Instance().GetDeployment("px-plugin", cluster.Namespace)
		if err != nil {
			return nil, true, err
		}

		if err := validateDeployment(pxPluginDeployment, timeout, interval); err != nil {
			return nil, true, err
		}

		pxPluginPods, err := appops.Instance().GetDeploymentPods(pxPluginDeployment)
		if err != nil {
			return nil, true, err
		}

		// Validate image inside px-plugin pods
		if image, ok := pxImageList["dynamicPlugin"]; ok {
			if err := validateContainerImageInsidePods(cluster, image, "px-plugin", &v1.PodList{Items: pxPluginPods}); err != nil {
				return nil, true, err
			}
		} else {
			logrus.Warn("Did not find [dynamicPlugin] in the list of images in the version manifest, skipping image validation")
		}

		// Validate px-plugin-proxy Deployment and pods
		pxPluginProxyDeployment, err := appops.Instance().GetDeployment("px-plugin-proxy", cluster.Namespace)
		if err != nil {
			return nil, true, err
		}

		if err := validateDeployment(pxPluginProxyDeployment, timeout, interval); err != nil {
			return nil, true, err
		}

		pxPluginProxyPods, err := appops.Instance().GetDeploymentPods(pxPluginProxyDeployment)
		if err != nil {
			return nil, true, err
		}

		// Validate image inside px-plugin-proxy pods
		if image, ok := pxImageList["dynamicPluginProxy"]; ok {
			if err := validateContainerImageInsidePods(cluster, image, "nginx", &v1.PodList{Items: pxPluginProxyPods}); err != nil {
				return nil, true, err
			}
		} else {
			logrus.Warn("Did not find [dynamicPluginProxy] in the list of images in the version manifest, skipping image validation")
		}

		// Validate px-plugin Service
		_, err = coreops.Instance().GetService("px-plugin", cluster.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate Service [px-plugin], Err: %v", err)
		}

		// Validate px-plugin-proxy Service
		_, err = coreops.Instance().GetService("px-plugin-proxy", cluster.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate Service [px-plugin-proxy], Err: %v", err)
		}

		// Validate px-plugin ConfigMap
		_, err = coreops.Instance().GetConfigMap("px-plugin", cluster.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate Configmap [px-plugin], Err: %v", err)
		}

		// Validate px-plugin-proxy-conf ConfigMap
		_, err = coreops.Instance().GetConfigMap("px-plugin-proxy-conf", cluster.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate Configmap [px-plugin-proxy-conf], Err: %v", err)
		}

		// Validate px-plugin-cer Secret
		_, err = coreops.Instance().GetSecret("px-plugin-cert", cluster.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate Secret [px-plugin-cert], Err: %v", err)
		}

		// Validate px-plugin-proxy Secret
		_, err = coreops.Instance().GetSecret("px-plugin-proxy", cluster.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate Secret [px-plugin-proxy], Err: %v", err)
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidateKvdb validates Portworx KVDB components
func ValidateKvdb(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Internal KVDB components")
	if cluster.Spec.Kvdb.Internal {
		logrus.Debug("Internal KVDB is enabled in StorageCluster")
		return ValidateInternalKvdbEnabled(pxImageList, cluster, timeout, interval)
	}
	logrus.Debug("Internal KVDB is disabled in StorageCluster")
	return ValidateInternalKvdbDisabled(cluster, timeout, interval)
}

// ValidateInternalKvdbEnabled validates that all Internal KVDB components are enabled/created
func ValidateInternalKvdbEnabled(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
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

		// Figure out what default registry to use for kvdb image, based on PX Operator version
		kvdbImageName := "k8s.gcr.io/pause"
		opVersion, _ := GetPxOperatorVersion()
		if opVersion.GreaterThanOrEqual(opVer23_3) {
			kvdbImageName = "registry.k8s.io/pause"
		}

		// Check if kvdb image was explicitly set in the px-version configmap
		explicitKvdbImage := ""
		if value, ok := pxImageList["pause"]; ok {
			explicitKvdbImage = value
		}

		if len(explicitKvdbImage) > 0 {
			if err := validateContainerImageInsidePods(cluster, explicitKvdbImage, "portworx-kvdb", podList); err != nil {
				return nil, true, err
			}
		} else {
			if err := validateContainerImageInsidePods(cluster, fmt.Sprintf("%s:3.1", kvdbImageName), "portworx-kvdb", podList); err != nil {
				return nil, true, err
			}
		}

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

	logrus.Debug("Internal KVDB components are enabled")
	return nil
}

// ValidateInternalKvdbDisabled validates that all Internal KVDB components are disabled/deleted
func ValidateInternalKvdbDisabled(cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Debug("Validate Internal KVDB components are disabled")

	t := func() (interface{}, bool, error) {
		// Validate KVDB pods
		listOptions := map[string]string{"kvdb": "true"}
		podList, err := coreops.Instance().GetPods(cluster.Namespace, listOptions)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get pods, Err: %v", err)
		}

		if len(podList.Items) != 0 {
			podNames := []string{}
			for _, pod := range podList.Items {
				podNames = append(podNames, pod.Name)
			}
			return nil, true, fmt.Errorf("found unexpected KVDB pod(s) %s", podNames)
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

	logrus.Debug("Internal KVDB components are disabled")
	return nil
}

// ValidatePvcController validates PVC Controller components and images
func ValidatePvcController(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate PVC Controller components")

	pvcControllerDp := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "portworx-pvc-controller",
			Namespace: cluster.Namespace,
		},
	}

	// Check if PVC Controller is enabled or disabled
	if isPVCControllerEnabled(cluster) {
		return ValidatePvcControllerEnabled(pvcControllerDp, cluster, timeout, interval)
	}
	return ValidatePvcControllerDisabled(pvcControllerDp, timeout, interval)
}

// ValidatePvcControllerEnabled validates that all PVC Controller components are enabled/created
func ValidatePvcControllerEnabled(pvcControllerDp *appsv1.Deployment, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("PVC Controller should be enabled")

	k8sVersion, err := GetK8SVersion()
	if err != nil {
		return err
	}

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
func ValidateStork(pxImageList map[string]string, originalClusterSpec, liveCluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Stork components")

	storkDp := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stork",
			Namespace: liveCluster.Namespace,
		},
	}

	storkSchedulerDp := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stork-scheduler",
			Namespace: liveCluster.Namespace,
		},
	}

	if liveCluster.Spec.Stork != nil && liveCluster.Spec.Stork.Enabled {
		logrus.Debug("Stork is enabled in StorageCluster")
		return ValidateStorkEnabled(pxImageList, originalClusterSpec, liveCluster, storkDp, storkSchedulerDp, timeout, interval)
	}
	logrus.Debug("Stork is disabled in StorageCluster")
	return ValidateStorkDisabled(liveCluster, storkDp, storkSchedulerDp, timeout, interval)
}

// ValidateStorkEnabled validates that all Stork components are enabled/created
func ValidateStorkEnabled(pxImageList map[string]string, originalClusterSpec, cluster *corev1.StorageCluster, storkDp, storkSchedulerDp *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Info("Validate Stork components are enabled")
	var storkImage string

	// See if original spec had stork image specified
	if originalClusterSpec.Spec.Stork != nil && originalClusterSpec.Spec.Stork.Image != "" {
		storkImage = originalClusterSpec.Spec.Stork.Image
	}

	t := func() (interface{}, bool, error) {
		if err := validateDeployment(storkDp, timeout, interval); err != nil {
			return nil, true, err
		}

		// If no Stork image was specified in both original and live StorageCluster specs, then take image from the versions URL
		if cluster.Spec.Stork.Image == "" && storkImage == "" {
			if value, ok := pxImageList["stork"]; ok {
				storkImage = value
			} else {
				return nil, true, fmt.Errorf("failed to find image for Stork")
			}
			logrus.Debugf("Using Stork image from PX endpoint version list [%s]", storkImage)
		} else {
			logrus.Debugf("Custom Stork image was specified in the spec [%s], custom image in the live spec [%s]", storkImage, cluster.Spec.Stork.Image)
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

		// Validate stork-scheduler deployment and image
		if err := ValidateStorkScheduler(pxImageList, cluster, storkSchedulerDp, timeout, interval); err != nil {
			return nil, true, err
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidateStorkScheduler validates stork-scheduler deployment and container images inside pods
func ValidateStorkScheduler(pxImageList map[string]string, cluster *corev1.StorageCluster, storkSchedulerDp *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Info("Validate stork-scheduler deployment and image")

	// Validate stork-scheduler deployment and pods
	if err := validateDeployment(storkSchedulerDp, timeout, interval); err != nil {
		return err
	}

	// Get stork-scheduler pods
	pods, err := coreops.Instance().GetPods(cluster.Namespace, map[string]string{"name": "stork-scheduler"})
	if err != nil {
		return err
	}

	K8sVer1_22, _ := version.NewVersion("1.22")
	k8sMinVersionForKubeSchedulerConfiguration, _ := version.NewVersion("1.23")
	kubeVersion, _, err := GetFullVersion()
	if err != nil {
		return err
	}

	// Figure out what default registry to use for stork-scheduler image, based on PX Operator version
	storkSchedulerImageName := "k8s.gcr.io/kube-scheduler-amd64"
	opVersion, _ := GetPxOperatorVersion()
	if opVersion.GreaterThanOrEqual(opVer23_3) {
		storkSchedulerImageName = "registry.k8s.io/kube-scheduler-amd64"
	}

	// Check if stork-scheduler image was explicitly set in the px-version configmap
	explicitStorkSchedulerImage := ""
	if value, ok := pxImageList["kubeScheduler"]; ok {
		explicitStorkSchedulerImage = value
	}

	if len(explicitStorkSchedulerImage) > 0 {
		logrus.Debugf("Image for stork-scheduler was explicitly set in the px-version configmap to [%s]", explicitStorkSchedulerImage)
		// Use this image as is, since it was explicitly set
		if err := validateContainerImageInsidePods(cluster, explicitStorkSchedulerImage, "stork-scheduler", pods); err != nil {
			return err
		}
	} else {
		if opVersion.LessThan(minOpVersionForKubeSchedConfig) {
			if kubeVersion != nil && kubeVersion.GreaterThanOrEqual(K8sVer1_22) {
				// This image should have v1.21.4 tag
				if err := validateContainerImageInsidePods(cluster, fmt.Sprintf("%s:v1.21.4", storkSchedulerImageName), "stork-scheduler", pods); err != nil {
					return err
				}
			} else {
				// This image should have k8s version tag
				if err := validateContainerImageInsidePods(cluster, fmt.Sprintf("%s:v%s", storkSchedulerImageName, kubeVersion.String()), "stork-scheduler", pods); err != nil {
					return err
				}
			}
		} else {
			if kubeVersion != nil && kubeVersion.GreaterThanOrEqual(K8sVer1_22) && kubeVersion.LessThan(k8sMinVersionForKubeSchedulerConfiguration) {
				// This image should have v1.21.4 tag
				if err := validateContainerImageInsidePods(cluster, fmt.Sprintf("%s:v1.21.4", storkSchedulerImageName), "stork-scheduler", pods); err != nil {
					return err
				}
			} else {
				// This image should have k8s version tag
				if err := validateContainerImageInsidePods(cluster, fmt.Sprintf("%s:v%s", storkSchedulerImageName, kubeVersion.String()), "stork-scheduler", pods); err != nil {
					return err
				}
			}
		}
	}

	// Validate stork-scheduler deployment pod topology spread constraints
	if err := validatePodTopologySpreadConstraints(storkSchedulerDp, timeout, interval); err != nil {
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
func ValidateAutopilot(pxImageList map[string]string, originalClusterSpec, liveCluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Autopilot components")

	autopilotDp := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "autopilot",
			Namespace: liveCluster.Namespace,
		},
	}

	if liveCluster.Spec.Autopilot != nil && liveCluster.Spec.Autopilot.Enabled {
		logrus.Debug("Autopilot is Enabled in StorageCluster")
		return ValidateAutopilotEnabled(pxImageList, originalClusterSpec, liveCluster, autopilotDp, timeout, interval)
	}
	logrus.Debug("Autopilot is Disabled in StorageCluster")
	return ValidateAutopilotDisabled(liveCluster, autopilotDp, timeout, interval)
}

// ValidateAutopilotEnabled validates that all Autopilot components are enabled/created
func ValidateAutopilotEnabled(pxImageList map[string]string, originalClusterSpec, cluster *corev1.StorageCluster, autopilotDp *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Info("Validate Autopilot components are enabled")
	var autopilotImage string

	// See if original spec had Autopilot image specified
	if originalClusterSpec.Spec.Autopilot != nil && originalClusterSpec.Spec.Autopilot.Image != "" {
		autopilotImage = originalClusterSpec.Spec.Autopilot.Image
	}

	t := func() (interface{}, bool, error) {
		// Validate autopilot deployment and pods
		if err := validateDeployment(autopilotDp, timeout, interval); err != nil {
			return nil, true, err
		}

		// If no Autopilot image was specified in both original and live StorageCluster specs, then take image from the versions URL
		if cluster.Spec.Autopilot.Image == "" && autopilotImage == "" {
			if value, ok := pxImageList[autopilotDp.Name]; ok {
				autopilotImage = value
			} else {
				return nil, true, fmt.Errorf("failed to find image for %s", autopilotDp.Name)
			}
			logrus.Debugf("Using Autopilot image from PX endpoint versions list [%s]", autopilotImage)
		} else {
			logrus.Debugf("Custom Autopilot image was specified in the spec [%s], custom image in the live spec [%s]", autopilotImage, cluster.Spec.Autopilot.Image)
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
			return nil, true, fmt.Errorf("failed to validate ClusterRole [%s], Err: %v", autopilotDp.Name, err)
		}

		// Validate Autopilot ClusterRoleBinding
		_, err = rbacops.Instance().GetClusterRoleBinding(autopilotDp.Name)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ClusterRoleBinding [%s], Err: %v", autopilotDp.Name, err)
		}

		// Validate Autopilot ConfigMap
		_, err = coreops.Instance().GetConfigMap("autopilot-config", autopilotDp.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ConfigMap [autopilot-config], Err: %v", err)
		}

		// Validate Autopilot ServiceAccount
		_, err = coreops.Instance().GetServiceAccount(autopilotDp.Name, autopilotDp.Namespace)
		if errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ServiceAccount [%s], Err: %v", autopilotDp.Name, err)
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
			return nil, true, fmt.Errorf("failed to validate ClusterRole [%s], is found when shouldn't be", autopilotDp.Name)
		}

		// Validate Autopilot ClusterRoleBinding doesn't exist
		_, err = rbacops.Instance().GetClusterRoleBinding(autopilotDp.Name)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ClusterRoleBinding [%s], is found when shouldn't be", autopilotDp.Name)
		}

		// Validate Autopilot ConfigMap doesn't exist
		_, err = coreops.Instance().GetConfigMap("autopilot-config", autopilotDp.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ConfigMap [autopilot-config], is found when shouldn't be")
		}

		// Validate Autopilot ServiceAccount doesn't exist
		_, err = coreops.Instance().GetServiceAccount(autopilotDp.Name, autopilotDp.Namespace)
		if !errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("failed to validate ServiceAccount [%s], is found when shouldn't be", autopilotDp.Name)
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
	pxVersion, _ := version.NewVersion(getPxVersion(pxImageList, cluster))
	if cluster.Spec.CSI.Enabled {
		logrus.Debug("CSI is enabled in StorageCluster")
		return ValidateCsiEnabled(pxImageList, cluster, pxCsiDp, timeout, interval, pxVersion)
	}
	logrus.Debug("CSI is disabled in StorageCluster")
	return ValidateCsiDisabled(cluster, pxCsiDp, timeout, interval, pxVersion)
}

// ValidateCsiEnabled validates that all CSI components are enabled/created
func ValidateCsiEnabled(pxImageList map[string]string, cluster *corev1.StorageCluster, pxCsiDp *appsv1.Deployment, timeout, interval time.Duration, pxVersion *version.Version) error {
	logrus.Info("Validate CSI components are enabled")

	t := func() (interface{}, bool, error) {
		logrus.Debug("CSI is enabled in StorageCluster")

		// After operator version 23.10.3, CSI node registrar container is inside portworx-api pods
		if opVersion, _ := GetPxOperatorVersion(); pxVersion.GreaterThanOrEqual(pxVer2_13) && opVersion.GreaterThanOrEqual(OpVer23_10_3) {
			if err := validateCsiContainerInPxApiPods(cluster.Namespace, true, timeout, interval); err != nil {
				return nil, true, err
			}
		} else {
			if err := validateCsiContainerInPxPods(cluster.Namespace, true, timeout, interval); err != nil {
				return nil, true, err
			}
		}

		// Validate CSI container image inside Portworx OCI Monitor pods
		var csiNodeDriverRegistrarImage string
		if value, ok := pxImageList["csiNodeDriverRegistrar"]; ok {
			csiNodeDriverRegistrarImage = value
		} else {
			return nil, true, fmt.Errorf("failed to find image for csiNodeDriverRegistrar")
		}

		var pods *v1.PodList
		var err error

		if opVersion, _ := GetPxOperatorVersion(); pxVersion.GreaterThanOrEqual(pxVer2_13) && opVersion.GreaterThanOrEqual(OpVer23_10_3) {
			pods, err = coreops.Instance().GetPods(cluster.Namespace, map[string]string{"name": "portworx-api"})
			if err != nil {
				return nil, true, err
			}
		} else {
			pods, err = coreops.Instance().GetPods(cluster.Namespace, map[string]string{"name": "portworx"})
			if err != nil {
				return nil, true, err
			}
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

		// Validate CSI snapshot controller
		if err := validateCSISnapshotController(cluster, pxImageList, timeout, interval); err != nil {
			return nil, true, err
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidateCsiDisabled validates that all CSI components are disabled/deleted
func ValidateCsiDisabled(cluster *corev1.StorageCluster, pxCsiDp *appsv1.Deployment, timeout, interval time.Duration, pxVersion *version.Version) error {
	logrus.Info("Validate CSI components are disabled")

	t := func() (interface{}, bool, error) {
		logrus.Debug("CSI is disabled in StorageCluster")
		if opVersion, _ := GetPxOperatorVersion(); pxVersion.GreaterThanOrEqual(pxVer2_13) && opVersion.GreaterThanOrEqual(OpVer23_10_3) {
			if err := validateCsiContainerInPxApiPods(cluster.Namespace, false, timeout, interval); err != nil {
				return nil, true, err
			}
		} else {
			if err := validateCsiContainerInPxPods(cluster.Namespace, false, timeout, interval); err != nil {
				return nil, true, err
			}
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

func validateCsiContainerInPxApiPods(namespace string, csi bool, timeout, interval time.Duration) error {
	logrus.Debug("Validating CSI container inside Portworx Api pods")
	listOptions := map[string]string{"name": "portworx-api"}

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
				return nil, true, fmt.Errorf("failed to validate CSI containers in PX Api pods [portworx-api]: expected %d, got %d, %d/%d Ready pods", len(pods.Items), len(pxPodsWithCsiContainer), podsReady, len(pods.Items))
			}
		} else {
			if len(pxPodsWithCsiContainer) > 0 || len(pods.Items) != podsReady {
				return nil, true, fmt.Errorf("failed to validate CSI container in PX Api pods [portworx-api]: expected: 0, got %d, %d/%d Ready pods", len(pxPodsWithCsiContainer), podsReady, len(pods.Items))
			}
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
				return nil, true, fmt.Errorf("failed to validate CSI containers in PX pods [portworx]: expected %d, got %d, %d/%d Ready pods", len(pods.Items), len(pxPodsWithCsiContainer), podsReady, len(pods.Items))
			}
		} else {
			if len(pxPodsWithCsiContainer) > 0 || len(pods.Items) != podsReady {
				return nil, true, fmt.Errorf("failed to validate CSI container in PX pods: expected [portworx]: 0, got %d, %d/%d Ready pods", len(pxPodsWithCsiContainer), podsReady, len(pods.Items))
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
		// Check whether snapshot controller container should be installed
		installSnapshotController := true
		podList, err := coreops.Instance().ListPods(nil)
		if err != nil {
			return nil, true, fmt.Errorf("failed to list pods from all namespaces")
		}
		for _, p := range podList.Items {
			// ignore pods deployed by operator
			if label, ok := p.Labels["app"]; ok && label == "px-csi-driver" {
				continue
			}
			for _, c := range p.Spec.Containers {
				if strings.Contains(c.Image, "/snapshot-controller:") {
					logrus.Infof("found external snapshot controller in pod %s/%s", p.Namespace, p.Name)
					installSnapshotController = false
					break
				}
			}
		}

		existingDeployment, err := appops.Instance().GetDeployment(deployment.Name, deployment.Namespace)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get deployment %s/%s", deployment.Namespace, deployment.Name)
		}
		pods, err := appops.Instance().GetDeploymentPods(existingDeployment)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get pods of deployment %s/%s", deployment.Namespace, deployment.Name)
		}
		if cluster.Spec.CSI.InstallSnapshotController != nil && *cluster.Spec.CSI.InstallSnapshotController && installSnapshotController {
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
	logrus.Infof("Validating image for [%s] container inside pod(s)", containerName)

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
					logrus.Infof("Image inside %s[%s] matches, expected: [%s], actual: [%s]", pod.Name, containerName, expectedImage, foundImage)
					foundContainer = true
					break
				} else if strings.Contains(foundImage, expectedImage) {
					logrus.Infof("Image inside %s[%s] matches, expected: [%s], actual: [%s]", pod.Name, containerName, expectedImage, foundImage)
					foundContainer = true
					break
				} else {
					return fmt.Errorf("failed to match container %s[%s] image, expected: [%s], actual: [%s]",
						pod.Name, containerName, expectedImage, foundImage)
				}
			}
		}

		if !foundContainer {
			return fmt.Errorf("failed to match container %s[%s] image, expected: [%s], actual: [%s]",
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
func ValidateMonitoring(pxImageList map[string]string, originalClusterSpec, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	if err := ValidatePrometheus(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	// Increasing timeout for Telemetry components as they take quite long time to initialize
	defaultTelemetryRetryInterval := 30 * time.Second
	defaultTelemetryTimeout := 30 * time.Minute
	if err := ValidateTelemetry(pxImageList, originalClusterSpec, cluster, defaultTelemetryTimeout, defaultTelemetryRetryInterval); err != nil {
		return err
	}

	if err := ValidateAlertManager(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	if err := ValidateGrafana(pxImageList, cluster); err != nil {
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

func ValidateGrafana(pxImageList map[string]string, cluster *corev1.StorageCluster) error {
	opVersion, err := GetPxOperatorVersion()
	if err != nil {
		return err
	}
	if opVersion.LessThan(opVer23_8) {
		logrus.Infof("Skipping grafana validation for operation version: [%s]", opVersion.String())
		return nil
	}

	shouldBeInstalled := cluster.Spec.Monitoring != nil &&
		cluster.Spec.Monitoring.Grafana != nil && cluster.Spec.Monitoring.Grafana.Enabled &&
		cluster.Spec.Monitoring.Prometheus != nil && cluster.Spec.Monitoring.Prometheus.Enabled
	err = ValidateGrafanaDeployment(cluster, shouldBeInstalled, pxImageList)
	if err != nil {
		return err
	}
	err = ValidateGrafanaService(cluster, shouldBeInstalled)
	if err != nil {
		return err
	}
	err = ValidateGrafanaConfigmaps(cluster, shouldBeInstalled)
	if err != nil {
		return err
	}

	return nil
}

func ValidateGrafanaDeployment(cluster *corev1.StorageCluster, shouldBeInstalled bool, pxImageList map[string]string) error {

	// Deployment to validate
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "px-grafana",
			Namespace: cluster.Namespace,
		},
	}
	if shouldBeInstalled {
		if err := appops.Instance().ValidateDeployment(deployment, 2*time.Minute, 10*time.Second); err != nil {
			return fmt.Errorf("failed to validate deployment %s/%s. should be installed: %v. err: %v",
				deployment.Namespace, deployment.Name, shouldBeInstalled, err)
		}
	} else {
		if err := appops.Instance().ValidateTerminatedDeployment(deployment, 2*time.Minute, 10*time.Second); err != nil {
			return fmt.Errorf("failed to validate terminated deployment %s/%s. should be installed: %v. err: %v",
				deployment.Namespace, deployment.Name, shouldBeInstalled, err)
		}
	}

	return nil
}

func ValidateGrafanaService(cluster *corev1.StorageCluster, shouldBeInstalled bool) error {
	svcs, err := coreops.Instance().ListServices(cluster.Namespace, metav1.ListOptions{
		LabelSelector: "app=grafana",
	})
	if err != nil {
		return err
	}

	if shouldBeInstalled {
		if len(svcs.Items) > 0 && svcs.Items[0].Spec.Ports[0].Port == 3000 {
			return nil
		} else {
			return fmt.Errorf("grafana is not installed when it should be")
		}
	} else {
		if len(svcs.Items) < 1 {
			return nil
		} else {
			return fmt.Errorf("grafana svc is installed when it is expected to be uninstalled")
		}
	}
}

func ValidateGrafanaConfigmaps(cluster *corev1.StorageCluster, shouldBeInstalled bool) error {
	cms, err := coreops.Instance().ListConfigMap(cluster.Namespace, metav1.ListOptions{})
	if err != nil {
		return err
	}

	var grafanaConfigmaps []v1.ConfigMap
	for _, cm := range cms.Items {
		if strings.Contains(cm.Name, "px-grafana-") {
			grafanaConfigmaps = append(grafanaConfigmaps, cm)
		}
	}

	if shouldBeInstalled {
		if len(grafanaConfigmaps) == 3 {
			return nil
		} else {
			return fmt.Errorf("grafana is not installed when it should be")
		}
	} else {
		if len(grafanaConfigmaps) < 3 {
			return nil
		} else {
			return fmt.Errorf("grafana configmaps are installed when it is expected to be uninstalled")
		}
	}
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
func ValidateTelemetry(pxImageList map[string]string, originalClusterSpec, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Telemetry components")
	logrus.Info("Check PX and PX Operator versions to determine which telemetry to validate against..")
	pxVersion := GetPortworxVersion(cluster)
	logrus.Infof("PX Version: [%s]", pxVersion.String())
	opVersion, err := GetPxOperatorVersion()
	if err != nil {
		return err
	}
	logrus.Infof("PX Operator version: [%s]", opVersion.String())

	if pxVersion.GreaterThanOrEqual(pxVer3_0) && opVersion.LessThan(opVer23_5_1) {
		logrus.Warnf("Skipping Telemetry validation as it is not support for PX Operator version less than [23.5.1] and PX version [3.0.0] or greater")
		return nil
	}

	if pxVersion.GreaterThanOrEqual(minimumPxVersionCCMGO) && opVersion.GreaterThanOrEqual(opVer1_10) {
		return ValidateTelemetryV2(pxImageList, originalClusterSpec, cluster, timeout, interval)
	}
	return ValidateTelemetryV1(pxImageList, originalClusterSpec, cluster, timeout, interval)
}

// ValidateTelemetryV1 validates old version of ccm-java telemetry
func ValidateTelemetryV1(pxImageList map[string]string, originalClusterSpec, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validating Telemetry (ccm-java)")
	if shouldTelemetryBeEnabled(originalClusterSpec, cluster) {
		if err := ValidateTelemetryV1Enabled(pxImageList, cluster, timeout, interval); err != nil {
			return fmt.Errorf("failed to validate Telemetry enabled, Err: %v", err)
		}
		return nil
	}

	if err := ValidateTelemetryV1Disabled(cluster, timeout, interval); err != nil {
		return fmt.Errorf("failed to validate Telemetry disabled, Err: %v", err)
	}
	return nil
}

// ValidateTelemetryV2 validates new version of ccm-go telemetry
func ValidateTelemetryV2(pxImageList map[string]string, originalClusterSpec, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validating Telemetry (ccm-go)")
	if shouldTelemetryBeEnabled(originalClusterSpec, cluster) {
		if err := ValidateTelemetryV2Enabled(pxImageList, cluster, timeout, interval); err != nil {
			return fmt.Errorf("failed to validate Telemetry enabled, Err: %v", err)
		}
		return nil
	}

	if err := ValidateTelemetryV2Disabled(cluster, timeout, interval); err != nil {
		return fmt.Errorf("failed to validate Telemetry disabled, Err: %v", err)
	}
	return nil
}

// shouldTelemetryBeEnabled validates if Telemetry should be auto enabled/disabled by default
func shouldTelemetryBeEnabled(originalClusterSpec, cluster *corev1.StorageCluster) bool {
	logrus.Info("Checking if Telemetry should be enabled or disabled")
	var shouldTelemetryBeEnabled bool
	var telemetryEnabledInTheSpec bool

	logrus.Info("Check PX and PX Operator versions to determine which Telemetry version to validate against..")
	pxVersion := GetPortworxVersion(cluster)
	logrus.Infof("PX Version: [%s]", pxVersion.String())
	opVersion, _ := GetPxOperatorVersion()
	logrus.Infof("PX Operator version: [%s]", opVersion.String())

	// Check if Telemetry is enabled or disabled in the original spec
	if originalClusterSpec.Spec.Monitoring != nil && originalClusterSpec.Spec.Monitoring.Telemetry != nil {
		if originalClusterSpec.Spec.Monitoring.Telemetry.Enabled {
			logrus.Debug("Telemetry is explicitly enabled in StorageCluster spec")
			telemetryEnabledInTheSpec = true
		} else {
			logrus.Debug("Telemetry is explicitly disabled in StorageCluster spec")
			telemetryEnabledInTheSpec = false
		}
	}

	// Get PX PROXY env vars from StorageCluster, if any
	proxyType, proxy := GetPxProxyEnvVarValue(cluster)

	// Validate conditions for when we are expecting Telemetry to be enabled/disabled
	if cluster.Spec.Monitoring != nil && cluster.Spec.Monitoring.Telemetry != nil {
		liveTelemetryEnabled := cluster.Spec.Monitoring.Telemetry.Enabled

		// If Telemetry is disabled in both live and original spec, expect it to be disabled
		if !telemetryEnabledInTheSpec && !liveTelemetryEnabled {
			logrus.Debug("Telemetry is explicitly disabled in live StorageCluster and original spec")
			return false
		}

		// If Telemetry is enabled in both live and original spec, expect it to be enabled
		if telemetryEnabledInTheSpec && liveTelemetryEnabled {
			logrus.Debug("Telemetry is explicitly enabled in live StorageCluster and original spec")
			shouldTelemetryBeEnabled = true
		}

		// If Telemetry is enabled in the original spec and disabled in the live spec, but the PX PROXY is present, expect it to be enabled
		if telemetryEnabledInTheSpec && len(proxy) > 0 && !liveTelemetryEnabled {
			logrus.Warnf("Telemetry is explicitly enabled in the original spec, but it seems to be disabled in live StorageCluster, expecting it to be enabled and working as PROXY was provided [%s]", proxy)
			shouldTelemetryBeEnabled = true
		}
	}

	if pxVersion.LessThan(minimumPxVersionCCMJAVA) {
		// PX version is lower than 2.8
		logrus.Warnf("Telemetry is not supported on Portworx version: [%s]", pxVersion.String())
		return false
	} else if !IsCCMGoSupported(pxVersion) {
		// CCM Java case, PX version is between 2.8 and 2.12, we do not enabled Telemetry by default here, unless its already enabled in the spec
		logrus.Warnf("Telemetry is Java based on Portworx version: [%s]", pxVersion.String())
		if shouldTelemetryBeEnabled {
			logrus.Infof("Telemetry should be enabled")
			return true
		}
		logrus.Infof("Telemetry should be disabled")
		return false
	} else if proxyType == EnvKeyPortworxHTTPProxy || proxyType == EnvKeyPortworxHTTPSProxy {
		if proxyType == EnvKeyPortworxHTTPSProxy && opVersion.LessThan(opVer23_7) {
			logrus.Warnf("Found [%s] env var. HTTPS proxy is only supported starting in PX Operator [23.7.0], Current PX Operator version is [%s]", EnvKeyPortworxHTTPSProxy, opVersion.String())
			logrus.Infof("Telemetry should be disabled")
			return false
		}
		// CCM Go is supported, but HTTP/HTTPS proxy cannot be split into host and port
		if _, _, _, proxyFormatErr := ParsePxProxyURL(proxy); proxyFormatErr != nil {
			logrus.Warnf("Telemetry is not supported with proxy in a format of: [%s] and should not be enabled", proxy)
			return false
		}
	}

	// Telemetry CCM Go is supported, at this point telemetry is enabled or can be enabled potentially
	// If the secret exists, means telemetry was enabled before and registered already, enable telemetry directly
	// If the telemetry secret doesn't exist, check if Arcus is reachable first before enabling telemetry:
	// * If Arcus is not reachable, registration will fail anyway, or it's an air-gapped cluster, disable telemetry
	// * If Arcus is reachable, enable telemetry by default
	secret, err := coreops.Instance().GetSecret(TelemetryCertName, cluster.Namespace)
	if err == nil {
		logrus.Debugf("Found Telemetry secret [%s] in [%s] namespace, Telemetry was previously enabled", secret.Name, secret.Namespace)
	}

	// If telemetry secret is not created yet, set telemetry as disabled if the registration endpoint is not reachable
	// Only ping Arcus when telemetry secret is not found, otherwise the cluster was already registered before
	if errors.IsNotFound(err) {
		logrus.Debugf("Telemetry secret [%s] was not found, will try to reach to Pure1 to see if Telemetry should be auto enabled by default", TelemetryCertName)
		if canAccess := CanAccessArcusRegisterEndpoint(cluster, proxy); !canAccess {
			if shouldTelemetryBeEnabled {
				logrus.Warnf("Not able to reach Pure1, but it should have been able to reach it due to PX PROXY was passed, please check your PROXY server")
				return true
			}
			logrus.Warnf("Telemetry be disabled due to cannot reach to Pure1")
			return false
		}
		logrus.Debug("Able to reach to Pure1")
	}

	logrus.Infof("Telemetry should be enabled")
	return true
}

// IsCCMGoSupported returns true if px version is higher than 2.12
func IsCCMGoSupported(pxVersion *version.Version) bool {
	return pxVersion.GreaterThanOrEqual(minimumPxVersionCCMGO)
}

// ParsePxProxy trims protocol prefix then splits the proxy address of the form "host:port" with possible basic authentication credential
func ParsePxProxyURL(proxy string) (string, string, string, error) {
	var authHeader string

	if strings.Contains(proxy, "@") {
		proxyUrl, err := url.Parse(proxy)
		if err != nil {
			return "", "", "", fmt.Errorf("failed to parse px proxy url [%s]", proxy)
		}
		username := proxyUrl.User.Username()
		password, _ := proxyUrl.User.Password()
		encodedAuth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		authHeader = fmt.Sprintf("Basic %s", encodedAuth)
		host, port, err := net.SplitHostPort(proxyUrl.Host)
		if err != nil {
			return "", "", "", err
		} else if host == "" || port == "" || encodedAuth == "" {
			return "", "", "", fmt.Errorf("failed to split px proxy to get host and port [%s]", proxy)
		}
		return host, port, authHeader, nil
	} else {
		proxy = strings.TrimPrefix(proxy, HttpProtocolPrefix)
		proxy = strings.TrimPrefix(proxy, HttpsProtocolPrefix) // treat https proxy as http proxy if no credential provided
		host, port, err := net.SplitHostPort(proxy)
		if err != nil {
			return "", "", "", err
		} else if host == "" || port == "" {
			return "", "", "", fmt.Errorf("failed to split px proxy to get host and port [%s]", proxy)
		}
		return host, port, authHeader, nil
	}
}

// CanAccessArcusRegisterEndpoint checks if telemetry registration endpoint is reachable
// return true immediately if it can reach to Arcus
// return false after failing 5 times in a row
func CanAccessArcusRegisterEndpoint(
	cluster *corev1.StorageCluster,
	proxy string,
) bool {
	endpoint := getArcusRegisterProxyURL(cluster)
	logrus.Debugf("Checking whether telemetry registration endpoint [%s] is accessible on cluster [%s]...",
		endpoint, cluster.Name)

	url, _ := url.Parse(fmt.Sprintf("https://%s:443/auth/1.0/ping", endpoint))
	request := &http.Request{
		Method: "GET",
		URL:    url,
		Header: map[string][]string{
			// 3 headers are required here by the API, but can use dummy values here.
			// cluster UUID can be empty, so not using it for appliance-id here.
			"product-name": {"portworx"},
			"appliance-id": {"portworx"},
			"component-sn": {cluster.Name},
		},
	}

	client := &http.Client{}
	if proxy != "" {
		if !strings.HasPrefix(strings.ToLower(proxy), "http://") {
			proxy = "http://" + proxy
		}
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			logrus.Warnf("failed to parse proxy [%s] for checking Pure1 connectivity, Err: %v", proxy, err)
			return false
		}
		client.Transport = &http.Transport{
			Proxy:           http.ProxyURL(proxyURL),
			TLSClientConfig: &cryptoTls.Config{InsecureSkipVerify: true},
		}
	}

	for i := 1; i <= arcusPingRetry; i++ {
		response, err := client.Do(request)
		warnMsg := fmt.Sprintf("Failed to access telemetry registration endpoint [%s]", endpoint)
		if err != nil {
			logrus.WithError(err).Warnf(warnMsg)
		} else if response.StatusCode != 200 {
			// Only consider 200 as a successful ping with a properly constructed request.
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			logrus.WithFields(logrus.Fields{
				"code": response.StatusCode,
				"body": string(body),
			}).Warnf(warnMsg)
		} else {
			logrus.Infof("Telemetry registration endpoint [%s] is accessible on cluster [%s]", endpoint, cluster.Name)
			return true
		}
		if i != arcusPingRetry {
			logrus.Warnf("Failed to ping Pure1 [%s], retrying...", endpoint)
			time.Sleep(arcusPingInterval)
		}
	}
	return false
}

func getArcusTelemetryLocation(cluster *corev1.StorageCluster) string {
	if cluster.Annotations[AnnotationTelemetryArcusLocation] != "" {
		location := strings.ToLower(strings.TrimSpace(cluster.Annotations[AnnotationTelemetryArcusLocation]))
		if location == stagingArcusLocation {
			return location
		}
	}
	return productionArcusLocation
}

func getArcusRegisterProxyURL(cluster *corev1.StorageCluster) string {
	if getArcusTelemetryLocation(cluster) == stagingArcusLocation {
		return stagingArcusRegisterProxyURL
	}
	return productionArcusRegisterProxyURL
}

// GetPxProxyEnvVarValue returns the PX_HTTP(S)_PROXY environment variable value for a cluster.
// Note: we only expect one proxy for the telemetry CCM container but we prefer https over http if both are specified
func GetPxProxyEnvVarValue(cluster *corev1.StorageCluster) (string, string) {
	httpProxy := ""
	for _, env := range cluster.Spec.Env {
		key, val := env.Name, env.Value
		if key == EnvKeyPortworxHTTPSProxy {
			return EnvKeyPortworxHTTPSProxy, val
		} else if key == EnvKeyPortworxHTTPProxy {
			httpProxy = val
		}
	}
	if httpProxy != "" {
		return EnvKeyPortworxHTTPProxy, httpProxy
	}
	return "", ""
}

// ValidateAllStorageNodesAreUpgraded validates that all storagenodes are online and have expected PX version
func ValidateAllStorageNodesAreUpgraded(pxImageList map[string]string, cluster *corev1.StorageCluster) error {
	// Get list of expected Portworx node names
	expectedPxNodeList, err := GetExpectedPxNodeList(cluster)
	if err != nil {
		return err
	}

	// Validate StorageNodes are online and have expected PX version
	if err = validateStorageNodes(pxImageList, cluster, len(expectedPxNodeList), defaultStorageNodesValidationTimeout, defaultStorageNodesValidationInterval); err != nil {
		return err
	}
	return nil
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
	imageTag, err := getPxOperatorImageTag()
	if err != nil {
		return nil, err
	}
	// tag is not a valid version, e.g. commit sha in PR automation builds "1a6a788" can be parsed to "1.0.0-a6a788"
	if !strings.Contains(imageTag, ".") {
		logrus.Errorf("Operator tag %s is not a valid version tag, assuming its latest and setting it to %s", imageTag, PxOperatorMasterVersion)
		imageTag = PxOperatorMasterVersion
	}
	// We may run the automation on operator installed using private images,
	// so assume we are testing the latest operator version if failed to parse the tag
	opVersion, err := version.NewVersion(imageTag)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to parse portworx-operator tag to version, assuming its latest and setting it to %s", PxOperatorMasterVersion)
		opVersion, _ = version.NewVersion(PxOperatorMasterVersion)
	}

	logrus.Infof("Testing portworx-operator version [%s]", opVersion.String())
	return opVersion, nil
}

func getPxOperatorImageTag() (string, error) {
	labelSelector := map[string]string{}

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

		var tag string
		for _, container := range operatorDeployment.Spec.Template.Spec.Containers {
			if container.Name == "portworx-operator" {
				if strings.Contains(container.Image, "registry.connect.redhat.com") { // PX Operator deployed via Openshift Marketplace will have "registry.connect.redhat.com" as part of image
					for _, env := range container.Env {
						if env.Name == "OPERATOR_CONDITION_NAME" {
							tag = strings.Split(env.Value, ".v")[1]
							logrus.Infof("Looks like portworx-operator was installed via Openshift Marketplace, image [%s], actual tag [%s]", container.Image, tag)
							return tag, nil
						}
					}
				} else {
					tag = strings.Split(container.Image, ":")[1]
					logrus.Infof("Get portworx-operator image installed [%s], actual tag [%s]", container.Image, tag)
					return tag, nil
				}
			}
		}
	}
	return "", fmt.Errorf("failed to get PX Operator tag")
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

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	logrus.Infof("Alert manager components do not exist")
	return nil
}

// This validates the telemetry container orchestrator usage.  Container orchestrator manages the telemetry registration certificate secret.
// The container orchestrator is a server running in PX which handles gprc requests to save/retrieve/delete the registration certificate secret.
func ValidateTelemetryContainerOrchestrator(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	const (
		OsdBaseLogDir = "/var/lib/osd/log"
		coStateDir    = OsdBaseLogDir + "/coState"
	)

	logrus.Infof("Validating telemetry container orchestrator usage...")

	configMap, err := coreops.Instance().GetConfigMap("px-telemetry-register", cluster.Namespace)
	if err != nil {
		return err
	}

	logrus.Infof("Obtained px-telemetry-register configmap...")

	isCOenabled := strings.Contains(configMap.Data["config_properties_px.yaml"], "certStoreType: \"kvstore\"")
	if !isCOenabled {
		return fmt.Errorf("telemetry container orchestrator 'certStoreType: kvstore' is expected to be set in 'px-telemetry-register' configmap")
	}

	logrus.Infof("Validated 'certStoreType: kvstore' configmap setting")

	// Test validation is done by checking timestamp entries of when the container orchestrator operations were called (server start, secret set/get).
	// The timestamps for secret set/get must be after the timestamp for the start.
	labelSelector := map[string]string{"role": "px-telemetry-registration"}
	nodeList, err := coreops.Instance().GetNodes()
	if err != nil {
		return err
	}

	logrus.Infof("find telemetry registration pod node and px pod on that node...")

	var pxPod v1.Pod
	for _, node := range nodeList.Items {
		// Get telemetry registration pod
		podList, err := coreops.Instance().GetPodsByNodeAndLabels(node.Name, cluster.Namespace, labelSelector)
		if err != nil {
			return err
		}

		if len(podList.Items) == 0 {
			continue
		}
		tregPod := podList.Items[0]
		logrus.Infof("Found telemetry registration pod [%s] on node [%s]", tregPod.Name, node.Name)
		labelSelector = map[string]string{"name": "portworx"}
		podList, err = coreops.Instance().GetPodsByNodeAndLabels(node.Name, cluster.Namespace, labelSelector)
		if err != nil {
			return err
		}

		if len(podList.Items) == 0 {
			return fmt.Errorf("failed to find Portworx pod on telemetry registration node [%s]", node.Name)
		}
		pxPod = podList.Items[0]
		logrus.Infof("Found PX pod [%s] on telemetry registration node [%s]", pxPod.Name, node.Name)
		break
	}

	getCoStateTimeStamp := func(tsFileName string) (int64, error) {
		tmData, err := runCmdInsidePxPod(&pxPod, "cat "+tsFileName, cluster.Namespace, false)
		if err != nil {
			return 0, err
		}

		tm, terr := strconv.ParseInt(string(tmData), 10, 64)
		if terr != nil {
			return 0, terr
		}
		return tm, nil
	}

	coStartTime, startErr := getCoStateTimeStamp(coStateDir + "/kvStart.ts")
	if startErr != nil {
		return fmt.Errorf("container orchestrator validated failed, error obtaining start time from costate file: %v", startErr)
	}
	logrus.Infof("container orchestrater server start time: %v", time.Unix(coStartTime, 0))

	coSetTime, setErr := getCoStateTimeStamp(coStateDir + "/kvSet.ts")
	coGetTime, getErr := getCoStateTimeStamp(coStateDir + "/kvGet.ts")

	if setErr == nil {
		// secret set time exists, check it
		logrus.Infof("container orchestrater set time: %v\n", time.Unix(coSetTime, 0))
		if coSetTime > coStartTime {
			logrus.Infof("container orchestrator validated via set time")
			return nil
		}
		setErr = fmt.Errorf("set time is less than start time, check for upgrade")
		logrus.Infof("%s", setErr.Error())
	}

	if setErr != nil && getErr != nil {
		// No secret handler timestamp exists
		return fmt.Errorf("container orchestrator validated failed, error obtaining costate file: %v and %v", setErr, getErr)
	}

	// secret get time stamp exists, check it
	logrus.Infof("container orchestrater get time: %v\n", time.Unix(coGetTime, 0))
	if coGetTime < coStartTime {
		return fmt.Errorf("container orchestrator validated failed, get time is less than start time")
	}

	logrus.Infof("container orchestrator validated via get time")

	return nil
}

// ValidateTelemetryV2Enabled validates telemetry component is running as expected
func ValidateTelemetryV2Enabled(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	logrus.Info("Validate Telemetry components are enabled")
	validateMetricsCollector := false // TODO: Change this to a specific operator version, once we know in which version metrics-collector will get enabled

	t := func() (interface{}, bool, error) {
		// Validate px-telemetry-registration deployment, pods and container images
		if err := validatePxTelemetryRegistrationV2(pxImageList, cluster, timeout, interval); err != nil {
			return nil, true, err
		}

		// Validate px-telemetry-metrics deployment, pods and container images on operator 1.11+,
		// as metrics collector is disabled in 1.10
		if validateMetricsCollector {
			if err := validatePxTelemetryMetricsCollectorV2(pxImageList, cluster, timeout, interval); err != nil {
				return nil, true, err
			}
		}

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
		if validateMetricsCollector {
			if _, err := coreops.Instance().GetConfigMap("px-telemetry-collector", cluster.Namespace); err != nil {
				return nil, true, err
			}
			if _, err := coreops.Instance().GetConfigMap("px-telemetry-collector-proxy", cluster.Namespace); err != nil {
				return nil, true, err
			}
		}

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

	// Validate registration certificate management via Container Orchestrator
	ccmGoImage, ok := pxImageList["telemetry"]
	if !ok {
		return fmt.Errorf("failed to find image for telemetry")
	}

	ccmGoVersionStr := strings.Split(ccmGoImage, ":")[len(strings.Split(ccmGoImage, ":"))-1]
	ccmGoVersion, err := version.NewSemver(ccmGoVersionStr)
	if err != nil {
		return fmt.Errorf("failed to find telemetry image version")
	}

	masterOpVersion, _ := version.NewVersion(PxOperatorMasterVersion)
	opVersion, _ := GetPxOperatorVersion()
	pxVersion := GetPortworxVersion(cluster)

	// NOTE: These versions will need to be updated when we move the CO code to a release. Currently its bound to master branches
	if opVersion.GreaterThanOrEqual(masterOpVersion) && pxVersion.GreaterThanOrEqual(minimumPxVersionCO) && ccmGoVersion.GreaterThanOrEqual(minimumCcmGoVersionCO) { // Validate the versions
		if err := ValidateTelemetryContainerOrchestrator(pxImageList, cluster, timeout, interval); err != nil {
			return err
		}
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

			// Find Telemetry status line
			telemetryStatusString := regexp.MustCompile("^Telemetry:.*").FindString(output)
			if len(telemetryStatusString) <= 0 {
				return nil, true, fmt.Errorf("got empty Telemetry status line, command output did not match expected, output [%s]", strings.TrimSpace(output))
			}

			if telemetryShouldBeEnabled && !strings.Contains(telemetryStatusString, "Healthy") {
				return nil, true, fmt.Errorf("[%s (%s)] Telemetry is enabled and should be Healthy in pxctl status on PX node, but got [%s]", pxPod.Spec.NodeName, pxPod.Name, strings.TrimSpace(telemetryStatusString))
			} else if !telemetryShouldBeEnabled && !strings.Contains(telemetryStatusString, "Disabled") {
				return nil, true, fmt.Errorf("[%s (%s)] Telemetry is not enabled and should be Disabled in pxctl status on PX node, but got [%s]", pxPod.Spec.NodeName, pxPod.Name, strings.TrimSpace(telemetryStatusString))
			} else if !strings.Contains(telemetryStatusString, "Disabled") && !strings.Contains(telemetryStatusString, "Healthy") {
				return nil, true, fmt.Errorf("[%s (%s)] Telemetry is Enabled=%v, but pxctl on PX node returned unexpected status [%s]", pxPod.Spec.NodeName, pxPod.Name, telemetryShouldBeEnabled, telemetryStatusString)
			}

			logrus.Infof("[%s(%s)] Telemetry is Enabled=%v and pxctl status on PX node reports [%s]", pxPod.Spec.NodeName, pxPod.Name, telemetryShouldBeEnabled, strings.TrimSpace(telemetryStatusString))
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
		return fmt.Errorf("failed to validate [px-telemetry-phonehome] daemonset in [%s] namespace, Err: %v", cluster.Namespace, err)
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

func validatePxTelemetryMetricsCollectorV2(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	// Validate px-telemetry-metrics-collector deployment, pods and container images
	logrus.Info("Validate px-telemetry-metrics-collector deployment and images")
	metricsCollectorDep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "px-telemetry-metrics-collector",
			Namespace: cluster.Namespace,
		},
	}
	if err := appops.Instance().ValidateDeployment(metricsCollectorDep, timeout, interval); err != nil {
		return fmt.Errorf("failed to validate [%s] deployment in [%s] namespace, Err: %v", metricsCollectorDep.Name, metricsCollectorDep.Namespace, err)
	}

	pods, err := appops.Instance().GetDeploymentPods(metricsCollectorDep)
	if err != nil {
		return err
	}

	// Validate image inside px-metric-collector[init-cont]
	if image, ok := pxImageList["telemetryProxy"]; ok {
		if err := validateContainerImageInsidePods(cluster, image, "init-cont", &v1.PodList{Items: pods}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find image for px-telemetry-metrics-collector[init-cont]")
	}

	// Validate image inside px-metrics-collector[collector]
	if image, ok := pxImageList["metricsCollector"]; ok {
		if err := validateContainerImageInsidePods(cluster, image, "collector", &v1.PodList{Items: pods}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find image for px-telemetry-metrics-collector[collector]")
	}

	// Validate image inside px-metric-collector[envoy]
	if image, ok := pxImageList["telemetryProxy"]; ok {
		if err := validateContainerImageInsidePods(cluster, image, "envoy", &v1.PodList{Items: pods}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find image for px-telemetry-metrics-collector[envoy]")
	}

	return nil
}

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
		return fmt.Errorf("failed to validate [%s] deployment in [%s] namespace, Err: %v", registrationServiceDep.Name, registrationServiceDep.Namespace, err)
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
	opVersion, _ := GetPxOperatorVersion()
	validateMetricsCollector := opVersion.LessThan(opVer1_10)

	t := func() (interface{}, bool, error) {
		if validateMetricsCollector {
			// Wait for the deployment to become online
			dep := appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "px-metrics-collector",
					Namespace: cluster.Namespace,
				},
			}
			if err := appops.Instance().ValidateDeployment(&dep, timeout, interval); err != nil {
				return nil, true, fmt.Errorf("failed to validate [%s] deployment in [%s] namespace, Err: %v", dep.Name, dep.Namespace, err)
			}

			deployment, err := appops.Instance().GetDeployment(dep.Name, dep.Namespace)
			if err != nil {
				return nil, true, err
			}

			/* TODO: We need to make this work for spawn
			expectedDeployment := GetExpectedDeployment(&testing.T{}, "metricsCollectorDeployment.yaml")
			if equal, err := util.DeploymentDeepEqual(expectedDeployment, deployment); !equal {
				return err
			}
			*/

			// Verify metrics collector image
			imageName, ok := pxImageList["metricsCollector"]
			if !ok {
				return nil, true, fmt.Errorf("failed to find image for metrics collector")
			}
			imageName = util.GetImageURN(cluster, imageName)

			if deployment.Spec.Template.Spec.Containers[0].Image != imageName {
				return nil, true, fmt.Errorf("collector image mismatch, image: %s, expected: %s",
					deployment.Spec.Template.Spec.Containers[0].Image,
					imageName)
			}

			// Verify metrics collector proxy image
			imageName, ok = pxImageList["metricsCollectorProxy"]
			if !ok {
				return nil, true, fmt.Errorf("failed to find image for metrics collector proxy")
			}
			imageName = util.GetImageURN(cluster, imageName)

			if deployment.Spec.Template.Spec.Containers[1].Image != imageName {
				return nil, true, fmt.Errorf("collector proxy image mismatch, image: %s, expected: %s",
					deployment.Spec.Template.Spec.Containers[1].Image,
					imageName)
			}

			_, err = rbacops.Instance().GetRole("px-metrics-collector", cluster.Namespace)
			if err != nil {
				return nil, true, err
			}

			_, err = rbacops.Instance().GetRoleBinding("px-metrics-collector", cluster.Namespace)
			if err != nil {
				return nil, true, err
			}

			// Verify collector config map
			_, err = coreops.Instance().GetConfigMap("px-collector-config", cluster.Namespace)
			if err != nil {
				return nil, true, err
			}

			// Verify collector proxy config map
			_, err = coreops.Instance().GetConfigMap("px-collector-proxy-config", cluster.Namespace)
			if err != nil {
				return nil, true, err
			}

			// Verify collector service account
			_, err = coreops.Instance().GetServiceAccount("px-metrics-collector", cluster.Namespace)
			if err != nil {
				return nil, true, err
			}
		}

		// Verify telemetry config map
		_, err := coreops.Instance().GetConfigMap("px-telemetry-config", cluster.Namespace)
		if err != nil {
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
		expectedConstraints, err := util.GetTopologySpreadConstraintsFromNodes(nodeList, existingDeployment.Spec.Selector.MatchLabels)
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
		isOKE(cluster) || cluster.Namespace != "kube-system" {
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

func isOKE(cluster *corev1.StorageCluster) bool {
	enabled, err := strconv.ParseBool(cluster.Annotations["portworx.io/is-oke"])
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

	htmlData, err := io.ReadAll(resp.Body)
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

func validateStorageClusterInState(cluster *corev1.StorageCluster, state string, conditions []corev1.ClusterCondition) func() (interface{}, bool, error) {
	return func() (interface{}, bool, error) {
		cluster, err := operatorops.Instance().GetStorageCluster(cluster.Name, cluster.Namespace)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get StorageCluster [%s] in [%s], Err: %v", cluster.Name, cluster.Namespace, err)
		}
		if cluster.Status.Phase != state {
			if cluster.Status.Phase == "" {
				return nil, true, fmt.Errorf("failed to get cluster status")
			}
			return nil, true, fmt.Errorf("cluster status: [%s], expected status: [%s]", cluster.Status.Phase, state)
		}
		for _, condition := range conditions {
			c := util.GetStorageClusterCondition(cluster, condition.Source, condition.Type)
			if c == nil {
				return nil, true, fmt.Errorf("failed to get cluster condition %s%s%s", condition.Source, condition.Type, condition.Status)
			} else if c.Status != condition.Status {
				return nil, true, fmt.Errorf("expect condition %s%s%s but got %s%s%s", condition.Source, condition.Type, condition.Status,
					c.Source, c.Type, c.Status)
			}
		}
		logrus.Infof("StorageCluster [%s] is [%s]", cluster.Name, cluster.Status.Phase)
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
	state := string(corev1.ClusterConditionStatusOnline)
	var conditions []corev1.ClusterCondition
	opVersion, _ := GetPxOperatorVersion()
	if opVersion.GreaterThanOrEqual(opVer23_5) {
		state = string(corev1.ClusterStateRunning)
		conditions = append(conditions, corev1.ClusterCondition{
			Source: "Portworx",
			Type:   corev1.ClusterConditionTypeRuntimeState,
			Status: corev1.ClusterConditionStatusOnline,
		})
	}
	out, err := task.DoRetryWithTimeout(validateStorageClusterInState(cluster, state, conditions), timeout, interval)
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
	expectedPxNodeList, err := GetExpectedPxNodeList(clusterSpec)
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
	if err = validateStorageClusterPods(clusterSpec, expectedPxNodeList, timeout, interval, podTestFn); err != nil {
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
			Monitoring: &corev1.MonitoringSpec{Telemetry: &corev1.TelemetrySpec{}},
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

// setupEtcHosts sets up given ip/hosts in `/etc/hosts` file for "local" DNS resolution  (i.e. emulate K8s DNS)
// - you will need to be a root-user to run this
// - also, make sure your `/etc/nsswitch.conf` file contains `hosts: files ...` as a first entry
// - hostnames should be in `<service>.<namespace>` format
func SetupEtcHosts(t *testing.T, ip string, hostnames ...string) {
	if len(hostnames) <= 0 {
		return
	}
	if err := unix.Access(etcHostsFile, unix.W_OK); err != nil {
		t.Skipf("This test requires ROOT user  (writeable /etc/hosts): %s", err)
	}

	// read original content
	content, err := os.ReadFile("/etc/hosts")
	require.NoError(t, err)

	if ip == "" {
		ip = "127.0.0.1"
	}

	// update content
	bb := bytes.NewBuffer(content)
	bb.WriteString(tempEtcHostsMarker)
	bb.WriteRune('\n')
	for _, hn := range hostnames {
		bb.WriteString(ip)
		bb.WriteRune('\t')
		bb.WriteString(hn)
		bb.WriteRune('\t')
		bb.WriteString(hn)
		bb.WriteString(".svc.cluster.local")
		bb.WriteRune('\n')
	}

	// overwrite /etc/hosts
	fd, err := os.OpenFile(etcHostsFile, os.O_WRONLY|os.O_TRUNC, 0666)
	require.NoError(t, err)

	n, err := fd.Write(bb.Bytes())
	require.NoError(t, err)
	assert.Equal(t, bb.Len(), n, "short write")
	fd.Close()

	// waiting for dns can be resolved
	for i := 0; i < 60; i++ {
		var ips []net.IP
		ips, err = net.LookupIP(hostnames[0])
		if err != nil || !strings.Contains(fmt.Sprintf("%v", ips), ip) {
			logrus.WithFields(logrus.Fields{
				"error": err,
				"ips":   ips,
				"ip":    ip,
				"hosts": hostnames,
			}).Warnf("failed to set /etc/hosts, retrying")
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	require.NoError(t, err)
}

func RestoreEtcHosts(t *testing.T) {
	fd, err := os.Open(etcHostsFile)
	require.NoError(t, err)
	var bb bytes.Buffer
	scan := bufio.NewScanner(fd)
	for scan.Scan() {
		line := scan.Text()
		if line == tempEtcHostsMarker {
			// skip copying everything below `tempEtcHostsMarker`
			break
		}
		bb.WriteString(line)
		bb.WriteRune('\n')
	}
	fd.Close()

	// overwrite /etc/hosts
	require.True(t, bb.Len() > 0)
	fd, err = os.OpenFile(etcHostsFile, os.O_WRONLY|os.O_TRUNC, 0666)
	require.NoError(t, err)

	n, err := fd.Write(bb.Bytes())
	require.NoError(t, err)
	assert.Equal(t, bb.Len(), n, "short write")
	fd.Close()
}
