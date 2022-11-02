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
)

// TestSpecPath is the path for all test specs. Due to currently functional test and
// unit test use different path, this needs to be set accordingly.
var TestSpecPath = "testspec"

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
		if _, err = operatorops.Instance().UpdateStorageCluster(cluster); err != nil {
			return err
		}
	}

	return operatorops.Instance().DeleteStorageCluster(cluster.Name, cluster.Namespace)
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

	// Construct PX Version string used to match to deployed expected PX version
	if strings.Contains(pxImageList["version"], "_") {
		if cluster.Spec.Env != nil {
			for _, env := range cluster.Spec.Env {
				if env.Name == PxReleaseManifestURLEnvVarName {
					// Looking for clear PX version before /version in the URL
					ver := regexp.MustCompile(`\S+\/(\d.\S+)\/version`).FindStringSubmatch(env.Value)
					if ver != nil {
						expectedPxVersion = ver[1]
					} else {
						// If the above regex found nothing, assuming it was a master version URL
						expectedPxVersion = PxMasterVersion
					}
					break
				}
			}
		}
	} else {
		expectedPxVersion = strings.TrimSpace(regexp.MustCompile(`:(\S+)`).FindStringSubmatch(pxImageList["version"])[1])
	}

	if expectedPxVersion == "" {
		return fmt.Errorf("failed to get expected PX version")
	}

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
	if validateCSI(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	// Validate Monitoring
	if err = ValidateMonitoring(pxImageList, cluster, timeout, interval); err != nil {
		return err
	}

	// Validate PortworxProxy
	if err = ValidatePortworxProxy(cluster, timeout); err != nil {
		return err
	}

	// Validate Security
	previouslyEnabled := false
	if err = ValidateSecurity(cluster, previouslyEnabled, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidatePvcController validates PVC Controller components and images
func ValidatePvcController(pxImageList map[string]string, cluster *corev1.StorageCluster, k8sVersion string, timeout, interval time.Duration) error {
	pvcControllerDp := &appsv1.Deployment{}
	pvcControllerDp.Name = "portworx-pvc-controller"
	pvcControllerDp.Namespace = cluster.Namespace

	if isPVCControllerEnabled(cluster) {
		logrus.Debug("PVC Controller is Enabled")

		if err := appops.Instance().ValidateDeployment(pvcControllerDp, timeout, interval); err != nil {
			return err
		}

		if err := validateImageTag(k8sVersion, cluster.Namespace, map[string]string{"name": "portworx-pvc-controller"}); err != nil {
			return err
		}

		// Validate PVC Controller custom port, if any were set
		if err := validatePvcControllerPorts(cluster.Annotations, pvcControllerDp, timeout, interval); err != nil {
			return err
		}

		// Validate PVC Controller ClusterRole
		_, err := rbacops.Instance().GetClusterRole(pvcControllerDp.Name)
		if errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ClusterRole %s, Err: %v", pvcControllerDp.Name, err)
		}

		// Validate PVC Controller ClusterRoleBinding
		_, err = rbacops.Instance().GetClusterRoleBinding(pvcControllerDp.Name)
		if errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ClusterRoleBinding %s, Err: %v", pvcControllerDp.Name, err)
		}

		// Validate PVC Controller ServiceAccount
		_, err = coreops.Instance().GetServiceAccount(pvcControllerDp.Name, pvcControllerDp.Namespace)
		if errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ServiceAccount %s, Err: %v", pvcControllerDp.Name, err)
		}

		// Validate PVC controller deployment pod topology spread constraints
		if err := validatePodTopologySpreadConstraints(pvcControllerDp, timeout, interval); err != nil {
			return err
		}
	} else {
		logrus.Debug("PVC Controller is Disabled")
		// Validate portworx-pvc-controller deployment is terminated or doesn't exist
		if err := validateTerminatedDeployment(pvcControllerDp, timeout, interval); err != nil {
			return err
		}

		// Validate VC Controller ClusterRole doesn't exist
		_, err := rbacops.Instance().GetClusterRole(pvcControllerDp.Name)
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ClusterRole %s, is found when shouldn't be", pvcControllerDp.Name)
		}

		// Validate VC Controller ClusterRoleBinding doesn't exist
		_, err = rbacops.Instance().GetClusterRoleBinding(pvcControllerDp.Name)
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ClusterRoleBinding %s, is found when shouldn't be", pvcControllerDp.Name)
		}

		// Validate VC Controller ServiceAccount doesn't exist
		_, err = coreops.Instance().GetServiceAccount(pvcControllerDp.Name, pvcControllerDp.Namespace)
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ServiceAccount %s, is found when shouldn't be", pvcControllerDp.Name)
		}
	}

	return nil
}

// ValidateStork validates Stork components and images
func ValidateStork(pxImageList map[string]string, cluster *corev1.StorageCluster, k8sVersion string, timeout, interval time.Duration) error {
	storkDp := &appsv1.Deployment{}
	storkDp.Name = "stork"
	storkDp.Namespace = cluster.Namespace

	storkSchedulerDp := &appsv1.Deployment{}
	storkSchedulerDp.Name = "stork-scheduler"
	storkSchedulerDp.Namespace = cluster.Namespace

	if cluster.Spec.Stork != nil && cluster.Spec.Stork.Enabled {
		logrus.Debug("Stork is Enabled in StorageCluster")

		// Validate stork deployment and pods
		if err := validateDeployment(storkDp, timeout, interval); err != nil {
			return err
		}

		var storkImageName string
		if cluster.Spec.Stork.Image == "" {
			if value, ok := pxImageList["stork"]; ok {
				storkImageName = value
			} else {
				return fmt.Errorf("failed to find image for stork")
			}
		} else {
			storkImageName = cluster.Spec.Stork.Image
		}

		storkImage := util.GetImageURN(cluster, storkImageName)
		err := validateImageOnPods(storkImage, cluster.Namespace, map[string]string{"name": "stork"})
		if err != nil {
			return err
		}

		// Validate stork namespace env var
		if err := validateStorkNamespaceEnvVar(cluster.Namespace, storkDp, timeout, interval); err != nil {
			return err
		}

		// Validate stork-scheduler deployment and pods
		if err := validateDeployment(storkSchedulerDp, timeout, interval); err != nil {
			return err
		}

		K8sVer1_22, _ := version.NewVersion("1.22")
		kubeVersion, _, err := GetFullVersion()
		if err != nil {
			return err
		}

		if kubeVersion != nil && kubeVersion.GreaterThanOrEqual(K8sVer1_22) {
			// TODO Image tag for stork-scheduler is hardcoded to v1.21.4 for clusters 1.22 and up
			if err = validateImageTag("v1.21.4", cluster.Namespace, map[string]string{"name": "stork-scheduler"}); err != nil {
				return err
			}
		} else {
			if err = validateImageTag(k8sVersion, cluster.Namespace, map[string]string{"name": "stork-scheduler"}); err != nil {
				return err
			}
		}

		// Validate webhook-controller arguments
		if err := validateStorkWebhookController(cluster.Spec.Stork.Args, storkDp, timeout, interval); err != nil {
			return err
		}

		// Validate hostNetwork parameter
		if err := validateStorkHostNetwork(cluster.Spec.Stork.HostNetwork, storkDp, timeout, interval); err != nil {
			return err
		}

		// Validate stork deployment pod topology spread constraints
		if err := validatePodTopologySpreadConstraints(storkDp, timeout, interval); err != nil {
			return err
		}

		// Validate stork scheduler deployment pod topology spread constraints
		if err := validatePodTopologySpreadConstraints(storkSchedulerDp, timeout, interval); err != nil {
			return err
		}
	} else {
		logrus.Debug("Stork is Disabled in StorageCluster")
		// Validate stork deployment is terminated or doesn't exist
		if err := validateTerminatedDeployment(storkDp, timeout, interval); err != nil {
			return err
		}

		// Validate stork-scheduler deployment is terminated or doesn't exist
		if err := validateTerminatedDeployment(storkSchedulerDp, timeout, interval); err != nil {
			return err
		}
	}

	return nil
}

// ValidateAutopilot validates Autopilot components and images
func ValidateAutopilot(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	autopilotDp := &appsv1.Deployment{}
	autopilotDp.Name = "autopilot"
	autopilotDp.Namespace = cluster.Namespace
	autopilotConfigMapName := "autopilot-config"

	if cluster.Spec.Autopilot != nil && cluster.Spec.Autopilot.Enabled {
		logrus.Debug("Autopilot is Enabled in StorageCluster")

		// Validate autopilot deployment and pods
		if err := validateDeployment(autopilotDp, timeout, interval); err != nil {
			return err
		}

		var autopilotImageName string
		if cluster.Spec.Autopilot.Image == "" {
			if value, ok := pxImageList[autopilotDp.Name]; ok {
				autopilotImageName = value
			} else {
				return fmt.Errorf("failed to find image for %s", autopilotDp.Name)
			}
		} else {
			autopilotImageName = cluster.Spec.Autopilot.Image
		}

		autopilotImage := util.GetImageURN(cluster, autopilotImageName)
		if err := validateImageOnPods(autopilotImage, cluster.Namespace, map[string]string{"name": autopilotDp.Name}); err != nil {
			return err
		}

		// Validate Autopilot ClusterRole
		_, err := rbacops.Instance().GetClusterRole(autopilotDp.Name)
		if errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ClusterRole %s, Err: %v", autopilotDp.Name, err)
		}

		// Validate Autopilot ClusterRoleBinding
		_, err = rbacops.Instance().GetClusterRoleBinding(autopilotDp.Name)
		if errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ClusterRoleBinding %s, Err: %v", autopilotDp.Name, err)
		}

		// Validate Autopilot ConfigMap
		_, err = coreops.Instance().GetConfigMap(autopilotConfigMapName, autopilotDp.Namespace)
		if errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ConfigMap %s, Err: %v", autopilotConfigMapName, err)
		}

		// Validate Autopilot ServiceAccount
		_, err = coreops.Instance().GetServiceAccount(autopilotDp.Name, autopilotDp.Namespace)
		if errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ServiceAccount %s, Err: %v", autopilotDp.Name, err)
		}
	} else {
		logrus.Debug("Autopilot is Disabled in StorageCluster")
		// Validate autopilot deployment is terminated or doesn't exist
		if err := validateTerminatedDeployment(autopilotDp, timeout, interval); err != nil {
			return err
		}

		// Validate Autopilot ClusterRole doesn't exist
		_, err := rbacops.Instance().GetClusterRole(autopilotDp.Name)
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ClusterRole %s, is found when shouldn't be", autopilotDp.Name)
		}

		// Validate Autopilot ClusterRoleBinding doesn't exist
		_, err = rbacops.Instance().GetClusterRoleBinding(autopilotDp.Name)
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ClusterRoleBinding %s, is found when shouldn't be", autopilotDp.Name)
		}

		// Validate Autopilot ConfigMap doesn't exist
		_, err = coreops.Instance().GetConfigMap(autopilotConfigMapName, autopilotDp.Namespace)
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ConfigMap %s, is found when shouldn't be", autopilotConfigMapName)
		}

		// Validate Autopilot ServiceAccount doesn't exist
		_, err = coreops.Instance().GetServiceAccount(autopilotDp.Name, autopilotDp.Namespace)
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ServiceAccount %s, is found when shouldn't be", autopilotDp.Name)
		}
	}

	return nil
}

// ValidatePortworxProxy validates portworx proxy components
func ValidatePortworxProxy(cluster *corev1.StorageCluster, timeout time.Duration) error {
	proxyDs := &appsv1.DaemonSet{}
	proxyDs.Name = "portworx-proxy"
	proxyDs.Namespace = "kube-system"

	pxService := &v1.Service{}
	pxService.Name = "portworx-service"
	pxService.Namespace = "kube-system"

	if isPortworxProxyEnabled(cluster) {
		logrus.Debug("Portworx proxy is enabled in StorageCluster")

		// Validate Portworx proxy DaemonSet
		if err := validateDaemonSet(proxyDs, timeout); err != nil {
			return err
		}

		// Validate Portworx proxy ServiceAccount
		_, err := coreops.Instance().GetServiceAccount(proxyDs.Name, proxyDs.Namespace)
		if errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ServiceAccount %s, Err: %v", proxyDs.Name, err)
		}

		// Validate Portworx proxy ClusterRoleBinding
		_, err = rbacops.Instance().GetClusterRoleBinding(proxyDs.Name)
		if errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ClusterRoleBinding %s, Err: %v", proxyDs.Name, err)
		}

		// Validate Portworx proxy Service in kube-system namespace
		if err := validatePortworxService("kube-system"); err != nil {
			return err
		}
		_, err = coreops.Instance().GetService(pxService.Name, pxService.Namespace)
		if errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate Service %s, Err: %v", pxService.Name, err)
		}
	} else {
		logrus.Debug("Portworx proxy is disabled in StorageCluster")

		// Validate Portworx proxy DaemonSet is terminated or doesn't exist
		if err := validateTerminatedDaemonSet(proxyDs, timeout); err != nil {
			return err
		}

		// Validate Portworx proxy Service doesn't exist if cluster is not deployed in kube-system namespace
		if cluster.Namespace != "kube-system" {
			_, err := coreops.Instance().GetService(pxService.Name, pxService.Namespace)
			if !errors.IsNotFound(err) {
				return fmt.Errorf("failed to validate Service %s, is found when shouldn't be", pxService.Name)
			}
		}

		// Validate Portworx proxy ClusterRoleBinding doesn't exist
		_, err := rbacops.Instance().GetClusterRoleBinding(proxyDs.Name)
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ClusterRoleBinding %s, is found when shouldn't be", proxyDs.Name)
		}

		// Validate Portworx proxy ServiceAccount doesn't exist
		_, err = coreops.Instance().GetServiceAccount(proxyDs.Name, proxyDs.Namespace)
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to validate ServiceAccount %s, is found when shouldn't be", proxyDs.Name)
		}
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

func validateCSI(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	csi := cluster.Spec.CSI.Enabled
	pxCsiDp := &appsv1.Deployment{}
	pxCsiDp.Name = "px-csi-ext"
	pxCsiDp.Namespace = cluster.Namespace

	if csi {
		logrus.Debug("CSI is enabled in StorageCluster")
		if err := validateCsiContainerInPxPods(cluster.Namespace, csi, timeout, interval); err != nil {
			return err
		}

		// Validate CSI container image inside Portworx OCI Monitor pods
		if err := validatePortworxOciMonCsiImage(cluster.Namespace, pxImageList); err != nil {
			return err
		}

		// Validate px-csi-ext deployment and pods
		if err := validateDeployment(pxCsiDp, timeout, interval); err != nil {
			return err
		}

		// Validate CSI container images inside px-csi-ext pods
		if err := validateCsiExtImages(cluster.Namespace, pxImageList); err != nil {
			return err
		}

		// Validate CSI deployment pod topology spread constraints
		if err := validatePodTopologySpreadConstraints(pxCsiDp, timeout, interval); err != nil {
			return err
		}

		// Validate CSI topology specs
		if err := validateCSITopologySpecs(cluster.Namespace, cluster.Spec.CSI.Topology, timeout, interval); err != nil {
			return err
		}
	} else {
		logrus.Debug("CSI is disabled in StorageCluster")
		if err := validateCsiContainerInPxPods(cluster.Namespace, csi, timeout, interval); err != nil {
			return err
		}

		// Validate px-csi-ext deployment doesn't exist
		if err := validateTerminatedDeployment(pxCsiDp, timeout, interval); err != nil {
			return err
		}
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

func validatePvcControllerPorts(annotations map[string]string, pvcControllerDeployment *appsv1.Deployment, timeout, interval time.Duration) error {
	logrus.Debug("Validate PVC Controller custom ports")

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
									return nil, true, fmt.Errorf("failed to validate secure-port, secure-port is missing from annotations in the StorageCluster, but is found in the PVC Controler pod %s", pod.Name)
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

func validatePortworxOciMonCsiImage(namespace string, pxImageList map[string]string) error {
	var csiNodeDriverRegistrar string

	logrus.Debug("Validating CSI container images inside Portworx OCI Monitor pods")

	// Get Portworx pods
	listOptions := map[string]string{"name": "portworx"}
	pods, err := coreops.Instance().GetPods(namespace, listOptions)
	if err != nil {
		return err
	}

	// We looking for this image in the container
	if value, ok := pxImageList["csiNodeDriverRegistrar"]; ok {
		csiNodeDriverRegistrar = value
	} else {
		return fmt.Errorf("failed to find image for csiNodeDriverRegistrar")
	}

	// Go through each pod and find all container and match images for each container
	for _, pod := range pods.Items {
		for _, container := range pod.Spec.Containers {
			if container.Name == "csi-node-driver-registrar" {
				if container.Image != csiNodeDriverRegistrar {
					return fmt.Errorf("found container %s, expected image: %s, actual image: %s", container.Name, csiNodeDriverRegistrar, container.Image)
				}
				break
			}
		}
	}

	return nil
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

func validateCsiExtImages(namespace string, pxImageList map[string]string) error {
	var csiProvisionerImage string
	var csiSnapshotterImage string
	var csiResizerImage string

	logrus.Debug("Validating CSI container images inside px-csi-ext pods")

	deployment, err := appops.Instance().GetDeployment("px-csi-ext", namespace)
	if err != nil {
		return err
	}

	pods, err := appops.Instance().GetDeploymentPods(deployment)
	if err != nil {
		return err
	}

	// We looking for these 3 images in 3 containers in the 3 px-csi-ext pods
	if value, ok := pxImageList["csiProvisioner"]; ok {
		csiProvisionerImage = value
	} else {
		return fmt.Errorf("failed to find image for csiProvisioner")
	}

	if value, ok := pxImageList["csiSnapshotter"]; ok {
		csiSnapshotterImage = value
	} else {
		return fmt.Errorf("failed to find image for csiSnapshotter")
	}

	if value, ok := pxImageList["csiResizer"]; ok {
		csiResizerImage = value
	} else {
		return fmt.Errorf("failed to find image for csiResizer")
	}

	// Go through each pod and find all container and match images for each container
	for _, pod := range pods {
		for _, container := range pod.Spec.Containers {
			if container.Name == "csi-external-provisioner" {
				if container.Image != csiProvisionerImage {
					return fmt.Errorf("found container %s, expected image: %s, actual image: %s", container.Name, csiProvisionerImage, container.Image)
				}
			} else if container.Name == "csi-snapshotter" {
				if container.Image != csiSnapshotterImage {
					return fmt.Errorf("found container %s, expected image: %s, actual image: %s", container.Name, csiSnapshotterImage, container.Image)
				}
			} else if container.Name == "csi-resizer" {
				if container.Image != csiResizerImage {
					return fmt.Errorf("found container %s, expected image: %s, actual image: %s", container.Name, csiResizerImage, container.Image)
				}
			}
		}
	}
	return nil
}

func validateImageOnPods(image, namespace string, listOptions map[string]string) error {
	pods, err := coreops.Instance().GetPods(namespace, listOptions)
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		foundImage := false
		for _, container := range pod.Spec.Containers {
			if container.Image == image {
				foundImage = true
				break
			}
		}

		if !foundImage {
			return fmt.Errorf("failed to validate image %s on pod: %v",
				image, pod)
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
	if cluster.Spec.Security != nil &&
		cluster.Spec.Security.Enabled {
		logrus.Infof("PX Security is enabled")
		return ValidateSecurityEnabled(cluster, timeout, interval)
	}

	logrus.Infof("PX Security is not enabled")
	return ValidateSecurityDisabled(cluster, previouslyEnabled, timeout, interval)
}

// ValidateSecurityEnabled validates PX Security components are enabled/running as expected
func ValidateSecurityEnabled(cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	storkDp := &appsv1.Deployment{}
	storkDp.Name = "stork"
	storkDp.Namespace = cluster.Namespace

	t := func() (interface{}, bool, error) {
		// Validate Stork ENV vars, if Stork is enabled
		if cluster.Spec.Stork != nil && cluster.Spec.Stork.Enabled {
			// Validate stork deployment and pods
			if err := validateDeployment(storkDp, timeout, interval); err != nil {
				return "", true, fmt.Errorf("failed to validate Stork deployment and pods, err %v", err)
			}

			// Validate Security ENv vars in Stork pods
			if err := validateStorkSecurityEnvVar(cluster, storkDp, timeout, interval); err != nil {
				return "", true, fmt.Errorf("failed to validate Stork Security ENV vars, err %v", err)
			}
		}

		if _, err := coreops.Instance().GetSecret("px-admin-token", cluster.Namespace); err != nil {
			return "", true, fmt.Errorf("failed to find secret px-admin-token, err %v", err)
		}

		if _, err := coreops.Instance().GetSecret("px-user-token", cluster.Namespace); err != nil {
			return "", true, fmt.Errorf("failed to find secret px-user-token, err %v", err)
		}

		if _, err := coreops.Instance().GetSecret("px-shared-secret", cluster.Namespace); err != nil {
			return "", true, fmt.Errorf("failed to find secret px-shared-secret, err %v", err)
		}

		if _, err := coreops.Instance().GetSecret("px-system-secrets", cluster.Namespace); err != nil {
			return "", true, fmt.Errorf("failed to find secret px-system-secrets, err %v", err)
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	return nil
}

// ValidateSecurityDisabled validates PX Security components are disabled/uninstalled as expected
func ValidateSecurityDisabled(cluster *corev1.StorageCluster, previouslyEnabled bool, timeout, interval time.Duration) error {
	storkDp := &appsv1.Deployment{}
	storkDp.Name = "stork"
	storkDp.Namespace = cluster.Namespace

	t := func() (interface{}, bool, error) {
		// Validate Stork ENV vars, if Stork is enabled
		if cluster.Spec.Stork != nil && cluster.Spec.Stork.Enabled {
			// Validate Stork deployment and pods
			if err := validateDeployment(storkDp, timeout, interval); err != nil {
				return "", true, fmt.Errorf("failed to validate Stork deployment and pods, err %v", err)
			}

			// Validate Security ENv vars in Stork pods
			if err := validateStorkSecurityEnvVar(cluster, storkDp, timeout, interval); err != nil {
				return "", true, fmt.Errorf("failed to validate Stork Security ENV vars, err %v", err)
			}
		}

		// *-token secrets are always deleted regardless if security was previously enabled or not
		_, err := coreops.Instance().GetSecret("px-admin-token", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return "", true, fmt.Errorf("found secret px-admin-token, when should't have, err %v", err)
		}

		_, err = coreops.Instance().GetSecret("px-user-token", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return "", true, fmt.Errorf("found secret px-user-token, when shouldn't have, err %v", err)
		}

		if previouslyEnabled {
			if _, err := coreops.Instance().GetSecret("px-shared-secret", cluster.Namespace); err != nil {
				return "", true, fmt.Errorf("failed to find secret px-shared-secret, err %v", err)
			}

			if _, err := coreops.Instance().GetSecret("px-system-secrets", cluster.Namespace); err != nil {
				return "", true, fmt.Errorf("failed to find secret px-system-secrets, err %v", err)
			}
		} else {
			_, err := coreops.Instance().GetSecret("px-shared-secret", cluster.Namespace)
			if !errors.IsNotFound(err) {
				return "", true, fmt.Errorf("found secret px-shared-secret, when shouldn't have, err %v", err)
			}

			_, err = coreops.Instance().GetSecret("px-system-secrets", cluster.Namespace)
			if !errors.IsNotFound(err) {
				return "", true, fmt.Errorf("found secret px-system-secrets, when shouldn't have, err %v", err)
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
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

	if err := ValidateTelemetry(pxImageList, cluster, timeout, interval); err != nil {
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

	return nil
}

// ValidateTelemetryUninstalled validates telemetry component is uninstalled as expected
func ValidateTelemetryUninstalled(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	t := func() (interface{}, bool, error) {
		_, err := appops.Instance().GetDeployment("px-metrics-collector", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return "", true, fmt.Errorf("wait for deletion, err %v", err)
		}

		_, err = rbacops.Instance().GetRole("px-metrics-collector", cluster.Name)
		if !errors.IsNotFound(err) {
			return "", true, fmt.Errorf("wait for deletion, err %v", err)
		}

		_, err = rbacops.Instance().GetRoleBinding("px-metrics-collector", cluster.Name)
		if !errors.IsNotFound(err) {
			return "", true, fmt.Errorf("wait for deletion, err %v", err)
		}

		_, err = coreops.Instance().GetConfigMap("px-telemetry-config", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return "", true, fmt.Errorf("wait for deletion, err %v", err)
		}

		_, err = coreops.Instance().GetConfigMap("px-collector-config", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return "", true, fmt.Errorf("wait for deletion, err %v", err)
		}

		_, err = coreops.Instance().GetConfigMap("px-collector-proxy-config", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return "", true, fmt.Errorf("wait for deletion, err %v", err)
		}

		_, err = coreops.Instance().GetServiceAccount("px-metrics-collector", cluster.Namespace)
		if !errors.IsNotFound(err) {
			return "", true, fmt.Errorf("wait for deletion, err %v", err)
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, interval); err != nil {
		return err
	}

	logrus.Infof("Telemetry is disabled")
	return nil
}

// ValidateTelemetry validates telemetry component is installed/uninstalled as expected
func ValidateTelemetry(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	if cluster.Spec.Monitoring != nil &&
		cluster.Spec.Monitoring.Telemetry != nil &&
		cluster.Spec.Monitoring.Telemetry.Enabled {
		return ValidateTelemetryInstalled(pxImageList, cluster, timeout, interval)
	}

	return ValidateTelemetryUninstalled(pxImageList, cluster, timeout, interval)
}

// ValidateAlertManager validates alertManager components
func ValidateAlertManager(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	if cluster.Spec.Monitoring != nil && cluster.Spec.Monitoring.Prometheus != nil {
		if cluster.Spec.Monitoring.Prometheus.Enabled {
			logrus.Infof("Prometheus is enabled")
			if cluster.Spec.Monitoring.Prometheus.AlertManager != nil && cluster.Spec.Monitoring.Prometheus.AlertManager.Enabled {
				logrus.Infof("AlertManager is enabled")
				return ValidateAlertManagerEnabled(pxImageList, cluster, timeout, interval)
			}
			logrus.Infof("AlertManager is not enabled")
			return ValidateAlertManagerDisabled(pxImageList, cluster, timeout, interval)
		}
	}

	logrus.Infof("AlertManager is disabled")
	return ValidateAlertManagerDisabled(pxImageList, cluster, timeout, interval)
}

// ValidateAlertManagerEnabled validates alert manager components are enabled/installed as expected
func ValidateAlertManagerEnabled(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	// Wait for the statefulset to become online
	sset := appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "alertmanager-portworx",
			Namespace: cluster.Namespace,
		},
	}
	if err := appops.Instance().ValidateStatefulSet(&sset, timeout); err != nil {
		return err
	}

	statefulSet, err := appops.Instance().GetStatefulSet(sset.Name, sset.Namespace)
	if err != nil {
		return err
	}

	// Verify alert manager services
	if _, err := coreops.Instance().GetService("alertmanager-portworx", cluster.Namespace); err != nil {
		return fmt.Errorf("failed to get service alertmanager-portworx")
	}

	if _, err := coreops.Instance().GetService("alertmanager-operated", cluster.Namespace); err != nil {
		return fmt.Errorf("failed to get service alertmanager-operated")
	}

	// Verify alert manager image
	imageName, ok := pxImageList["alertManager"]
	if !ok {
		return fmt.Errorf("failed to find image for alert manager")
	}

	imageName = util.GetImageURN(cluster, imageName)

	if statefulSet.Spec.Template.Spec.Containers[0].Image != imageName {
		return fmt.Errorf("alertmanager image mismatch, image: %s, expected: %s",
			statefulSet.Spec.Template.Spec.Containers[0].Image,
			imageName)
	}

	K8sVer1_22, _ := version.NewVersion("1.22")
	kubeVersion, _, err := GetFullVersion()
	if err != nil {
		return err
	}

	// NOTE: Prometheus uses different images for k8s 1.22 and up then for 1.21 and below
	if kubeVersion != nil && kubeVersion.GreaterThanOrEqual(K8sVer1_22) {
		imageName, ok = pxImageList["prometheusConfigReloader"]
		if !ok {
			return fmt.Errorf("failed to find image for prometheus config reloader")
		}
	} else {
		imageName, ok = pxImageList["prometheusConfigMapReload"]
		if !ok {
			return fmt.Errorf("failed to find image for prometheus configmap reloader")
		}
	}

	imageName = util.GetImageURN(cluster, imageName)
	if statefulSet.Spec.Template.Spec.Containers[1].Image != imageName {
		return fmt.Errorf("config-reloader image mismatch, image: %s, expected: %s",
			statefulSet.Spec.Template.Spec.Containers[1].Image,
			imageName)
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

// ValidateTelemetryInstalled validates telemetry component is running as expected
func ValidateTelemetryInstalled(pxImageList map[string]string, cluster *corev1.StorageCluster, timeout, interval time.Duration) error {
	// Wait for the deployment to become online
	dep := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "px-metrics-collector",
			Namespace: cluster.Namespace,
		},
	}
	if err := appops.Instance().ValidateDeployment(&dep, timeout, interval); err != nil {
		return err
	}

	/* TODO: We need to make this work for spawn
	expectedDeployment := GetExpectedDeployment(&testing.T{}, "metricsCollectorDeployment.yaml")
	*/

	deployment, err := appops.Instance().GetDeployment(dep.Name, dep.Namespace)
	if err != nil {
		return err
	}

	/* TODO: We need to make this work for spawn
	if equal, err := util.DeploymentDeepEqual(expectedDeployment, deployment); !equal {
		return err
	}
	*/

	_, err = rbacops.Instance().GetRole("px-metrics-collector", cluster.Namespace)
	if err != nil {
		return err
	}

	_, err = rbacops.Instance().GetRoleBinding("px-metrics-collector", cluster.Namespace)
	if err != nil {
		return err
	}

	// Verify telemetry config map
	_, err = coreops.Instance().GetConfigMap("px-telemetry-config", cluster.Namespace)
	if err != nil {
		return err
	}

	// Verify collector config map
	_, err = coreops.Instance().GetConfigMap("px-collector-config", cluster.Namespace)
	if err != nil {
		return err
	}

	// Verify collector proxy config map
	_, err = coreops.Instance().GetConfigMap("px-collector-proxy-config", cluster.Namespace)
	if err != nil {
		return err
	}

	// Verify collector service account
	_, err = coreops.Instance().GetServiceAccount("px-metrics-collector", cluster.Namespace)
	if err != nil {
		return err
	}

	// Verify metrics collector image
	imageName, ok := pxImageList["metricsCollector"]
	if !ok {
		return fmt.Errorf("failed to find image for metrics collector")
	}

	imageName = util.GetImageURN(cluster, imageName)

	if deployment.Spec.Template.Spec.Containers[0].Image != imageName {
		return fmt.Errorf("collector image mismatch, image: %s, expected: %s",
			deployment.Spec.Template.Spec.Containers[0].Image,
			imageName)
	}

	// Verify metrics collector proxy image
	imageName, ok = pxImageList["metricsCollectorProxy"]
	if !ok {
		return fmt.Errorf("failed to find image for metrics collector proxy")
	}

	imageName = util.GetImageURN(cluster, imageName)
	if deployment.Spec.Template.Spec.Containers[1].Image != imageName {
		return fmt.Errorf("collector proxy image mismatch, image: %s, expected: %s",
			deployment.Spec.Template.Spec.Containers[1].Image,
			imageName)
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
			if ev.LastTimestamp.Before(&v1Time) {
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
