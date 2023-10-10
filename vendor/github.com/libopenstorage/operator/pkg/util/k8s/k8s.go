package k8s

import (
	"context"
	"fmt"
	"os"
	"path"
	"reflect"
	"regexp"
	"strconv"

	"github.com/hashicorp/go-version"
	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/libopenstorage/operator/pkg/constants"
	consolev1 "github.com/openshift/api/console/v1"
	apiextensionsops "github.com/portworx/sched-ops/k8s/apiextensions"
	coreops "github.com/portworx/sched-ops/k8s/core"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Constants for k8s object kinds
const (
	Pod     = "pod"
	Service = "service"

	NodeRoleLabelMaster       = "node-role.kubernetes.io/master"
	NodeRoleLabelControlPlane = "node-role.kubernetes.io/control-plane"
	NodeRoleLabelInfra        = "node-role.kubernetes.io/infra"
	NodeRoleLabelWorker       = "node-role.kubernetes.io/worker"
	// UpdateRevisionConflictErr contains controller-runtime update error message when updating stale objects
	UpdateRevisionConflictErr = "the object has been modified; please apply your changes to the latest version and try again"
	DefaultK8SRegistryPath    = "registry.k8s.io"
)

var (
	kbVerRegex = regexp.MustCompile(`^(v\d+\.\d+\.\d+)(.*)`)
	// K8sVer1_13 k8s 1.13
	K8sVer1_13, _ = version.NewVersion("1.13")
	// K8sVer1_17 k8s 1.17
	K8sVer1_17, _ = version.NewVersion("1.17")
	// K8sVer1_18 k8s 1.18
	K8sVer1_18, _ = version.NewVersion("1.18")
	// K8sVer1_20 k8s 1.20
	K8sVer1_20, _ = version.NewVersion("1.20")
	// K8sVer1_22 k8s 1.22
	K8sVer1_22, _ = version.NewVersion("1.22")
	// K8sVer1_24 k8s 1.24
	K8sVer1_24, _ = version.NewVersion("1.24")
	// K8sVer1_25 k8s 1.25
	K8sVer1_25, _ = version.NewVersion("1.25")
	// K8sVer1_26 k8s 1.26
	K8sVer1_26, _ = version.NewVersion("1.26")
)

// NewK8sClient returns a new controller runtime Kubernetes client
func NewK8sClient(scheme *runtime.Scheme) (client.Client, error) {
	var err error
	var config *rest.Config

	kubeconfig := os.Getenv("KUBECONFIG")
	if len(kubeconfig) > 0 {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("error getting Kubernetes config. %v", err)
	}

	clientOptions := client.Options{
		Scheme: scheme,
	}
	cl, err := client.New(config, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("error creating a new Kubernetes client. %v", err)
	}
	return cl, nil
}

// GetVersion returns the kubernetes server version
func GetVersion() (*version.Version, error) {
	ver, _, err := GetFullVersion()
	return ver, err
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

// IsNewKubernetesRegistry returns true if the kubernetes images for the
// given k8s version are stored in a newer container registry
func IsNewKubernetesRegistry(k8sVersion *version.Version) bool {
	ver1_16_13, _ := version.NewVersion("1.16.13")
	ver1_17, _ := version.NewVersion("1.17")
	ver1_17_9, _ := version.NewVersion("1.17.9")
	ver1_18, _ := version.NewVersion("1.18")
	ver1_18_6, _ := version.NewVersion("1.18.6")

	return k8sVersion.GreaterThan(ver1_18_6) ||
		(k8sVersion.GreaterThan(ver1_16_13) && k8sVersion.LessThan(ver1_17)) ||
		(k8sVersion.GreaterThan(ver1_17_9) && k8sVersion.LessThan(ver1_18))
}

// ParseObjectFromFile reads the given file and loads the object from the file in obj
func ParseObjectFromFile(
	filepath string,
	scheme *runtime.Scheme,
	obj runtime.Object,
) error {
	objBytes, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}
	codecs := serializer.NewCodecFactory(scheme)
	_, _, err = codecs.UniversalDeserializer().Decode([]byte(objBytes), nil, obj)
	return err
}

// CreateOrUpdateCRD creates a crd if not present or update it
func CreateOrUpdateCRD(
	crd *apiextensionsv1.CustomResourceDefinition,
) error {
	deployedCRD, err := apiextensionsops.Instance().GetCRD(crd.Name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return apiextensionsops.Instance().RegisterCRD(crd)
	} else if err != nil {
		return err
	}

	// After CRD is deployed, some new fields may be added, hence we use DeepDerivative instead of DeepEqual.
	// For example, following section will be added to prometheus CRD.
	//   conversion:
	//    strategy: None
	// We only compare spec, as other fields will change after it's deployed, for example: creationTimestamp: null will
	// be changed to a real timestamp.
	if !equality.Semantic.DeepDerivative(crd.Spec, deployedCRD.Spec) {
		// if !reflect.DeepEqual(crd.Spec, deployedCRD.Spec) {
		logrus.Infof("Updating CRD %s", crd.Name)
		crd.ResourceVersion = deployedCRD.ResourceVersion
		if _, err = apiextensionsops.Instance().UpdateCRD(crd); err != nil {
			return err
		}
	}

	return nil
}

// CreateCRD creates a crd if not present
func CreateCRD(
	crd *apiextensionsv1.CustomResourceDefinition,
) error {
	_, err := apiextensionsops.Instance().GetCRD(crd.Name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		if err = apiextensionsops.Instance().RegisterCRD(crd); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

// CreateOrUpdateServiceAccount creates a service account if not present,
// else updates it if it has changed
func CreateOrUpdateServiceAccount(
	k8sClient client.Client,
	sa *v1.ServiceAccount,
	ownerRef *metav1.OwnerReference,
) error {
	existingSA := &v1.ServiceAccount{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      sa.Name,
			Namespace: sa.Namespace,
		},
		existingSA,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s/%s ServiceAccount", sa.Namespace, sa.Name)
		return k8sClient.Create(context.TODO(), sa)
	} else if err != nil {
		return err
	}

	for _, o := range existingSA.OwnerReferences {
		if ownerRef != nil && o.UID != ownerRef.UID {
			sa.OwnerReferences = append(sa.OwnerReferences, o)
		}
	}

	if len(sa.OwnerReferences) > len(existingSA.OwnerReferences) {
		logrus.Infof("Updating %s/%s ServiceAccount", sa.Namespace, sa.Name)
		return k8sClient.Update(context.TODO(), sa)
	}
	return nil
}

// DeleteServiceAccount deletes a service account if present and owned
func DeleteServiceAccount(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	serviceAccount := &v1.ServiceAccount{}
	err := k8sClient.Get(context.TODO(), resource, serviceAccount)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(serviceAccount.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(serviceAccount.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(serviceAccount.OwnerReferences) > 0 && len(serviceAccount.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete ServiceAccount %s/%s as it is not owned",
			namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s ServiceAccount", namespace, name)
		return k8sClient.Delete(context.TODO(), serviceAccount)
	}
	serviceAccount.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s ServiceAccount", namespace, name)
	return k8sClient.Update(context.TODO(), serviceAccount)
}

// CreateOrUpdateRole creates a role if not present,
// else updates it if it has changed
func CreateOrUpdateRole(
	k8sClient client.Client,
	role *rbacv1.Role,
	ownerRef *metav1.OwnerReference,
) error {
	existingRole := &rbacv1.Role{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      role.Name,
			Namespace: role.Namespace,
		},
		existingRole,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s/%s Role", role.Namespace, role.Name)
		return k8sClient.Create(context.TODO(), role)
	} else if err != nil {
		return err
	}

	modified := !reflect.DeepEqual(role.Rules, existingRole.Rules)

	for _, o := range existingRole.OwnerReferences {
		if ownerRef != nil && o.UID != ownerRef.UID {
			role.OwnerReferences = append(role.OwnerReferences, o)
		}
	}

	if modified || len(role.OwnerReferences) > len(existingRole.OwnerReferences) {
		logrus.Infof("Updating %s/%s Role", role.Namespace, role.Name)
		return k8sClient.Update(context.TODO(), role)
	}
	return nil
}

// DeleteRole deletes a role if present and owned
func DeleteRole(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	role := &rbacv1.Role{}
	err := k8sClient.Get(context.TODO(), resource, role)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(role.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(role.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(role.OwnerReferences) > 0 && len(role.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete Role %s/%s as it is not owned",
			namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s Role", namespace, name)
		return k8sClient.Delete(context.TODO(), role)
	}
	role.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s Role", namespace, name)
	return k8sClient.Update(context.TODO(), role)
}

// CreateOrUpdateRoleBinding creates a role binding if not present,
// else updates it if it has changed
func CreateOrUpdateRoleBinding(
	k8sClient client.Client,
	rb *rbacv1.RoleBinding,
	ownerRef *metav1.OwnerReference,
) error {
	existingRB := &rbacv1.RoleBinding{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      rb.Name,
			Namespace: rb.Namespace,
		},
		existingRB,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s/%s RoleBinding", rb.Namespace, rb.Name)
		return k8sClient.Create(context.TODO(), rb)
	} else if err != nil {
		return err
	}

	modified := !reflect.DeepEqual(rb.Subjects, existingRB.Subjects) ||
		!reflect.DeepEqual(rb.RoleRef, existingRB.RoleRef)

	for _, o := range existingRB.OwnerReferences {
		if ownerRef != nil && o.UID != ownerRef.UID {
			rb.OwnerReferences = append(rb.OwnerReferences, o)
		}
	}

	if modified || len(rb.OwnerReferences) > len(existingRB.OwnerReferences) {
		rb.ResourceVersion = existingRB.ResourceVersion
		logrus.Infof("Updating %s/%s RoleBinding", rb.Namespace, rb.Name)
		return k8sClient.Update(context.TODO(), rb)
	}
	return nil
}

// DeleteRoleBinding deletes a role binding if present and owned
func DeleteRoleBinding(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	roleBinding := &rbacv1.RoleBinding{}
	err := k8sClient.Get(context.TODO(), resource, roleBinding)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(roleBinding.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(roleBinding.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(roleBinding.OwnerReferences) > 0 && len(roleBinding.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete RoleBinding %s/%s as it is not owned",
			namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s RoleBinding", namespace, name)
		return k8sClient.Delete(context.TODO(), roleBinding)
	}
	roleBinding.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s RoleBinding", namespace, name)
	return k8sClient.Update(context.TODO(), roleBinding)
}

// CreateOrUpdateClusterRole creates a cluster role if not present,
// else updates it if it has changed
func CreateOrUpdateClusterRole(
	k8sClient client.Client,
	cr *rbacv1.ClusterRole,
) error {
	existingCR := &rbacv1.ClusterRole{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{Name: cr.Name},
		existingCR,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s ClusterRole", cr.Name)
		return k8sClient.Create(context.TODO(), cr)
	} else if err != nil {
		return err
	}

	// Cluster scoped objects should not have owner references
	if !reflect.DeepEqual(cr.Rules, existingCR.Rules) ||
		len(existingCR.OwnerReferences) > 0 {
		existingCR.Rules = cr.DeepCopy().Rules
		existingCR.OwnerReferences = nil
		logrus.Infof("Updating %s ClusterRole", existingCR.Name)
		return k8sClient.Update(context.TODO(), existingCR)
	}
	return nil
}

// DeleteClusterRole deletes a cluster role if present
func DeleteClusterRole(
	k8sClient client.Client,
	name string,
) error {
	resource := types.NamespacedName{
		Name: name,
	}
	clusterRole := &rbacv1.ClusterRole{}
	err := k8sClient.Get(context.TODO(), resource, clusterRole)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	// Do not delete the object if it has an owner
	if len(clusterRole.OwnerReferences) > 0 {
		logrus.Infof("Cannot delete ClusterRole %s as it is owned", name)
		return nil
	}

	logrus.Infof("Deleting %s ClusterRole", name)
	return k8sClient.Delete(context.TODO(), clusterRole)
}

// CreateOrUpdateClusterRoleBinding creates a cluster role binding if not present,
// else updates it if it has changed
func CreateOrUpdateClusterRoleBinding(
	k8sClient client.Client,
	crb *rbacv1.ClusterRoleBinding,
) error {
	existingCRB := &rbacv1.ClusterRoleBinding{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{Name: crb.Name},
		existingCRB,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s ClusterRoleBinding", crb.Name)
		return k8sClient.Create(context.TODO(), crb)
	} else if err != nil {
		return err
	}

	modified := !reflect.DeepEqual(crb.Subjects, existingCRB.Subjects) ||
		!reflect.DeepEqual(crb.RoleRef, existingCRB.RoleRef)

	// Cluster scoped objects should not have owner references
	if modified || len(existingCRB.OwnerReferences) > 0 {
		copy := crb.DeepCopy()
		existingCRB.Subjects = copy.Subjects
		existingCRB.RoleRef = copy.RoleRef
		existingCRB.OwnerReferences = nil
		logrus.Infof("Updating %s ClusterRoleBinding", existingCRB.Name)
		return k8sClient.Update(context.TODO(), existingCRB)
	}
	return nil
}

// DeleteClusterRoleBinding deletes a cluster role binding if present
func DeleteClusterRoleBinding(
	k8sClient client.Client,
	name string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name: name,
	}
	crb := &rbacv1.ClusterRoleBinding{}
	err := k8sClient.Get(context.TODO(), resource, crb)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	// Do not delete the object if it has an owner
	if len(crb.OwnerReferences) > 0 {
		logrus.Infof("Cannot delete ClusterRoleBinding %s as it is owned", name)
		return nil
	}

	logrus.Infof("Deleting %s ClusterRoleBinding", name)
	return k8sClient.Delete(context.TODO(), crb)
}

// CreateOrUpdateConfigMap creates a config map if not present, else updates it if it has changed
// returns whether data is modified
func CreateOrUpdateConfigMap(
	k8sClient client.Client,
	configMap *v1.ConfigMap,
	ownerRef *metav1.OwnerReference,
) (bool, error) {
	existingConfigMap := &v1.ConfigMap{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      configMap.Name,
			Namespace: configMap.Namespace,
		},
		existingConfigMap,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %v ConfigMap", configMap.Name)
		return false, k8sClient.Create(context.TODO(), configMap)
	} else if err != nil {
		return false, err
	}

	enabled, err := strconv.ParseBool(existingConfigMap.Annotations[constants.AnnotationReconcileObject])
	if err == nil && !enabled {
		return false, nil
	}

	modified := !reflect.DeepEqual(configMap.Data, existingConfigMap.Data) ||
		!reflect.DeepEqual(configMap.BinaryData, existingConfigMap.BinaryData)

	for _, o := range existingConfigMap.OwnerReferences {
		if o.UID != ownerRef.UID {
			configMap.OwnerReferences = append(configMap.OwnerReferences, o)
		}
	}

	if modified || len(configMap.OwnerReferences) > len(existingConfigMap.OwnerReferences) {
		logrus.Infof("Updating %v ConfigMap", configMap.Name)
		return modified, k8sClient.Update(context.TODO(), configMap)
	}
	return false, nil
}

// DeleteConfigMap deletes a config map if present and owned
func DeleteConfigMap(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	configMap := &v1.ConfigMap{}
	err := k8sClient.Get(context.TODO(), resource, configMap)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(configMap.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(configMap.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(configMap.OwnerReferences) > 0 && len(configMap.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete ConfigMap %s/%s as it is not owned",
			namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s ConfigMap", namespace, name)
		return k8sClient.Delete(context.TODO(), configMap)
	}
	configMap.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s ConfigMap", namespace, name)
	return k8sClient.Update(context.TODO(), configMap)
}

// CreateStorageClass creates a storage class only if not present.
// It will not return error if already present.
func CreateStorageClass(
	k8sClient client.Client,
	sc *storagev1.StorageClass,
) error {
	existingSC := &storagev1.StorageClass{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{Name: sc.Name},
		existingSC,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s StorageClass", sc.Name)
		return k8sClient.Create(context.TODO(), sc)
	} else if err == nil && len(existingSC.OwnerReferences) > 0 {
		// Cluster scoped objects should not have owner references
		logrus.Infof("Updating %s StorageClass", sc.Name)
		existingSC.OwnerReferences = nil
		return k8sClient.Update(context.TODO(), existingSC)
	}
	return err
}

// DeleteStorageClass deletes a storage class if present
func DeleteStorageClass(
	k8sClient client.Client,
	name string,
) error {
	resource := types.NamespacedName{
		Name: name,
	}
	storageClass := &storagev1.StorageClass{}
	err := k8sClient.Get(context.TODO(), resource, storageClass)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	// Do not delete the object if it has an owner
	if len(storageClass.OwnerReferences) > 0 {
		logrus.Infof("Cannot delete StorageClass %s as it is owned", name)
		return nil
	}

	logrus.Infof("Deleting %s StorageClass", name)
	return k8sClient.Delete(context.TODO(), storageClass)
}

// CreateOrUpdateCSIDriver creates a CSIDriver if not present,
// else updates it if it has changed
func CreateOrUpdateCSIDriver(
	k8sClient client.Client,
	driver *storagev1.CSIDriver,
) error {
	existingDriver := &storagev1.CSIDriver{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{Name: driver.Name},
		existingDriver,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s CSIDriver", driver.Name)
		return k8sClient.Create(context.TODO(), driver)
	} else if err != nil {
		return err
	}

	// Copy the fields that are automatically populated by k8s
	driver.Spec.FSGroupPolicy = existingDriver.Spec.FSGroupPolicy
	driver.Spec.RequiresRepublish = existingDriver.Spec.RequiresRepublish
	driver.Spec.StorageCapacity = existingDriver.Spec.StorageCapacity

	// Cluster scoped objects should not have owner references
	if !reflect.DeepEqual(driver.Spec, existingDriver.Spec) ||
		len(existingDriver.OwnerReferences) > 0 {
		existingDriver.Spec = *driver.Spec.DeepCopy()
		existingDriver.OwnerReferences = nil

		logrus.Infof("Updating %s CSIDriver", existingDriver.Name)
		err := k8sClient.Update(context.TODO(), existingDriver, &client.UpdateOptions{})
		if err != nil {
			logrus.Infof("Update failed: %s, re-creating %s CSIDriver", err.Error(), existingDriver.Name)
			err = k8sClient.Delete(context.TODO(), existingDriver)
			if err != nil {
				return err
			}

			driver.ResourceVersion = ""
			return k8sClient.Create(context.TODO(), driver)
		}
	}
	return nil
}

// CreateOrUpdateCSIDriverBeta creates a CSIDriver if not present,
// else updates it if it has changed
func CreateOrUpdateCSIDriverBeta(
	k8sClient client.Client,
	driver *storagev1beta1.CSIDriver,
) error {
	existingDriver := &storagev1beta1.CSIDriver{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{Name: driver.Name},
		existingDriver,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s CSIDriver", driver.Name)
		return k8sClient.Create(context.TODO(), driver)
	} else if err != nil {
		return err
	}

	// Cluster scoped objects should not have owner references
	if !reflect.DeepEqual(driver.Spec, existingDriver.Spec) ||
		len(existingDriver.OwnerReferences) > 0 {
		existingDriver.Spec = *driver.Spec.DeepCopy()
		existingDriver.OwnerReferences = nil

		logrus.Infof("Updating %s CSIDriver", existingDriver.Name)
		err := k8sClient.Update(context.TODO(), existingDriver, &client.UpdateOptions{})
		if err != nil {
			logrus.Infof("Update failed: %s, re-creating %s CSIDriver", err.Error(), existingDriver.Name)
			err = k8sClient.Delete(context.TODO(), existingDriver)
			if err != nil {
				return err
			}

			driver.ResourceVersion = ""
			return k8sClient.Create(context.TODO(), driver)
		}
	}
	return nil
}

// DeleteCSIDriver deletes the CSIDriver beta object if present
func DeleteCSIDriver(
	k8sClient client.Client,
	name string,
) error {
	resource := types.NamespacedName{
		Name: name,
	}

	csiDriver := &storagev1.CSIDriver{}
	err := k8sClient.Get(context.TODO(), resource, csiDriver)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	// Do not delete the object if it has an owner
	if len(csiDriver.OwnerReferences) > 0 {
		logrus.Infof("Cannot delete CSIDriver %s as it is owned", name)
		return nil
	}

	logrus.Infof("Deleting %s CSIDriver", name)
	return k8sClient.Delete(context.TODO(), csiDriver)
}

// DeleteCSIDriverBeta deletes the CSIDriver beta object if present
func DeleteCSIDriverBeta(
	k8sClient client.Client,
	name string,
) error {
	resource := types.NamespacedName{
		Name: name,
	}

	csiDriver := &storagev1beta1.CSIDriver{}
	err := k8sClient.Get(context.TODO(), resource, csiDriver)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	// Do not delete the object if it has an owner
	if len(csiDriver.OwnerReferences) > 0 {
		logrus.Infof("Cannot delete CSIDriver %s as it is owned", name)
		return nil
	}

	logrus.Infof("Deleting %s CSIDriver", name)
	return k8sClient.Delete(context.TODO(), csiDriver)
}

// CreateOrUpdateService creates a service if not present, else updates it
func CreateOrUpdateService(
	k8sClient client.Client,
	service *v1.Service,
	ownerRef *metav1.OwnerReference,
) error {
	existingService := &v1.Service{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      service.Name,
			Namespace: service.Namespace,
		},
		existingService,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s/%s Service", service.Namespace, service.Name)
		return k8sClient.Create(context.TODO(), service)
	} else if err != nil {
		return err
	}

	ownerRefs := make([]metav1.OwnerReference, 0)
	if ownerRef != nil {
		ownerRefs = append(ownerRefs, *ownerRef)
		for _, o := range existingService.OwnerReferences {
			if o.UID != ownerRef.UID {
				ownerRefs = append(ownerRefs, o)
			}
		}
	}

	if service.Spec.Type == "" {
		service.Spec.Type = v1.ServiceTypeClusterIP
	}

	modified := existingService.Spec.Type != service.Spec.Type ||
		!reflect.DeepEqual(existingService.Labels, service.Labels) ||
		!reflect.DeepEqual(existingService.Spec.Selector, service.Spec.Selector) ||
		!reflect.DeepEqual(existingService.Annotations, service.Annotations)

	portMapping := make(map[string]v1.ServicePort)
	for _, port := range service.Spec.Ports {
		portMapping[port.Name] = port
	}

	servicePorts := make([]v1.ServicePort, 0)
	for _, existingPort := range existingService.Spec.Ports {
		newPort, exists := portMapping[existingPort.Name]
		if !exists {
			// The port is no longer present in the new ports list
			modified = true
			continue
		}
		if existingPort.Protocol == "" {
			existingPort.Protocol = v1.ProtocolTCP
		}
		if newPort.Protocol == "" {
			newPort.Protocol = v1.ProtocolTCP
		}
		if existingPort.Port != newPort.Port ||
			!reflect.DeepEqual(existingPort.TargetPort, newPort.TargetPort) ||
			existingPort.Protocol != newPort.Protocol {
			modified = true
		}
		toUpdate := existingPort.DeepCopy()
		toUpdate.Port = newPort.Port
		toUpdate.TargetPort = newPort.TargetPort
		toUpdate.Protocol = newPort.Protocol
		if service.Spec.Type != v1.ServiceTypeLoadBalancer &&
			service.Spec.Type != v1.ServiceTypeNodePort {
			toUpdate.NodePort = int32(0)
		}
		servicePorts = append(servicePorts, *toUpdate)
		delete(portMapping, newPort.Name)
	}

	// Copy new ports that were not present in the service before
	for _, port := range portMapping {
		modified = true
		toUpdate := port.DeepCopy()
		servicePorts = append(servicePorts, *toUpdate)
	}

	if modified || len(ownerRefs) > len(existingService.OwnerReferences) {
		existingService.OwnerReferences = ownerRefs
		existingService.Labels = service.Labels
		existingService.Spec.Selector = service.Spec.Selector
		existingService.Spec.Type = service.Spec.Type
		existingService.Spec.Ports = servicePorts
		existingService.Annotations = service.Annotations
		logrus.Infof("Updating %s/%s Service", service.Namespace, service.Name)
		return k8sClient.Update(context.TODO(), existingService)
	}
	return nil
}

// DeleteService deletes a service if present and owned
func DeleteService(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	service := &v1.Service{}
	err := k8sClient.Get(context.TODO(), resource, service)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(service.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(service.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(service.OwnerReferences) > 0 && len(service.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete Service %s/%s as it is not owned",
			namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s Service", namespace, name)
		return k8sClient.Delete(context.TODO(), service)
	}
	service.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s Service", namespace, name)
	return k8sClient.Update(context.TODO(), service)
}

// GetDeployment get deployment
func GetDeployment(
	k8sClient client.Client,
	name string,
	namespace string,
	deployment *appsv1.Deployment,
) error {
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		},
		deployment,
	)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	return nil
}

// CreateOrUpdateDeployment creates a deployment if not present, else updates it
func CreateOrUpdateDeployment(
	k8sClient client.Client,
	deployment *appsv1.Deployment,
	ownerRef *metav1.OwnerReference,
) error {
	existingDeployment := &appsv1.Deployment{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      deployment.Name,
			Namespace: deployment.Namespace,
		},
		existingDeployment,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s Deployment", deployment.Name)
		return k8sClient.Create(context.TODO(), deployment)
	} else if err != nil {
		return err
	}

	for _, o := range existingDeployment.OwnerReferences {
		if o.UID != ownerRef.UID {
			deployment.OwnerReferences = append(deployment.OwnerReferences, o)
		}
	}

	logrus.Debugf("Updating %s Deployment", deployment.Name)
	return k8sClient.Update(context.TODO(), deployment)
}

// DeleteDeployment deletes a deployment if present and owned
func DeleteDeployment(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	deployment := &appsv1.Deployment{}
	err := k8sClient.Get(context.TODO(), resource, deployment)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(deployment.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(deployment.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(deployment.OwnerReferences) > 0 && len(deployment.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete Deployment %s/%s as it is not owned",
			namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s Deployment", namespace, name)
		return k8sClient.Delete(context.TODO(), deployment)
	}
	deployment.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s Deployment", namespace, name)
	return k8sClient.Update(context.TODO(), deployment)
}

// CreateOrUpdateStatefulSet creates a stateful set if not present, else updates it
func CreateOrUpdateStatefulSet(
	k8sClient client.Client,
	ss *appsv1.StatefulSet,
	ownerRef *metav1.OwnerReference,
) error {
	existingSS := &appsv1.StatefulSet{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      ss.Name,
			Namespace: ss.Namespace,
		},
		existingSS,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s StatefulSet", ss.Name)
		return k8sClient.Create(context.TODO(), ss)
	} else if err != nil {
		return err
	}

	for _, o := range existingSS.OwnerReferences {
		if o.UID != ownerRef.UID {
			ss.OwnerReferences = append(ss.OwnerReferences, o)
		}
	}

	logrus.Debugf("Updating %s StatefulSet", ss.Name)
	return k8sClient.Update(context.TODO(), ss)
}

// DeleteStatefulSet deletes a stateful set if present and owned
func DeleteStatefulSet(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	statefulSet := &appsv1.StatefulSet{}
	err := k8sClient.Get(context.TODO(), resource, statefulSet)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(statefulSet.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(statefulSet.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(statefulSet.OwnerReferences) > 0 && len(statefulSet.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete StatefulSet %s/%s as it is not owned",
			namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s StatefulSet", namespace, name)
		return k8sClient.Delete(context.TODO(), statefulSet)
	}
	statefulSet.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s StatefulSet", namespace, name)
	return k8sClient.Update(context.TODO(), statefulSet)
}

// GetDaemonSet get daemonset
func GetDaemonSet(
	k8sClient client.Client,
	name string,
	namespace string,
	daemonset *appsv1.DaemonSet,
) error {
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		},
		daemonset,
	)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	return nil
}

// CreateOrUpdateDaemonSet creates a daemon set if not present, else updates it
func CreateOrUpdateDaemonSet(
	k8sClient client.Client,
	ds *appsv1.DaemonSet,
	ownerRef *metav1.OwnerReference,
) error {
	existingDS := &appsv1.DaemonSet{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      ds.Name,
			Namespace: ds.Namespace,
		},
		existingDS,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s/%s DaemonSet", ds.Namespace, ds.Name)
		return k8sClient.Create(context.TODO(), ds)
	} else if err != nil {
		return err
	}

	if ownerRef != nil {
		ownerRefPresent := false
		for _, o := range existingDS.OwnerReferences {
			if o.UID == ownerRef.UID {
				ownerRefPresent = true
				break
			}
		}
		if !ownerRefPresent {
			if existingDS.OwnerReferences == nil {
				existingDS.OwnerReferences = make([]metav1.OwnerReference, 0)
			}
			existingDS.OwnerReferences = append(existingDS.OwnerReferences, *ownerRef)
		}
	}

	existingDS.Labels = ds.Labels
	existingDS.Spec = ds.Spec

	logrus.Debugf("Updating %s/%s DaemonSet", ds.Namespace, ds.Name)
	return k8sClient.Update(context.TODO(), existingDS)
}

// DeleteDaemonSet deletes a DaemonSet if present and owned
func DeleteDaemonSet(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	ds := &appsv1.DaemonSet{}
	err := k8sClient.Get(context.TODO(), resource, ds)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(ds.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(ds.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(ds.OwnerReferences) > 0 && len(ds.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete DaemonSet %s/%s as it is not owned",
			namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s DaemonSet", namespace, name)
		return k8sClient.Delete(context.TODO(), ds)
	}
	ds.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s DaemonSet", namespace, name)
	return k8sClient.Update(context.TODO(), ds)
}

// UpdateStorageCluster updates given StorageCluster object on the latest copy
func UpdateStorageCluster(
	k8sClient client.Client,
	cluster *corev1.StorageCluster,
) error {
	existingCluster := &corev1.StorageCluster{}
	if err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
		existingCluster,
	); err != nil {
		return err
	}

	cluster.ResourceVersion = existingCluster.ResourceVersion
	if !reflect.DeepEqual(cluster, existingCluster) {
		return k8sClient.Update(context.TODO(), cluster)
	}
	return nil
}

// UpdateStorageClusterStatus updates the status of given StorageCluster object on the latest copy
func UpdateStorageClusterStatus(
	k8sClient client.Client,
	cluster *corev1.StorageCluster,
) error {
	existingCluster := &corev1.StorageCluster{}
	if err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
		existingCluster,
	); err != nil {
		return err
	}

	cluster.ResourceVersion = existingCluster.ResourceVersion
	if !reflect.DeepEqual(cluster.Status, existingCluster.Status) {
		return k8sClient.Status().Update(context.TODO(), cluster)
	}
	return nil
}

// CreateOrUpdateStorageNode creates a StorageNode if not present, else updates it
func CreateOrUpdateStorageNode(
	k8sClient client.Client,
	node *corev1.StorageNode,
	ownerRef *metav1.OwnerReference,
) error {
	existingNode := &corev1.StorageNode{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      node.Name,
			Namespace: node.Namespace,
		},
		existingNode,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating StorageNode %s/%s", node.Namespace, node.Name)
		return k8sClient.Create(context.TODO(), node)
	} else if err != nil {
		return err
	}

	for _, o := range existingNode.OwnerReferences {
		if o.UID != ownerRef.UID {
			node.OwnerReferences = append(node.OwnerReferences, o)
		}
	}

	modified := !reflect.DeepEqual(node.Status, existingNode.Status) ||
		!reflect.DeepEqual(node.Spec, existingNode.Spec)

	if modified || len(node.OwnerReferences) > len(existingNode.OwnerReferences) {
		// Create a copy of node as Update() will change the object,
		// but we need the original to update the status
		nodeStatus := node.DeepCopy()
		node.ResourceVersion = existingNode.ResourceVersion
		logrus.Infof("Updating StorageNode %s/%s", node.Namespace, node.Name)
		if err := k8sClient.Update(context.TODO(), node); err != nil {
			return err
		}
		nodeStatus.ResourceVersion = node.ResourceVersion
		return k8sClient.Status().Update(context.TODO(), nodeStatus)
	}
	return nil
}

// CreateOrUpdateServiceMonitor creates a ServiceMonitor if not present, else updates it
func CreateOrUpdateServiceMonitor(
	k8sClient client.Client,
	monitor *monitoringv1.ServiceMonitor,
	ownerRef *metav1.OwnerReference,
) error {
	existingMonitor := &monitoringv1.ServiceMonitor{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      monitor.Name,
			Namespace: monitor.Namespace,
		},
		existingMonitor,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating ServiceMonitor %s/%s", monitor.Namespace, monitor.Name)
		return k8sClient.Create(context.TODO(), monitor)
	} else if err != nil {
		return err
	}

	modified := !reflect.DeepEqual(monitor.Spec, existingMonitor.Spec)

	for _, o := range existingMonitor.OwnerReferences {
		if o.UID != ownerRef.UID {
			monitor.OwnerReferences = append(monitor.OwnerReferences, o)
		}
	}

	if modified || len(monitor.OwnerReferences) > len(existingMonitor.OwnerReferences) {
		monitor.ResourceVersion = existingMonitor.ResourceVersion
		logrus.Infof("Updating ServiceMonitor %s/%s", monitor.Namespace, monitor.Name)
		return k8sClient.Update(context.TODO(), monitor)
	}
	return nil
}

// DeleteServiceMonitor deletes a ServiceMonitor if present and owned
func DeleteServiceMonitor(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	monitor := &monitoringv1.ServiceMonitor{}
	err := k8sClient.Get(context.TODO(), resource, monitor)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(monitor.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(monitor.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(monitor.OwnerReferences) > 0 && len(monitor.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete ServiceMonitor %s/%s as it is not owned", namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s ServiceMonitor", namespace, name)
		return k8sClient.Delete(context.TODO(), monitor)
	}
	monitor.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s ServiceMonitor", namespace, name)
	return k8sClient.Update(context.TODO(), monitor)
}

// CreateOrUpdatePrometheusRule creates a PrometheusRule if not present, else updates it
func CreateOrUpdatePrometheusRule(
	k8sClient client.Client,
	rule *monitoringv1.PrometheusRule,
	ownerRef *metav1.OwnerReference,
) error {
	existingRule := &monitoringv1.PrometheusRule{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      rule.Name,
			Namespace: rule.Namespace,
		},
		existingRule,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating PrometheusRule %s/%s", rule.Namespace, rule.Name)
		return k8sClient.Create(context.TODO(), rule)
	} else if err != nil {
		return err
	}

	enabled, err := strconv.ParseBool(existingRule.ObjectMeta.Annotations[constants.AnnotationReconcileObject])
	if err == nil && !enabled {
		return nil
	}

	modified := !reflect.DeepEqual(rule.Spec, existingRule.Spec)

	for _, o := range existingRule.OwnerReferences {
		if o.UID != ownerRef.UID {
			rule.OwnerReferences = append(rule.OwnerReferences, o)
		}
	}

	if modified || len(rule.OwnerReferences) > len(existingRule.OwnerReferences) {
		rule.ResourceVersion = existingRule.ResourceVersion
		logrus.Infof("Updating PrometheusRule %s/%s", rule.Namespace, rule.Name)
		return k8sClient.Update(context.TODO(), rule)
	}
	return nil
}

// DeletePrometheusRule deletes a PrometheusRule if present and owned
func DeletePrometheusRule(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	rule := &monitoringv1.PrometheusRule{}
	err := k8sClient.Get(context.TODO(), resource, rule)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(rule.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(rule.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(rule.OwnerReferences) > 0 && len(rule.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete PrometheusRule %s/%s as it is not owned", namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s PrometheusRule", namespace, name)
		return k8sClient.Delete(context.TODO(), rule)
	}
	rule.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s PrometheusRule", namespace, name)
	return k8sClient.Update(context.TODO(), rule)
}

// CreateOrUpdatePrometheus creates a Prometheus object if not present, else updates it
func CreateOrUpdatePrometheus(
	k8sClient client.Client,
	prometheus *monitoringv1.Prometheus,
	ownerRef *metav1.OwnerReference,
) error {
	existingPrometheus := &monitoringv1.Prometheus{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      prometheus.Name,
			Namespace: prometheus.Namespace,
		},
		existingPrometheus,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating Prometheus %s/%s", prometheus.Namespace, prometheus.Name)
		return k8sClient.Create(context.TODO(), prometheus)
	} else if err != nil {
		return err
	}

	modified := !reflect.DeepEqual(prometheus.Spec, existingPrometheus.Spec)

	for _, o := range existingPrometheus.OwnerReferences {
		if o.UID != ownerRef.UID {
			prometheus.OwnerReferences = append(prometheus.OwnerReferences, o)
		}
	}

	if prometheus.Spec.PodMetadata == nil {
		prometheus.Spec.PodMetadata = &monitoringv1.EmbeddedObjectMetadata{}
	}
	if prometheus.Spec.PodMetadata.Labels == nil {
		prometheus.Spec.PodMetadata.Labels = make(map[string]string)
	}
	prometheus.Spec.PodMetadata.Labels[constants.OperatorLabelManagedByKey] = constants.OperatorLabelManagedByValue
	if modified || len(prometheus.OwnerReferences) > len(existingPrometheus.OwnerReferences) {
		prometheus.ResourceVersion = existingPrometheus.ResourceVersion
		logrus.Infof("Updating Prometheus %s/%s", prometheus.Namespace, prometheus.Name)
		return k8sClient.Update(context.TODO(), prometheus)
	}
	return nil
}

// DeletePrometheus deletes a Prometheus instance if present and owned
func DeletePrometheus(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	prometheus := &monitoringv1.Prometheus{}
	err := k8sClient.Get(context.TODO(), resource, prometheus)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(prometheus.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(prometheus.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(prometheus.OwnerReferences) > 0 && len(prometheus.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete Prometheus %s/%s as it is not owned", namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s Prometheus", namespace, name)
		return k8sClient.Delete(context.TODO(), prometheus)
	}
	prometheus.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s Prometheus", namespace, name)
	return k8sClient.Update(context.TODO(), prometheus)
}

// CreateOrUpdateAlertManager creates a AlertManager object if not present, else updates it
func CreateOrUpdateAlertManager(
	k8sClient client.Client,
	alertManager *monitoringv1.Alertmanager,
	ownerRef *metav1.OwnerReference,
) error {
	existingAlertManager := &monitoringv1.Alertmanager{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      alertManager.Name,
			Namespace: alertManager.Namespace,
		},
		existingAlertManager,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating AlertManager %s/%s", alertManager.Namespace, alertManager.Name)
		return k8sClient.Create(context.TODO(), alertManager)
	} else if err != nil {
		return err
	}

	modified := !reflect.DeepEqual(alertManager.Spec, existingAlertManager.Spec)

	for _, o := range existingAlertManager.OwnerReferences {
		if o.UID != ownerRef.UID {
			alertManager.OwnerReferences = append(alertManager.OwnerReferences, o)
		}
	}

	if alertManager.Spec.PodMetadata == nil {
		alertManager.Spec.PodMetadata = &monitoringv1.EmbeddedObjectMetadata{}
	}
	if alertManager.Spec.PodMetadata.Labels == nil {
		alertManager.Spec.PodMetadata.Labels = make(map[string]string)
	}
	alertManager.Spec.PodMetadata.Labels[constants.OperatorLabelManagedByKey] = constants.OperatorLabelManagedByValue
	if modified || len(alertManager.OwnerReferences) > len(existingAlertManager.OwnerReferences) {
		alertManager.ResourceVersion = existingAlertManager.ResourceVersion
		logrus.Infof("Updating AlertManager %s/%s", alertManager.Namespace, alertManager.Name)
		return k8sClient.Update(context.TODO(), alertManager)
	}
	return nil
}

// DeleteAlertManager deletes a AlertManager instance if present and owned
func DeleteAlertManager(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	alertManager := &monitoringv1.Alertmanager{}
	err := k8sClient.Get(context.TODO(), resource, alertManager)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(alertManager.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(alertManager.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(alertManager.OwnerReferences) > 0 && len(alertManager.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete AlertManager %s/%s as it is not owned", namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s AlertManager", namespace, name)
		return k8sClient.Delete(context.TODO(), alertManager)
	}
	alertManager.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s AlertManager", namespace, name)
	return k8sClient.Update(context.TODO(), alertManager)
}

// CreateOrUpdatePodDisruptionBudget creates a PodDisruptionBudget object if not present, else updates it
func CreateOrUpdatePodDisruptionBudget(
	k8sClient client.Client,
	pdb *policyv1.PodDisruptionBudget,
	ownerRef *metav1.OwnerReference,
) error {
	existingPDB := &policyv1.PodDisruptionBudget{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      pdb.Name,
			Namespace: pdb.Namespace,
		},
		existingPDB,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating PodDisruptionBudget %s/%s", pdb.Namespace, pdb.Name)
		return k8sClient.Create(context.TODO(), pdb)
	} else if err != nil {
		return err
	}

	modified := !reflect.DeepEqual(pdb.Spec, existingPDB.Spec)

	for _, o := range existingPDB.OwnerReferences {
		if o.UID != ownerRef.UID {
			pdb.OwnerReferences = append(pdb.OwnerReferences, o)
		}
	}

	if modified || len(pdb.OwnerReferences) > len(existingPDB.OwnerReferences) {
		pdb.ResourceVersion = existingPDB.ResourceVersion
		logrus.Infof("Updating PodDisruptionBudget %s/%s", pdb.Namespace, pdb.Name)
		return k8sClient.Update(context.TODO(), pdb)
	}
	return nil
}

// DeletePodDisruptionBudget deletes a PodDisruptionBudget instance if present and owned
func DeletePodDisruptionBudget(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	pdb := &policyv1.PodDisruptionBudget{}
	err := k8sClient.Get(context.TODO(), resource, pdb)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(pdb.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(pdb.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(pdb.OwnerReferences) > 0 && len(pdb.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete PodDisruptionBudget %s/%s as it is not owned", namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s PodDisruptionBudget", namespace, name)
		return k8sClient.Delete(context.TODO(), pdb)
	}
	pdb.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s PodDisruptionBudget", namespace, name)
	return k8sClient.Update(context.TODO(), pdb)
}

// CreateOrUpdateConsolePlugin creates a ConsolePlougin instance of ConsolePlugin CRD if not present, else updates it
func CreateOrUpdateConsolePlugin(
	k8sClient client.Client,
	cp *consolev1.ConsolePlugin,
	ownerRef *metav1.OwnerReference,
) error {

	existingPlugin := &consolev1.ConsolePlugin{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      cp.Name,
			Namespace: cp.Namespace,
		},
		existingPlugin,
	)

	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s Consoleplugin", cp.Name)
		return k8sClient.Create(context.TODO(), cp)
	} else if err != nil {
		return err
	}

	modified := !reflect.DeepEqual(cp.Spec, existingPlugin.Spec)

	for _, o := range existingPlugin.OwnerReferences {
		if o.UID != ownerRef.UID {
			cp.OwnerReferences = append(cp.OwnerReferences, o)
		}
	}

	if modified || len(cp.OwnerReferences) > len(existingPlugin.OwnerReferences) {
		cp.ResourceVersion = existingPlugin.ResourceVersion
		logrus.Infof("Updating Console Plugin %s/%s", cp.Namespace, cp.Name)
		return k8sClient.Update(context.TODO(), cp)
	}
	return nil
}

// DeleteConsolePlugin deletes a ConsolePlugin instance of ConsolePlugin CRD if present and owned
func DeleteConsolePlugin(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	consolePlugin := &consolev1.ConsolePlugin{}
	err := k8sClient.Get(context.TODO(), resource, consolePlugin)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(consolePlugin.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(consolePlugin.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(consolePlugin.OwnerReferences) > 0 && len(consolePlugin.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete ConsolePlugin %s/%s as it is not owned",
			namespace, name)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s ConsolePlugin", namespace, name)
		return k8sClient.Delete(context.TODO(), consolePlugin)
	}
	consolePlugin.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s ConsolePlugin", namespace, name)
	return k8sClient.Update(context.TODO(), consolePlugin)
}

// GetDaemonSetPods returns a list of pods for the given daemon set
func GetDaemonSetPods(
	k8sClient client.Client,
	ds *appsv1.DaemonSet,
) ([]*v1.Pod, error) {
	return GetPodsByOwner(k8sClient, ds.UID, ds.Namespace)
}

// GetPodsByOwner returns pods for a given owner and namespace
func GetPodsByOwner(
	k8sClient client.Client,
	ownerUID types.UID,
	namespace string,
) ([]*v1.Pod, error) {
	podList := &v1.PodList{}
	err := k8sClient.List(
		context.TODO(),
		podList,
		&client.ListOptions{
			Namespace: namespace,
		},
	)
	if err != nil {
		return nil, err
	}

	result := make([]*v1.Pod, 0)
	for _, pod := range podList.Items {
		for _, owner := range pod.OwnerReferences {
			if owner.UID == ownerUID {
				result = append(result, pod.DeepCopy())
			}
		}
	}
	return result, nil
}

// GetImageFromDeployment returns the image for given container in the deployment
func GetImageFromDeployment(deployment *appsv1.Deployment, containerName string) string {
	for _, c := range deployment.Spec.Template.Spec.Containers {
		if c.Name == containerName {
			return c.Image
		}
	}
	for _, c := range deployment.Spec.Template.Spec.InitContainers {
		if c.Name == containerName {
			return c.Image
		}
	}
	return ""
}

// GetImagePullPolicyFromDeployment returns the image pull policy for given container in the deployment
func GetImagePullPolicyFromDeployment(deployment *appsv1.Deployment, containerName string) v1.PullPolicy {
	for _, c := range deployment.Spec.Template.Spec.Containers {
		if c.Name == containerName {
			return c.ImagePullPolicy
		}
	}
	for _, c := range deployment.Spec.Template.Spec.InitContainers {
		if c.Name == containerName {
			return c.ImagePullPolicy
		}
	}
	return ""
}

// GetContainerFromDeployment returns the container given the name in the deployment
func GetContainerFromDeployment(deployment *appsv1.Deployment, containerName string) *v1.Container {
	for _, c := range deployment.Spec.Template.Spec.Containers {
		if c.Name == containerName {
			return &c
		}
	}
	for _, c := range deployment.Spec.Template.Spec.InitContainers {
		if c.Name == containerName {
			return &c
		}
	}
	return nil
}

// GetValueFromEnv returns a value for the given key name in list of env vars
func GetValueFromEnv(imageKey string, envs []v1.EnvVar) string {
	for _, env := range envs {
		if env.Name == imageKey {
			return env.Value
		}
	}
	return ""
}

func removeOwners(current, toBeDeleted []metav1.OwnerReference) []metav1.OwnerReference {
	toBeDeletedOwnerMap := make(map[types.UID]bool)
	for _, owner := range toBeDeleted {
		toBeDeletedOwnerMap[owner.UID] = true
	}
	newOwners := make([]metav1.OwnerReference, 0)
	for _, currOwner := range current {
		if _, exists := toBeDeletedOwnerMap[currOwner.UID]; !exists {
			newOwners = append(newOwners, currOwner)
		}
	}
	return newOwners
}

// CreateOrUpdateSecret creates a secret if not present, else updates it
func CreateOrUpdateSecret(
	k8sClient client.Client,
	secret *v1.Secret,
	ownerRef *metav1.OwnerReference,
) error {
	existingSecret := &v1.Secret{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      secret.Name,
			Namespace: secret.Namespace,
		},
		existingSecret,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s/%s Secret", secret.Namespace, secret.Name)
		return k8sClient.Create(context.TODO(), secret)
	} else if err != nil {
		return err
	}

	modified := !reflect.DeepEqual(secret.Data, existingSecret.Data)

	for _, o := range existingSecret.OwnerReferences {
		if ownerRef != nil && o.UID != ownerRef.UID {
			secret.OwnerReferences = append(secret.OwnerReferences, o)
		}
	}

	if modified || len(secret.OwnerReferences) > len(existingSecret.OwnerReferences) {
		logrus.Infof("Updating %s/%s Secret", secret.Namespace, secret.Name)
		return k8sClient.Update(context.TODO(), secret)
	}
	return nil
}

// CreateOrAppendToSecret creates a secret if not present, else appends data to it
func CreateOrAppendToSecret(
	k8sClient client.Client,
	secret *v1.Secret,
	ownerRef *metav1.OwnerReference,
) error {
	existingSecret := &v1.Secret{}
	err := k8sClient.Get(
		context.TODO(),
		types.NamespacedName{
			Name:      secret.Name,
			Namespace: secret.Namespace,
		},
		existingSecret,
	)
	if errors.IsNotFound(err) {
		logrus.Infof("Creating %s/%s Secret", secret.Namespace, secret.Name)
		return k8sClient.Create(context.TODO(), secret)
	} else if err != nil {
		return err
	}

	modified := !reflect.DeepEqual(secret.Data, existingSecret.Data)
	if modified {
		// append all existing data to new secret
		for k, v := range existingSecret.Data {
			secret.Data[k] = v
		}
	}

	modified = modified || !reflect.DeepEqual(secret.Annotations, existingSecret.Annotations)

	for _, o := range existingSecret.OwnerReferences {
		if ownerRef != nil && o.UID != ownerRef.UID {
			secret.OwnerReferences = append(secret.OwnerReferences, o)
		}
	}

	if modified || len(secret.OwnerReferences) > len(existingSecret.OwnerReferences) {
		logrus.Infof("Updating %s/%s Secret", secret.Namespace, secret.Name)
		return k8sClient.Update(context.TODO(), secret)
	}
	return nil
}

// DeleteSecret deletes a secret if present and owned
func DeleteSecret(
	k8sClient client.Client,
	name, namespace string,
	owners ...metav1.OwnerReference,
) error {
	resource := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	secret := &v1.Secret{}
	err := k8sClient.Get(context.TODO(), resource, secret)
	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	newOwners := removeOwners(secret.OwnerReferences, owners)

	// Do not delete the object if it does not have the owner that was passed;
	// even if the object has no owner
	if (len(secret.OwnerReferences) == 0 && len(owners) > 0) ||
		(len(secret.OwnerReferences) > 0 && len(secret.OwnerReferences) == len(newOwners)) {
		logrus.Infof("Cannot delete secret %s/%s as it is not owned, secret ownerRef: %v, desired ownerRef: %v",
			namespace, name, secret.OwnerReferences, owners)
		return nil
	}

	if len(newOwners) == 0 {
		logrus.Infof("Deleting %s/%s Secret", namespace, name)
		return k8sClient.Delete(context.TODO(), secret)
	}
	secret.OwnerReferences = newOwners
	logrus.Infof("Disowning %s/%s Secret", namespace, name)
	return k8sClient.Update(context.TODO(), secret)
}

// GetCRDFromFile parses a CRD definition filename from crdBaseDir and returns the parsed object
func GetCRDFromFile(
	filename string,
	crdBaseDir string,
) (*apiextensionsv1.CustomResourceDefinition, error) {
	filepath := path.Join(crdBaseDir, filename)
	scheme := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := ParseObjectFromFile(filepath, scheme, crd); err != nil {
		return nil, err
	}
	return crd, nil
}

// GetV1beta1CRDFromFile parses a CRD definition filename from crdBaseDir and returns the parsed object
func GetV1beta1CRDFromFile(
	filename string,
	crdBaseDir string,
) (*v1beta1.CustomResourceDefinition, error) {
	filepath := path.Join(crdBaseDir, filename)
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	crd := &v1beta1.CustomResourceDefinition{}
	if err := ParseObjectFromFile(filepath, scheme, crd); err != nil {
		return nil, err
	}
	return crd, nil
}

// GetRoleFromFile parses a Role object from a file
func GetRoleFromFile(
	filename string,
	baseDir string,
) (*rbacv1.Role, error) {
	filepath := path.Join(baseDir, filename)
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	role := &rbacv1.Role{}
	if err := ParseObjectFromFile(filepath, scheme, role); err != nil {
		return nil, err
	}
	return role, nil
}

// GetRoleBindingFromFile parses a RoleBinding object from a file
func GetRoleBindingFromFile(
	filename string,
	baseDir string,
) (*rbacv1.RoleBinding, error) {
	filepath := path.Join(baseDir, filename)
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	roleBinding := &rbacv1.RoleBinding{}
	if err := ParseObjectFromFile(filepath, scheme, roleBinding); err != nil {
		return nil, err
	}
	return roleBinding, nil
}

// GetDeploymentFromFile parses a Deployment object from a file
func GetDeploymentFromFile(
	filename string,
	baseDir string,
) (*appsv1.Deployment, error) {
	filepath := path.Join(baseDir, filename)
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	deployment := &appsv1.Deployment{}
	if err := ParseObjectFromFile(filepath, scheme, deployment); err != nil {
		return nil, err
	}
	return deployment, nil
}

// GetDaemonSetFromFile parses a DaemonSet object from a file
func GetDaemonSetFromFile(
	filename string,
	baseDir string,
) (*appsv1.DaemonSet, error) {
	filepath := path.Join(baseDir, filename)
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	daemonset := &appsv1.DaemonSet{}
	if err := ParseObjectFromFile(filepath, scheme, daemonset); err != nil {
		return nil, err
	}
	return daemonset, nil
}

// GetAllObjects gets all objects from k8s
func GetAllObjects(k8sClient client.Client, namespace string) ([]client.Object, error) {
	var objs []client.Object

	objectLists := []client.ObjectList{
		&v1.ServiceList{},
		&v1.ServiceAccountList{},
		&v1.SecretList{},
		&v1.ConfigMapList{},
		&v1.PodList{},
		&v1.PersistentVolumeClaimList{},
		&appsv1.DaemonSetList{},
		&appsv1.DeploymentList{},
		&appsv1.StatefulSetList{},
		&rbacv1.ClusterRoleList{},
		&rbacv1.ClusterRoleBindingList{},
		&rbacv1.RoleList{},
		&rbacv1.RoleBindingList{},
		&storagev1.StorageClassList{},
	}

	for _, objectList := range objectLists {
		if err := AppendObjectList(k8sClient, namespace, objectList, &objs); err != nil {
			return objs, err
		}
	}

	return objs, nil
}

// AppendObjectList gets objects from k8s and adds to the list
func AppendObjectList(k8sClient client.Client, namespace string, list client.ObjectList, objs *[]client.Object) error {
	err := k8sClient.List(
		context.TODO(),
		list,
		&client.ListOptions{
			Namespace: namespace,
		})
	if err != nil {
		logrus.WithError(err).Errorf("failed to list objects %v", list.GetObjectKind())
		return err
	}

	if l, ok := list.(*v1.ServiceList); ok {
		for _, o := range l.Items {
			o.Kind = "Service"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*v1.ServiceAccountList); ok {
		for _, o := range l.Items {
			o.Kind = "ServiceAccount"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*v1.SecretList); ok {
		for _, o := range l.Items {
			o.Kind = "Secret"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*v1.ConfigMapList); ok {
		for _, o := range l.Items {
			o.Kind = "ConfigMap"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*v1.PodList); ok {
		for _, o := range l.Items {
			o.Kind = "Pod"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*v1.PersistentVolumeClaimList); ok {
		for _, o := range l.Items {
			o.Kind = "PersistentVolumeClaim"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*appsv1.DaemonSetList); ok {
		for _, o := range l.Items {
			o.Kind = "DaemonSet"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*appsv1.DeploymentList); ok {
		for _, o := range l.Items {
			o.Kind = "Deployment"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*appsv1.StatefulSetList); ok {
		for _, o := range l.Items {
			o.Kind = "StatefulSet"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*rbacv1.ClusterRoleList); ok {
		for _, o := range l.Items {
			o.Kind = "ClusterRole"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*rbacv1.ClusterRoleBindingList); ok {
		for _, o := range l.Items {
			o.Kind = "ClusterRoleBinding"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*rbacv1.RoleList); ok {
		for _, o := range l.Items {
			o.Kind = "Role"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*rbacv1.RoleBindingList); ok {
		for _, o := range l.Items {
			o.Kind = "RoleBinding"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*storagev1.StorageClassList); ok {
		for _, o := range l.Items {
			o.Kind = "StorageClass"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*apiextensionsv1.CustomResourceDefinitionList); ok {
		for _, o := range l.Items {
			o.Kind = "CustomResourceDefinition"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*monitoringv1.ServiceMonitorList); ok {
		for _, o := range l.Items {
			o.Kind = "ServiceMonitor"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*monitoringv1.PrometheusRuleList); ok {
		for _, o := range l.Items {
			o.Kind = "PrometheusRule"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*monitoringv1.PrometheusList); ok {
		for _, o := range l.Items {
			o.Kind = "Prometheus"
			*objs = append(*objs, o.DeepCopy())
		}
	} else if l, ok := list.(*monitoringv1.AlertmanagerList); ok {
		for _, o := range l.Items {
			o.Kind = "Alertmanager"
			*objs = append(*objs, o.DeepCopy())
		}
	} else {
		msg := fmt.Sprintf("Unknown object kind %s", list.GetObjectKind())
		logrus.Error(msg)
		return fmt.Errorf(msg)
	}

	return nil
}

func AddManagedByOperatorLabel(om metav1.ObjectMeta) metav1.ObjectMeta {
	if om.Labels == nil {
		om.Labels = make(map[string]string)
	}
	om.Labels[constants.OperatorLabelManagedByKey] = constants.OperatorLabelManagedByValue
	return om
}
