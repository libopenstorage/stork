package k8s

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	prometheusclient "github.com/coreos/prometheus-operator/pkg/client/versioned"
	snap_v1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	autopilotclientset "github.com/libopenstorage/autopilot-api/pkg/client/clientset/versioned"
	ostclientset "github.com/libopenstorage/operator/pkg/client/clientset/versioned"
	storkclientset "github.com/libopenstorage/stork/pkg/client/clientset/versioned"
	ocp_clientset "github.com/openshift/client-go/apps/clientset/versioned"
	ocp_security_clientset "github.com/openshift/client-go/security/clientset/versioned"
	"github.com/portworx/sched-ops/task"
	talismanclientset "github.com/portworx/talisman/pkg/client/clientset/versioned"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	batch_v1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	rbac_v1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // needed to initialize plugins on cloud providers
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

const (
	masterLabelKey           = "node-role.kubernetes.io/master"
	hostnameKey              = "kubernetes.io/hostname"
	pvcStorageClassKey       = "volume.beta.kubernetes.io/storage-class"
	pvcStorageProvisionerKey = "volume.beta.kubernetes.io/storage-provisioner"
	labelUpdateMaxRetries    = 5
)

var deleteForegroundPolicy = meta_v1.DeletePropagationForeground

// Ops is an interface to perform any kubernetes related operations
type Ops interface {
	NamespaceOps
	NodeOps
	ServiceOps
	StatefulSetOps
	DeploymentOps
	DeploymentConfigOps
	JobOps
	DaemonSetOps
	RBACOps
	PodOps
	StorageClassOps
	PersistentVolumeClaimOps
	VolumeAttachmentOps
	SnapshotOps
	SnapshotScheduleOps
	GroupSnapshotOps
	RuleOps
	SecretOps
	ConfigMapOps
	EventOps
	CRDOps
	ClusterPairOps
	MigrationOps
	ClusterDomainsOps
	AutopilotRuleOps
	StorageClusterOps
	ObjectOps
	SchedulePolicyOps
	VolumePlacementStrategyOps
	BackupLocationOps
	ApplicationBackupRestoreOps
	ApplicationCloneOps
	VolumeSnapshotRestoreOps
	SecurityContextConstraintsOps
	PrometheusOps
	ClientSetter
	GetVersion() (*version.Info, error)
	// private methods for unit tests
	privateMethods
}

// EventOps is an interface to put and get k8s events
type EventOps interface {
	// CreateEvent puts an event into k8s etcd
	CreateEvent(event *v1.Event) (*v1.Event, error)
	// ListEvents retrieves all events registered with kubernetes
	ListEvents(namespace string, opts meta_v1.ListOptions) (*v1.EventList, error)
}

// NamespaceOps is an interface to perform namespace operations
type NamespaceOps interface {
	// ListNamespaces returns all the namespaces
	ListNamespaces(labelSelector map[string]string) (*v1.NamespaceList, error)
	// GetNamespace returns a namespace object for given name
	GetNamespace(name string) (*v1.Namespace, error)
	// CreateNamespace creates a namespace with given name and metadata
	CreateNamespace(name string, metadata map[string]string) (*v1.Namespace, error)
	// DeleteNamespace deletes a namespace with given name
	DeleteNamespace(name string) error
}

// NodeOps is an interface to perform k8s node operations
type NodeOps interface {
	// CreateNode creates the given node
	CreateNode(n *v1.Node) (*v1.Node, error)
	// UpdateNode updates the given node
	UpdateNode(n *v1.Node) (*v1.Node, error)
	// GetNodes talks to the k8s api server and gets the nodes in the cluster
	GetNodes() (*v1.NodeList, error)
	// GetNodeByName returns the k8s node given it's name
	GetNodeByName(string) (*v1.Node, error)
	// SearchNodeByAddresses searches corresponding k8s node match any of the given address
	SearchNodeByAddresses(addresses []string) (*v1.Node, error)
	// FindMyNode finds LOCAL Node in Kubernetes cluster
	FindMyNode() (*v1.Node, error)
	// IsNodeReady checks if node with given name is ready. Returns nil is ready.
	IsNodeReady(string) error
	// IsNodeMaster returns true if given node is a kubernetes master node
	IsNodeMaster(v1.Node) bool
	// GetLabelsOnNode gets all the labels on the given node
	GetLabelsOnNode(string) (map[string]string, error)
	// AddLabelOnNode adds a label key=value on the given node
	AddLabelOnNode(string, string, string) error
	// RemoveLabelOnNode removes the label with key on given node
	RemoveLabelOnNode(string, string) error
	// WatchNode sets up a watcher that listens for the changes on Node.
	WatchNode(node *v1.Node, fn WatchFunc) error
	// CordonNode cordons the given node
	CordonNode(nodeName string, timeout, retryInterval time.Duration) error
	// UnCordonNode uncordons the given node
	UnCordonNode(nodeName string, timeout, retryInterval time.Duration) error
	// DrainPodsFromNode drains given pods from given node. If timeout is set to
	// a non-zero value, it waits for timeout duration for each pod to get deleted
	DrainPodsFromNode(nodeName string, pods []v1.Pod, timeout, retryInterval time.Duration) error
}

// ServiceOps is an interface to perform k8s service operations
type ServiceOps interface {
	// GetService gets the service by the name
	GetService(string, string) (*v1.Service, error)
	// CreateService creates the given service
	CreateService(*v1.Service) (*v1.Service, error)
	// DeleteService deletes the given service
	DeleteService(name, namespace string) error
	// ValidateDeletedService validates if given service is deleted
	ValidateDeletedService(string, string) error
	// DescribeService gets the service status
	DescribeService(string, string) (*v1.ServiceStatus, error)
	// PatchService patches the current service with the given json path
	PatchService(name, namespace string, jsonPatch []byte) (*v1.Service, error)
}

// StatefulSetOps is an interface to perform k8s stateful set operations
type StatefulSetOps interface {
	// ListStatefulSets lists all the statefulsets for a given namespace
	ListStatefulSets(namespace string) (*appsv1.StatefulSetList, error)
	// GetStatefulSet returns a statefulset for given name and namespace
	GetStatefulSet(name, namespace string) (*appsv1.StatefulSet, error)
	// CreateStatefulSet creates the given statefulset
	CreateStatefulSet(ss *appsv1.StatefulSet) (*appsv1.StatefulSet, error)
	// UpdateStatefulSet creates the given statefulset
	UpdateStatefulSet(ss *appsv1.StatefulSet) (*appsv1.StatefulSet, error)
	// DeleteStatefulSet deletes the given statefulset
	DeleteStatefulSet(name, namespace string) error
	// ValidateStatefulSet validates the given statefulset if it's running and healthy within the given timeout
	ValidateStatefulSet(ss *appsv1.StatefulSet, timeout time.Duration) error
	// ValidateTerminatedStatefulSet validates if given deployment is terminated
	ValidateTerminatedStatefulSet(ss *appsv1.StatefulSet, timeout, retryInterval time.Duration) error
	// GetStatefulSetPods returns pods for the given statefulset
	GetStatefulSetPods(ss *appsv1.StatefulSet) ([]v1.Pod, error)
	// DescribeStatefulSet gets status of the statefulset
	DescribeStatefulSet(name, namespace string) (*appsv1.StatefulSetStatus, error)
	// GetStatefulSetsUsingStorageClass returns all statefulsets using given storage class
	GetStatefulSetsUsingStorageClass(scName string) ([]appsv1.StatefulSet, error)
	// GetPVCsForStatefulSet returns all the PVCs for given stateful set
	GetPVCsForStatefulSet(ss *appsv1.StatefulSet) (*v1.PersistentVolumeClaimList, error)
	// ValidatePVCsForStatefulSet validates the PVCs for the given stateful set
	ValidatePVCsForStatefulSet(ss *appsv1.StatefulSet, timeout, retryInterval time.Duration) error
}

// DeploymentOps is an interface to perform k8s deployment operations
type DeploymentOps interface {
	// ListDeployments lists all deployments for the given namespace
	ListDeployments(namespace string, options meta_v1.ListOptions) (*appsv1.DeploymentList, error)
	// GetDeployment returns a deployment for the give name and namespace
	GetDeployment(name, namespace string) (*appsv1.Deployment, error)
	// CreateDeployment creates the given deployment
	CreateDeployment(*appsv1.Deployment) (*appsv1.Deployment, error)
	// UpdateDeployment updates the given deployment
	UpdateDeployment(*appsv1.Deployment) (*appsv1.Deployment, error)
	// DeleteDeployment deletes the given deployment
	DeleteDeployment(name, namespace string) error
	// ValidateDeployment validates the given deployment if it's running and healthy
	ValidateDeployment(deployment *appsv1.Deployment, timeout, retryInterval time.Duration) error
	// ValidateTerminatedDeployment validates if given deployment is terminated
	ValidateTerminatedDeployment(*appsv1.Deployment, time.Duration, time.Duration) error
	// GetDeploymentPods returns pods for the given deployment
	GetDeploymentPods(*appsv1.Deployment) ([]v1.Pod, error)
	// DescribeDeployment gets the deployment status
	DescribeDeployment(name, namespace string) (*appsv1.DeploymentStatus, error)
	// GetDeploymentsUsingStorageClass returns all deployments using the given storage class
	GetDeploymentsUsingStorageClass(scName string) ([]appsv1.Deployment, error)
}

// DaemonSetOps is an interface to perform k8s daemon set operations
type DaemonSetOps interface {
	// CreateDaemonSet creates the given daemonset
	CreateDaemonSet(ds *appsv1.DaemonSet) (*appsv1.DaemonSet, error)
	// ListDaemonSets lists all daemonsets in given namespace
	ListDaemonSets(namespace string, listOpts meta_v1.ListOptions) ([]appsv1.DaemonSet, error)
	// GetDaemonSet gets the the daemon set with given name
	GetDaemonSet(string, string) (*appsv1.DaemonSet, error)
	// ValidateDaemonSet checks if the given daemonset is ready within given timeout
	ValidateDaemonSet(name, namespace string, timeout time.Duration) error
	// GetDaemonSetPods returns list of pods for the daemonset
	GetDaemonSetPods(*appsv1.DaemonSet) ([]v1.Pod, error)
	// UpdateDaemonSet updates the given daemon set and returns the updated ds
	UpdateDaemonSet(*appsv1.DaemonSet) (*appsv1.DaemonSet, error)
	// DeleteDaemonSet deletes the given daemonset
	DeleteDaemonSet(name, namespace string) error
}

// JobOps is an interface to perform job operations
type JobOps interface {
	// CreateJob creates the given job
	CreateJob(job *batch_v1.Job) (*batch_v1.Job, error)
	// GetJob returns the job from given namespace and name
	GetJob(name, namespace string) (*batch_v1.Job, error)
	// DeleteJob deletes the job with given namespace and name
	DeleteJob(name, namespace string) error
	// ValidateJob validates if the job with given namespace and name succeeds.
	//     It waits for timeout duration for job to succeed
	ValidateJob(name, namespace string, timeout time.Duration) error
}

// RBACOps is an interface to perform RBAC operations
type RBACOps interface {
	// CreateRole creates the given role
	CreateRole(role *rbac_v1.Role) (*rbac_v1.Role, error)
	// UpdateRole updates the given role
	UpdateRole(role *rbac_v1.Role) (*rbac_v1.Role, error)
	// CreateClusterRole creates the given cluster role
	CreateClusterRole(role *rbac_v1.ClusterRole) (*rbac_v1.ClusterRole, error)
	// GetClusterRole gets the given cluster role
	GetClusterRole(name string) (*rbac_v1.ClusterRole, error)
	// UpdateClusterRole updates the given cluster role
	UpdateClusterRole(role *rbac_v1.ClusterRole) (*rbac_v1.ClusterRole, error)
	// CreateRoleBinding creates the given role binding
	CreateRoleBinding(role *rbac_v1.RoleBinding) (*rbac_v1.RoleBinding, error)
	// UpdateRoleBinding updates the given role binding
	UpdateRoleBinding(role *rbac_v1.RoleBinding) (*rbac_v1.RoleBinding, error)
	// GetClusterRoleBinding gets the given cluster role binding
	GetClusterRoleBinding(name string) (*rbac_v1.ClusterRoleBinding, error)
	// ListClusterRoleBindings lists the cluster role bindings
	ListClusterRoleBindings() (*rbac_v1.ClusterRoleBindingList, error)
	// CreateClusterRoleBinding creates the given cluster role binding
	CreateClusterRoleBinding(role *rbac_v1.ClusterRoleBinding) (*rbac_v1.ClusterRoleBinding, error)
	// UpdateClusterRoleBinding updates the given cluster role binding
	UpdateClusterRoleBinding(role *rbac_v1.ClusterRoleBinding) (*rbac_v1.ClusterRoleBinding, error)
	// CreateServiceAccount creates the given service account
	CreateServiceAccount(account *v1.ServiceAccount) (*v1.ServiceAccount, error)
	// DeleteRole deletes the given role
	DeleteRole(name, namespace string) error
	// DeleteRoleBinding deletes the given role binding
	DeleteRoleBinding(name, namespace string) error
	// DeleteClusterRole deletes the given cluster role
	DeleteClusterRole(roleName string) error
	// DeleteClusterRoleBinding deletes the given cluster role binding
	DeleteClusterRoleBinding(roleName string) error
	// DeleteServiceAccount deletes the given service account
	DeleteServiceAccount(accountName, namespace string) error
}

// PodOps is an interface to perform k8s pod operations
type PodOps interface {
	// CreatePod creates the given pod.
	CreatePod(pod *v1.Pod) (*v1.Pod, error)
	// UpdatePod updates the given pod
	UpdatePod(pod *v1.Pod) (*v1.Pod, error)
	// GetPods returns pods for the given namespace
	GetPods(string, map[string]string) (*v1.PodList, error)
	// GetPodsByNode returns all pods in given namespace and given k8s node name.
	//  If namespace is empty, it will return pods from all namespaces
	GetPodsByNode(nodeName, namespace string) (*v1.PodList, error)
	// GetPodsByOwner returns pods for the given owner and namespace
	GetPodsByOwner(types.UID, string) ([]v1.Pod, error)
	// GetPodsUsingPV returns all pods in cluster using given pv
	GetPodsUsingPV(pvName string) ([]v1.Pod, error)
	// GetPodsUsingPVByNodeName returns all pods running on the node using the given pv
	GetPodsUsingPVByNodeName(pvName, nodeName string) ([]v1.Pod, error)
	// GetPodsUsingPVC returns all pods in cluster using given pvc
	GetPodsUsingPVC(pvcName, pvcNamespace string) ([]v1.Pod, error)
	// GetPodsUsingPVCByNodeName returns all pods running on the node using given pvc
	GetPodsUsingPVCByNodeName(pvcName, pvcNamespace, nodeName string) ([]v1.Pod, error)
	// GetPodsUsingVolumePlugin returns all pods who use PVCs provided by the given volume plugin
	GetPodsUsingVolumePlugin(plugin string) ([]v1.Pod, error)
	// GetPodsUsingVolumePluginByNodeName returns all pods who use PVCs provided by the given volume plugin on the given node
	GetPodsUsingVolumePluginByNodeName(nodeName, plugin string) ([]v1.Pod, error)
	// GetPodByName returns pod for the given pod name and namespace
	GetPodByName(string, string) (*v1.Pod, error)
	// GetPodByUID returns pod with the given UID, or error if nothing found
	GetPodByUID(types.UID, string) (*v1.Pod, error)
	// DeletePod deletes the given pod
	DeletePod(string, string, bool) error
	// DeletePods deletes the given pods
	DeletePods([]v1.Pod, bool) error
	// IsPodRunning checks if all containers in a pod are in running state
	IsPodRunning(v1.Pod) bool
	// IsPodReady checks if all containers in a pod are ready (passed readiness probe)
	IsPodReady(v1.Pod) bool
	// IsPodBeingManaged returns true if the pod is being managed by a controller
	IsPodBeingManaged(v1.Pod) bool
	// WaitForPodDeletion waits for given timeout for given pod to be deleted
	WaitForPodDeletion(uid types.UID, namespace string, timeout time.Duration) error
	// RunCommandInPod runs given command in the given pod
	RunCommandInPod(cmds []string, podName, containerName, namespace string) (string, error)
	// ValidatePod validates the given pod if it's ready
	ValidatePod(pod *v1.Pod, timeout, retryInterval time.Duration) error
	// WatchPods sets up a watcher that listens for the changes to pods in given namespace
	WatchPods(namespace string, fn WatchFunc, listOptions meta_v1.ListOptions) error
}

// StorageClassOps is an interface to perform k8s storage class operations
type StorageClassOps interface {
	// GetStorageClasses returns all storageClasses that match given optional label selector
	GetStorageClasses(labelSelector map[string]string) (*storagev1.StorageClassList, error)
	// GetStorageClass returns the storage class for the give namme
	GetStorageClass(name string) (*storagev1.StorageClass, error)
	// CreateStorageClass creates the given storage class
	CreateStorageClass(sc *storagev1.StorageClass) (*storagev1.StorageClass, error)
	// DeleteStorageClass deletes the given storage class
	DeleteStorageClass(name string) error
	// GetStorageClassParams returns the parameters of the given sc in the native map format
	GetStorageClassParams(sc *storagev1.StorageClass) (map[string]string, error)
	// ValidateStorageClass validates the given storage class
	// TODO: This is currently the same as GetStorageClass. If no one is using it,
	// we should remove this method
	ValidateStorageClass(name string) (*storagev1.StorageClass, error)
}

// PersistentVolumeClaimOps is an interface to perform k8s PVC operations
type PersistentVolumeClaimOps interface {
	// CreatePersistentVolumeClaim creates the given persistent volume claim
	CreatePersistentVolumeClaim(*v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error)
	// UpdatePersistentVolumeClaim updates an existing persistent volume claim
	UpdatePersistentVolumeClaim(*v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error)
	// DeletePersistentVolumeClaim deletes the given persistent volume claim
	DeletePersistentVolumeClaim(name, namespace string) error
	// ValidatePersistentVolumeClaim validates the given pvc
	ValidatePersistentVolumeClaim(vv *v1.PersistentVolumeClaim, timeout, retryInterval time.Duration) error
	// ValidatePersistentVolumeClaimSize validates the given pvc size
	ValidatePersistentVolumeClaimSize(vv *v1.PersistentVolumeClaim, expectedPVCSize int64, timeout, retryInterval time.Duration) error
	// GetPersistentVolumeClaim returns the PVC for given name and namespace
	GetPersistentVolumeClaim(pvcName string, namespace string) (*v1.PersistentVolumeClaim, error)
	// GetPersistentVolumeClaims returns all PVCs in given namespace and that match the optional labelSelector
	GetPersistentVolumeClaims(namespace string, labelSelector map[string]string) (*v1.PersistentVolumeClaimList, error)
	// CreatePersistentVolume creates the given PV
	CreatePersistentVolume(pv *v1.PersistentVolume) (*v1.PersistentVolume, error)
	// GetPersistentVolume returns the PV for given name
	GetPersistentVolume(pvName string) (*v1.PersistentVolume, error)
	// DeletePersistentVolume deletes the PV for given name
	DeletePersistentVolume(pvName string) error
	// GetPersistentVolumes returns all PVs in cluster
	GetPersistentVolumes() (*v1.PersistentVolumeList, error)
	// GetVolumeForPersistentVolumeClaim returns the volumeID for the given PVC
	GetVolumeForPersistentVolumeClaim(*v1.PersistentVolumeClaim) (string, error)
	// GetPersistentVolumeClaimParams fetches custom parameters for the given PVC
	GetPersistentVolumeClaimParams(*v1.PersistentVolumeClaim) (map[string]string, error)
	// GetPersistentVolumeClaimStatus returns the status of the given pvc
	GetPersistentVolumeClaimStatus(*v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaimStatus, error)
	// GetPVCsUsingStorageClass returns all PVCs that use the given storage class
	GetPVCsUsingStorageClass(scName string) ([]v1.PersistentVolumeClaim, error)
	// GetStorageProvisionerForPVC returns storage provisioner for given PVC if it exists
	GetStorageProvisionerForPVC(pvc *v1.PersistentVolumeClaim) (string, error)
}

// SnapshotOps is an interface to perform k8s VolumeSnapshot operations
type SnapshotOps interface {
	// GetSnapshot returns the snapshot for given name and namespace
	GetSnapshot(name string, namespace string) (*snap_v1.VolumeSnapshot, error)
	// ListSnapshots lists all snapshots in the given namespace
	ListSnapshots(namespace string) (*snap_v1.VolumeSnapshotList, error)
	// CreateSnapshot creates the given snapshot
	CreateSnapshot(*snap_v1.VolumeSnapshot) (*snap_v1.VolumeSnapshot, error)
	// UpdateSnapshot updates the given snapshot
	UpdateSnapshot(*snap_v1.VolumeSnapshot) (*snap_v1.VolumeSnapshot, error)
	// DeleteSnapshot deletes the given snapshot
	DeleteSnapshot(name string, namespace string) error
	// ValidateSnapshot validates the given snapshot.
	ValidateSnapshot(name string, namespace string, retry bool, timeout, retryInterval time.Duration) error
	// GetVolumeForSnapshot returns the volumeID for the given snapshot
	GetVolumeForSnapshot(name string, namespace string) (string, error)
	// GetSnapshotStatus returns the status of the given snapshot
	GetSnapshotStatus(name string, namespace string) (*snap_v1.VolumeSnapshotStatus, error)
	// GetSnapshotData returns the snapshot for given name
	GetSnapshotData(name string) (*snap_v1.VolumeSnapshotData, error)
	// CreateSnapshotData creates the given volume snapshot data object
	CreateSnapshotData(*snap_v1.VolumeSnapshotData) (*snap_v1.VolumeSnapshotData, error)
	// DeleteSnapshotData deletes the given snapshot
	DeleteSnapshotData(name string) error
	// ValidateSnapshotData validates the given snapshot data object
	ValidateSnapshotData(name string, retry bool, timeout, retryInterval time.Duration) error
}

// VolumeAttachmentOps is an interface to perform k8s VolumeAttachmentOps operations
type VolumeAttachmentOps interface {
	// ListVolumeAttachments lists all volume attachments
	ListVolumeAttachments() (*storagev1beta1.VolumeAttachmentList, error)
	// DeleteVolumeAttachment deletes a given Volume Attachment by name
	DeleteVolumeAttachment(name string) error
	// CreateVolumeAttachment creates a volume attachment
	CreateVolumeAttachment(*storagev1beta1.VolumeAttachment) (*storagev1beta1.VolumeAttachment, error)
	// UpdateVolumeAttachment updates a volume attachment
	UpdateVolumeAttachment(*storagev1beta1.VolumeAttachment) (*storagev1beta1.VolumeAttachment, error)
	// UpdateVolumeAttachmentStatus updates a volume attachment status
	UpdateVolumeAttachmentStatus(*storagev1beta1.VolumeAttachment) (*storagev1beta1.VolumeAttachment, error)
}

// SecretOps is an interface to perform k8s Secret operations
type SecretOps interface {
	// GetSecret gets the secrets object given its name and namespace
	GetSecret(name string, namespace string) (*v1.Secret, error)
	// CreateSecret creates the given secret
	CreateSecret(*v1.Secret) (*v1.Secret, error)
	// UpdateSecret updates the given secret
	UpdateSecret(*v1.Secret) (*v1.Secret, error)
	// UpdateSecretData updates or creates a new secret with the given data
	UpdateSecretData(string, string, map[string][]byte) (*v1.Secret, error)
	// DeleteSecret deletes the given secret
	DeleteSecret(name, namespace string) error
}

// ConfigMapOps is an interface to perform k8s ConfigMap operations
type ConfigMapOps interface {
	// GetConfigMap gets the config map object for the given name and namespace
	GetConfigMap(name string, namespace string) (*v1.ConfigMap, error)
	// CreateConfigMap creates a new config map object if it does not already exist.
	CreateConfigMap(configMap *v1.ConfigMap) (*v1.ConfigMap, error)
	// DeleteConfigMap deletes the given config map
	DeleteConfigMap(name, namespace string) error
	// UpdateConfigMap updates the given config map object
	UpdateConfigMap(configMap *v1.ConfigMap) (*v1.ConfigMap, error)
	// WatchConfigMap sets up a watcher that listens for changes on the config map
	WatchConfigMap(configMap *v1.ConfigMap, fn WatchFunc) error
}

// ObjectOps is an interface to perform generic Object operations
type ObjectOps interface {
	// GetObject returns the latest object given a generic Object
	GetObject(object runtime.Object) (runtime.Object, error)
	// UpdateObject updates a generic Object
	UpdateObject(object runtime.Object) (runtime.Object, error)
}

type privateMethods interface {
	initK8sClient() error
}

var (
	instance Ops
	once     sync.Once
)

type k8sOps struct {
	client             kubernetes.Interface
	snapClient         rest.Interface
	storkClient        storkclientset.Interface
	ostClient          ostclientset.Interface
	talismanClient     talismanclientset.Interface
	autopilotClient    autopilotclientset.Interface
	apiExtensionClient apiextensionsclient.Interface
	config             *rest.Config
	dynamicInterface   dynamic.Interface
	ocpClient          ocp_clientset.Interface
	ocpSecurityClient  ocp_security_clientset.Interface
	prometheusClient   prometheusclient.Interface
}

// Instance returns a singleton instance of k8sOps type
func Instance() Ops {
	once.Do(func() {
		instance = &k8sOps{}
	})
	return instance
}

// NewInstance returns new instance of k8sOps by using given config
func NewInstance(config string) (Ops, error) {
	newInstance := &k8sOps{}
	err := newInstance.loadClientFromKubeconfig(config)
	if err != nil {
		logrus.Errorf("Unable to set new instance: %v", err)
		return nil, err
	}
	return newInstance, nil
}

func (k *k8sOps) GetVersion() (*version.Info, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.Discovery().ServerVersion()
}

// Namespace APIs - BEGIN

func (k *k8sOps) ListNamespaces(labelSelector map[string]string) (*v1.NamespaceList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().Namespaces().List(meta_v1.ListOptions{
		LabelSelector: mapToCSV(labelSelector),
	})
}

func (k *k8sOps) GetNamespace(name string) (*v1.Namespace, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().Namespaces().Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) CreateNamespace(name string, metadata map[string]string) (*v1.Namespace, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().Namespaces().Create(&v1.Namespace{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:   name,
			Labels: metadata,
		},
	})
}

func (k *k8sOps) DeleteNamespace(name string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.CoreV1().Namespaces().Delete(name, &meta_v1.DeleteOptions{})
}

// Namespace APIs - END
func (k *k8sOps) CreateNode(n *v1.Node) (*v1.Node, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().Nodes().Create(n)
}

func (k *k8sOps) UpdateNode(n *v1.Node) (*v1.Node, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().Nodes().Update(n)
}

func (k *k8sOps) GetNodes() (*v1.NodeList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	nodes, err := k.client.CoreV1().Nodes().List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

func (k *k8sOps) GetNodeByName(name string) (*v1.Node, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	node, err := k.client.CoreV1().Nodes().Get(name, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (k *k8sOps) IsNodeReady(name string) error {
	node, err := k.GetNodeByName(name)
	if err != nil {
		return err
	}

	for _, condition := range node.Status.Conditions {
		switch condition.Type {
		case v1.NodeConditionType(v1.NodeReady):
			if condition.Status != v1.ConditionStatus(v1.ConditionTrue) {
				return fmt.Errorf("node: %v is not ready as condition: %v (%v) is %v. Reason: %v",
					name, condition.Type, condition.Message, condition.Status, condition.Reason)
			}
		case v1.NodeConditionType(v1.NodeOutOfDisk),
			v1.NodeConditionType(v1.NodeMemoryPressure),
			v1.NodeConditionType(v1.NodeDiskPressure),
			v1.NodeConditionType(v1.NodeNetworkUnavailable):
			// only checks if condition is true, ignoring condition Unknown
			if condition.Status == v1.ConditionStatus(v1.ConditionTrue) {
				return fmt.Errorf("node: %v is not ready as condition: %v (%v) is %v. Reason: %v",
					name, condition.Type, condition.Message, condition.Status, condition.Reason)
			}
		}
	}

	return nil
}

func (k *k8sOps) IsNodeMaster(node v1.Node) bool {
	_, ok := node.Labels[masterLabelKey]
	return ok
}

func (k *k8sOps) GetLabelsOnNode(name string) (map[string]string, error) {
	node, err := k.GetNodeByName(name)
	if err != nil {
		return nil, err
	}

	return node.Labels, nil
}

// SearchNodeByAddresses searches the node based on the IP addresses, then it falls back to a
// search by hostname, and finally by the labels
func (k *k8sOps) SearchNodeByAddresses(addresses []string) (*v1.Node, error) {
	nodes, err := k.GetNodes()
	if err != nil {
		return nil, err
	}

	// sweep #1 - locating based on IP address
	for _, node := range nodes.Items {
		for _, addr := range node.Status.Addresses {
			switch addr.Type {
			case v1.NodeExternalIP:
				fallthrough
			case v1.NodeInternalIP:
				for _, ip := range addresses {
					if addr.Address == ip {
						return &node, nil
					}
				}
			}
		}
	}

	// sweep #2 - locating based on Hostname
	for _, node := range nodes.Items {
		for _, addr := range node.Status.Addresses {
			switch addr.Type {
			case v1.NodeHostName:
				for _, ip := range addresses {
					if addr.Address == ip {
						return &node, nil
					}
				}
			}
		}
	}

	// sweep #3 - locating based on labels
	for _, node := range nodes.Items {
		if hn, has := node.GetLabels()[hostnameKey]; has {
			for _, ip := range addresses {
				if hn == ip {
					return &node, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("failed to find k8s node for given addresses: %v", addresses)
}

// FindMyNode finds LOCAL Node in Kubernetes cluster.
func (k *k8sOps) FindMyNode() (*v1.Node, error) {
	ipList, err := getLocalIPList(true)
	if err != nil {
		return nil, fmt.Errorf("Could not find my IPs/Hostname: %s", err)
	}
	return k.SearchNodeByAddresses(ipList)
}

func (k *k8sOps) AddLabelOnNode(name, key, value string) error {
	var err error
	if err := k.initK8sClient(); err != nil {
		return err
	}

	retryCnt := 0
	for retryCnt < labelUpdateMaxRetries {
		retryCnt++

		node, err := k.client.CoreV1().Nodes().Get(name, meta_v1.GetOptions{})
		if err != nil {
			return err
		}

		if val, present := node.Labels[key]; present && val == value {
			return nil
		}

		node.Labels[key] = value
		if _, err = k.client.CoreV1().Nodes().Update(node); err == nil {
			return nil
		}
	}

	return err
}

func (k *k8sOps) RemoveLabelOnNode(name, key string) error {
	var err error
	if err := k.initK8sClient(); err != nil {
		return err
	}

	retryCnt := 0
	for retryCnt < labelUpdateMaxRetries {
		retryCnt++

		node, err := k.client.CoreV1().Nodes().Get(name, meta_v1.GetOptions{})
		if err != nil {
			return err
		}

		if _, present := node.Labels[key]; present {
			delete(node.Labels, key)
			if _, err = k.client.CoreV1().Nodes().Update(node); err == nil {
				return nil
			}
		}
	}

	return err
}

// WatchFunc is a callback provided to the Watch functions
// which is invoked when the given object is changed.
type WatchFunc func(object runtime.Object) error

// handleWatch is internal function that handles the watch.  On channel shutdown (ie. stop watch),
// it'll attempt to reestablish its watch function.
func (k *k8sOps) handleWatch(
	watchInterface watch.Interface,
	object runtime.Object,
	namespace string,
	fn WatchFunc,
	listOptions meta_v1.ListOptions) {
	defer watchInterface.Stop()
	for {
		select {
		case event, more := <-watchInterface.ResultChan():
			if !more {
				logrus.Debug("Kubernetes watch closed (attempting to re-establish)")

				t := func() (interface{}, bool, error) {
					var err error
					if node, ok := object.(*v1.Node); ok {
						err = k.WatchNode(node, fn)
					} else if cm, ok := object.(*v1.ConfigMap); ok {
						err = k.WatchConfigMap(cm, fn)
					} else if _, ok := object.(*v1.Pod); ok {
						err = k.WatchPods(namespace, fn, listOptions)
					} else {
						return "", false, fmt.Errorf("unsupported object: %v given to handle watch", object)
					}

					return "", true, err
				}

				if _, err := task.DoRetryWithTimeout(t, 10*time.Minute, 10*time.Second); err != nil {
					logrus.WithError(err).Error("Could not re-establish the watch")
				} else {
					logrus.Debug("watch re-established")
				}
				return
			}

			fn(event.Object)
		}
	}
}

func (k *k8sOps) WatchNode(node *v1.Node, watchNodeFn WatchFunc) error {
	if node == nil {
		return fmt.Errorf("no node given to watch")
	}

	if err := k.initK8sClient(); err != nil {
		return err
	}

	listOptions := meta_v1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", node.Name).String(),
		Watch:         true,
	}

	watchInterface, err := k.client.CoreV1().Nodes().Watch(listOptions)
	if err != nil {
		return err
	}

	// fire off watch function
	go k.handleWatch(watchInterface, node, "", watchNodeFn, listOptions)
	return nil
}

func (k *k8sOps) CordonNode(nodeName string, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return nil, true, err
		}

		n, err := k.GetNodeByName(nodeName)
		if err != nil {
			return nil, true, err
		}

		nCopy := n.DeepCopy()
		nCopy.Spec.Unschedulable = true
		n, err = k.client.CoreV1().Nodes().Update(nCopy)
		if err != nil {
			return nil, true, err
		}

		return nil, false, nil

	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}

func (k *k8sOps) UnCordonNode(nodeName string, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return nil, true, err
		}

		n, err := k.GetNodeByName(nodeName)
		if err != nil {
			return nil, true, err
		}

		nCopy := n.DeepCopy()
		nCopy.Spec.Unschedulable = false
		n, err = k.client.CoreV1().Nodes().Update(nCopy)
		if err != nil {
			return nil, true, err
		}

		return nil, false, nil

	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}

func (k *k8sOps) DrainPodsFromNode(nodeName string, pods []v1.Pod, timeout time.Duration, retryInterval time.Duration) error {
	err := k.CordonNode(nodeName, timeout, retryInterval)
	if err != nil {
		return err
	}

	err = k.DeletePods(pods, false)
	if err != nil {
		e := k.UnCordonNode(nodeName, timeout, retryInterval) // rollback cordon
		if e != nil {
			log.Printf("failed to uncordon node: %s", nodeName)
		}
		return err
	}

	if timeout > 0 {
		for _, p := range pods {
			err = k.WaitForPodDeletion(p.UID, p.Namespace, timeout)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (k *k8sOps) WaitForPodDeletion(uid types.UID, namespace string, timeout time.Duration) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return nil, true, err
		}

		p, err := k.GetPodByUID(uid, namespace)
		if err != nil {
			if err == ErrPodsNotFound {
				return nil, false, nil
			}

			return nil, true, err
		}

		if p != nil {
			return nil, true, fmt.Errorf("pod %s:%s (%s) still present in the system", namespace, p.Name, uid)
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, 5*time.Second); err != nil {
		return err
	}

	return nil
}

func (k *k8sOps) RunCommandInPod(cmds []string, podName, containerName, namespace string) (string, error) {
	err := k.initK8sClient()
	if err != nil {
		return "", err
	}

	var (
		execOut bytes.Buffer
		execErr bytes.Buffer
	)

	pod, err := k.client.CoreV1().Pods(namespace).Get(podName, meta_v1.GetOptions{})
	if err != nil {
		return "", err
	}

	if len(containerName) == 0 {
		if len(pod.Spec.Containers) != 1 {
			return "", fmt.Errorf("could not determine which container to use")
		}

		containerName = pod.Spec.Containers[0].Name
	}

	req := k.client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec")

	req.VersionedParams(&v1.PodExecOptions{
		Container: containerName,
		Command:   cmds,
		Stdout:    true,
		Stderr:    true,
	}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(k.config, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("failed to init executor: %v", err)
	}

	err = exec.Stream(remotecommand.StreamOptions{
		Stdout: &execOut,
		Stderr: &execErr,
		Tty:    false,
	})

	if err != nil {
		return execErr.String(), fmt.Errorf("could not execute: %v: %v %v", err, execErr.String(), execOut.String())
	}

	if execErr.Len() > 0 {
		return execErr.String(), nil
	}

	return execOut.String(), nil
}

// Service APIs - BEGIN

func (k *k8sOps) CreateService(service *v1.Service) (*v1.Service, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ns := service.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return k.client.CoreV1().Services(ns).Create(service)
}

func (k *k8sOps) DeleteService(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.CoreV1().Services(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) GetService(svcName string, svcNS string) (*v1.Service, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	if svcName == "" {
		return nil, fmt.Errorf("cannot return service obj without service name")
	}

	return k.client.CoreV1().Services(svcNS).Get(svcName, meta_v1.GetOptions{})
}

func (k *k8sOps) DescribeService(svcName string, svcNamespace string) (*v1.ServiceStatus, error) {
	svc, err := k.GetService(svcName, svcNamespace)
	if err != nil {
		return nil, err
	}
	return &svc.Status, err
}

func (k *k8sOps) ValidateDeletedService(svcName string, svcNS string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	if svcName == "" {
		return fmt.Errorf("cannot validate service without service name")
	}

	_, err := k.client.CoreV1().Services(svcNS).Get(svcName, meta_v1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	return nil
}

func (k *k8sOps) PatchService(name, namespace string, jsonPatch []byte) (*v1.Service, error) {
	current, err := k.GetService(name, namespace)
	if err != nil {
		return nil, err
	}

	return k.client.CoreV1().Services(current.Namespace).Patch(current.Name, types.StrategicMergePatchType, jsonPatch)
}

// Service APIs - END

// Deployment APIs - BEGIN

func (k *k8sOps) ListDeployments(namespace string, options meta_v1.ListOptions) (*appsv1.DeploymentList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.AppsV1().Deployments(namespace).List(options)
}

func (k *k8sOps) GetDeployment(name, namespace string) (*appsv1.Deployment, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.AppsV1().Deployments(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) CreateDeployment(deployment *appsv1.Deployment) (*appsv1.Deployment, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ns := deployment.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return k.client.AppsV1().Deployments(ns).Create(deployment)
}

func (k *k8sOps) DeleteDeployment(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.AppsV1().Deployments(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) DescribeDeployment(depName, depNamespace string) (*appsv1.DeploymentStatus, error) {
	dep, err := k.GetDeployment(depName, depNamespace)
	if err != nil {
		return nil, err
	}
	return &dep.Status, err
}

func (k *k8sOps) UpdateDeployment(deployment *appsv1.Deployment) (*appsv1.Deployment, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.client.AppsV1().Deployments(deployment.Namespace).Update(deployment)
}

func (k *k8sOps) ValidateDeployment(deployment *appsv1.Deployment, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		dep, err := k.GetDeployment(deployment.Name, deployment.Namespace)
		if err != nil {
			return "", true, err
		}

		requiredReplicas := *dep.Spec.Replicas
		shared := false

		if requiredReplicas != 1 {
			foundPVC := false
			for _, vol := range dep.Spec.Template.Spec.Volumes {
				if vol.PersistentVolumeClaim != nil {
					foundPVC = true

					claim, err := k.client.CoreV1().
						PersistentVolumeClaims(dep.Namespace).
						Get(vol.PersistentVolumeClaim.ClaimName, meta_v1.GetOptions{})
					if err != nil {
						return "", true, err
					}

					if k.isPVCShared(claim) {
						shared = true
						break
					}
				}
			}

			if foundPVC && !shared {
				requiredReplicas = 1
			}
		}

		pods, err := k.GetDeploymentPods(deployment)
		if err != nil || pods == nil {
			return "", true, &ErrAppNotReady{
				ID:    dep.Name,
				Cause: fmt.Sprintf("Failed to get pods for deployment. Err: %v", err),
			}
		}

		if len(pods) == 0 {
			return "", true, &ErrAppNotReady{
				ID:    dep.Name,
				Cause: "Deployment has 0 pods",
			}
		}
		podsOverviewString := k.generatePodsOverviewString(pods)
		if requiredReplicas > dep.Status.AvailableReplicas {
			return "", true, &ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Available replicas: %v Current pods overview:\n%s",
					requiredReplicas, dep.Status.AvailableReplicas, podsOverviewString),
			}
		}

		if requiredReplicas > dep.Status.ReadyReplicas {
			return "", true, &ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Ready replicas: %v Current pods overview:\n%s",
					requiredReplicas, dep.Status.ReadyReplicas, podsOverviewString),
			}
		}

		if requiredReplicas != dep.Status.UpdatedReplicas && shared {
			return "", true, &ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Updated replicas: %v Current pods overview:\n%s",
					requiredReplicas, dep.Status.UpdatedReplicas, podsOverviewString),
			}
		}

		// look for "requiredReplicas" number of pods in ready state
		var notReadyPods []string
		var readyCount int32
		for _, pod := range pods {
			if !k.IsPodReady(pod) {
				notReadyPods = append(notReadyPods, pod.Name)
			} else {
				readyCount++
			}
		}

		if readyCount >= requiredReplicas {
			return "", false, nil
		}

		return "", true, &ErrAppNotReady{
			ID:    dep.Name,
			Cause: fmt.Sprintf("Pod(s): %#v not yet ready", notReadyPods),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) ValidateTerminatedDeployment(deployment *appsv1.Deployment, timeout, timeBeforeRetry time.Duration) error {
	t := func() (interface{}, bool, error) {
		dep, err := k.GetDeployment(deployment.Name, deployment.Namespace)
		if err != nil {
			if errors.IsNotFound(err) {
				return "", false, nil
			}
			return "", true, err
		}

		pods, err := k.GetDeploymentPods(deployment)
		if err != nil {
			return "", true, &ErrAppNotTerminated{
				ID:    dep.Name,
				Cause: fmt.Sprintf("Failed to get pods for deployment. Err: %v", err),
			}
		}

		if pods != nil && len(pods) > 0 {
			var podNames []string
			for _, pod := range pods {
				podNames = append(podNames, pod.Name)
			}
			return "", true, &ErrAppNotTerminated{
				ID:    dep.Name,
				Cause: fmt.Sprintf("pods: %v are still present", podNames),
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, timeBeforeRetry); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) GetDeploymentPods(deployment *appsv1.Deployment) ([]v1.Pod, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	rSets, err := k.client.AppsV1().ReplicaSets(deployment.Namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, rSet := range rSets.Items {
		for _, owner := range rSet.OwnerReferences {
			if owner.Name == deployment.Name {
				return k.GetPodsByOwner(rSet.UID, rSet.Namespace)
			}
		}
	}

	return nil, nil
}

func (k *k8sOps) GetDeploymentsUsingStorageClass(scName string) ([]appsv1.Deployment, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	deps, err := k.client.AppsV1().Deployments("").List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var retList []appsv1.Deployment
	for _, dep := range deps.Items {
		for _, v := range dep.Spec.Template.Spec.Volumes {
			if v.PersistentVolumeClaim == nil {
				continue
			}

			pvc, err := k.GetPersistentVolumeClaim(v.PersistentVolumeClaim.ClaimName, dep.Namespace)
			if err != nil {
				continue // don't let one bad pvc stop processing
			}

			sc, err := k.getStorageClassForPVC(pvc)
			if err == nil && sc.Name == scName {
				retList = append(retList, dep)
				break
			}
		}
	}

	return retList, nil
}

// Deployment APIs - END

// DaemonSet APIs - BEGIN

func (k *k8sOps) CreateDaemonSet(ds *appsv1.DaemonSet) (*appsv1.DaemonSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.AppsV1().DaemonSets(ds.Namespace).Create(ds)
}

func (k *k8sOps) ListDaemonSets(namespace string, listOpts meta_v1.ListOptions) ([]appsv1.DaemonSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	dsList, err := k.client.AppsV1().DaemonSets(namespace).List(listOpts)
	if err != nil {
		return nil, err
	}

	return dsList.Items, nil
}

func (k *k8sOps) GetDaemonSet(name, namespace string) (*appsv1.DaemonSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	if len(namespace) == 0 {
		namespace = v1.NamespaceDefault
	}

	ds, err := k.client.AppsV1().DaemonSets(namespace).Get(name, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return ds, nil
}

func (k *k8sOps) GetDaemonSetPods(ds *appsv1.DaemonSet) ([]v1.Pod, error) {
	return k.GetPodsByOwner(ds.UID, ds.Namespace)
}

func (k *k8sOps) ValidateDaemonSet(name, namespace string, timeout time.Duration) error {
	t := func() (interface{}, bool, error) {
		ds, err := k.GetDaemonSet(name, namespace)
		if err != nil {
			return "", true, err
		}

		if ds.Status.ObservedGeneration == 0 {
			return "", true, &ErrAppNotReady{
				ID:    name,
				Cause: "Observed generation is still 0. Check back status after some time",
			}
		}

		pods, err := k.GetDaemonSetPods(ds)
		if err != nil || pods == nil {
			return "", true, &ErrAppNotReady{
				ID:    ds.Name,
				Cause: fmt.Sprintf("Failed to get pods for daemonset. Err: %v", err),
			}
		}

		if len(pods) == 0 {
			return "", true, &ErrAppNotReady{
				ID:    ds.Name,
				Cause: "DaemonSet has 0 pods",
			}
		}

		podsOverviewString := k.generatePodsOverviewString(pods)

		if ds.Status.DesiredNumberScheduled != ds.Status.UpdatedNumberScheduled {
			return "", true, &ErrAppNotReady{
				ID: name,
				Cause: fmt.Sprintf("Not all pods are updated. expected: %v updated: %v. Current pods overview:\n%s",
					ds.Status.DesiredNumberScheduled, ds.Status.UpdatedNumberScheduled, podsOverviewString),
			}
		}

		if ds.Status.NumberUnavailable > 0 {
			return "", true, &ErrAppNotReady{
				ID: name,
				Cause: fmt.Sprintf("%d pods are not available. available: %d ready: %d. Current pods overview:\n%s",
					ds.Status.NumberUnavailable, ds.Status.NumberAvailable,
					ds.Status.NumberReady, podsOverviewString),
			}
		}

		if ds.Status.DesiredNumberScheduled != ds.Status.NumberReady {
			return "", true, &ErrAppNotReady{
				ID: name,
				Cause: fmt.Sprintf("Expected ready: %v Actual ready:%v Current pods overview:\n%s",
					ds.Status.DesiredNumberScheduled, ds.Status.NumberReady, podsOverviewString),
			}
		}

		var notReadyPods []string
		var readyCount int32
		for _, pod := range pods {
			if !k.IsPodReady(pod) {
				notReadyPods = append(notReadyPods, pod.Name)
			} else {
				readyCount++
			}
		}

		if readyCount == ds.Status.DesiredNumberScheduled {
			return "", false, nil
		}

		return "", true, &ErrAppNotReady{
			ID:    ds.Name,
			Cause: fmt.Sprintf("Pod(s): %#v not yet ready", notReadyPods),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, 15*time.Second); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) generatePodsOverviewString(pods []v1.Pod) string {
	var buffer bytes.Buffer
	for _, p := range pods {
		running := k.IsPodRunning(p)
		ready := k.IsPodReady(p)
		podString := fmt.Sprintf("  pod name:%s namespace:%s running:%v ready:%v node:%s\n", p.Name, p.Namespace, running, ready, p.Status.HostIP)
		buffer.WriteString(podString)
	}

	return buffer.String()
}

func (k *k8sOps) UpdateDaemonSet(ds *appsv1.DaemonSet) (*appsv1.DaemonSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.AppsV1().DaemonSets(ds.Namespace).Update(ds)
}

func (k *k8sOps) DeleteDaemonSet(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	policy := meta_v1.DeletePropagationForeground
	return k.client.AppsV1().DaemonSets(namespace).Delete(
		name,
		&meta_v1.DeleteOptions{PropagationPolicy: &policy})
}

// DaemonSet APIs - END

// Job APIs - BEGIN
func (k *k8sOps) CreateJob(job *batch_v1.Job) (*batch_v1.Job, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.BatchV1().Jobs(job.Namespace).Create(job)
}

func (k *k8sOps) GetJob(name, namespace string) (*batch_v1.Job, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.BatchV1().Jobs(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) DeleteJob(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.BatchV1().Jobs(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) ValidateJob(name, namespace string, timeout time.Duration) error {
	t := func() (interface{}, bool, error) {
		job, err := k.GetJob(name, namespace)
		if err != nil {
			return nil, true, err
		}

		if job.Status.Failed > 0 {
			return nil, false, fmt.Errorf("job: [%s] %s has %d failed pod(s)", namespace, name, job.Status.Failed)
		}

		if job.Status.Active > 0 {
			return nil, true, fmt.Errorf("job: [%s] %s still has %d active pod(s)", namespace, name, job.Status.Active)
		}

		if job.Status.Succeeded == 0 {
			return nil, true, fmt.Errorf("job: [%s] %s no pod(s) that have succeeded", namespace, name)
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, 10*time.Second); err != nil {
		return err
	}

	return nil
}

// Job APIs - END

// StatefulSet APIs - BEGIN

func (k *k8sOps) ListStatefulSets(namespace string) (*appsv1.StatefulSetList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.AppsV1().StatefulSets(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) GetStatefulSet(name, namespace string) (*appsv1.StatefulSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.AppsV1().StatefulSets(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) CreateStatefulSet(statefulset *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ns := statefulset.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return k.client.AppsV1().StatefulSets(ns).Create(statefulset)
}

func (k *k8sOps) DeleteStatefulSet(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.AppsV1().StatefulSets(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) DescribeStatefulSet(ssetName string, ssetNamespace string) (*appsv1.StatefulSetStatus, error) {
	sset, err := k.GetStatefulSet(ssetName, ssetNamespace)
	if err != nil {
		return nil, err
	}

	return &sset.Status, err
}

func (k *k8sOps) UpdateStatefulSet(statefulset *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.client.AppsV1().StatefulSets(statefulset.Namespace).Update(statefulset)
}

func (k *k8sOps) ValidateStatefulSet(statefulset *appsv1.StatefulSet, timeout time.Duration) error {
	t := func() (interface{}, bool, error) {
		sset, err := k.GetStatefulSet(statefulset.Name, statefulset.Namespace)
		if err != nil {
			return "", true, err
		}

		pods, err := k.GetStatefulSetPods(sset)
		if err != nil || pods == nil {
			return "", true, &ErrAppNotReady{
				ID:    sset.Name,
				Cause: fmt.Sprintf("Failed to get pods for statefulset. Err: %v", err),
			}
		}

		if len(pods) == 0 {
			return "", true, &ErrAppNotReady{
				ID:    sset.Name,
				Cause: "StatefulSet has 0 pods",
			}
		}

		podsOverviewString := k.generatePodsOverviewString(pods)

		if *sset.Spec.Replicas != sset.Status.Replicas { // Not sure if this is even needed but for now let's have one check before
			//readiness check
			return "", true, &ErrAppNotReady{
				ID: sset.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Observed replicas: %v. Current pods overview:\n%s",
					*sset.Spec.Replicas, sset.Status.Replicas, podsOverviewString),
			}
		}

		if *sset.Spec.Replicas != sset.Status.ReadyReplicas {
			return "", true, &ErrAppNotReady{
				ID: sset.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Ready replicas: %v Current pods overview:\n%s",
					*sset.Spec.Replicas, sset.Status.ReadyReplicas, podsOverviewString),
			}
		}

		for _, pod := range pods {
			if !k.IsPodReady(pod) {
				return "", true, &ErrAppNotReady{
					ID:    sset.Name,
					Cause: fmt.Sprintf("Pod: %v is not yet ready", pod.Name),
				}
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, 10*time.Second); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) GetStatefulSetPods(statefulset *appsv1.StatefulSet) ([]v1.Pod, error) {
	return k.GetPodsByOwner(statefulset.UID, statefulset.Namespace)
}

func (k *k8sOps) ValidateTerminatedStatefulSet(statefulset *appsv1.StatefulSet, timeout, timeBeforeRetry time.Duration) error {
	t := func() (interface{}, bool, error) {
		sset, err := k.GetStatefulSet(statefulset.Name, statefulset.Namespace)
		if err != nil {
			if errors.IsNotFound(err) {
				return "", false, nil
			}

			return "", true, err
		}

		pods, err := k.GetStatefulSetPods(statefulset)
		if err != nil {
			return "", true, &ErrAppNotTerminated{
				ID:    sset.Name,
				Cause: fmt.Sprintf("Failed to get pods for statefulset. Err: %v", err),
			}
		}

		if pods != nil && len(pods) > 0 {
			var podNames []string
			for _, pod := range pods {
				podNames = append(podNames, pod.Name)
			}
			return "", true, &ErrAppNotTerminated{
				ID:    sset.Name,
				Cause: fmt.Sprintf("pods: %v are still present", podNames),
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, timeBeforeRetry); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) GetStatefulSetsUsingStorageClass(scName string) ([]appsv1.StatefulSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ss, err := k.client.AppsV1().StatefulSets("").List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var retList []appsv1.StatefulSet
	for _, s := range ss.Items {
		if s.Spec.VolumeClaimTemplates == nil {
			continue
		}

		for _, template := range s.Spec.VolumeClaimTemplates {
			sc, err := k.getStorageClassForPVC(&template)
			if err == nil && sc.Name == scName {
				retList = append(retList, s)
				break
			}
		}
	}

	return retList, nil
}

func (k *k8sOps) GetPVCsForStatefulSet(ss *appsv1.StatefulSet) (*v1.PersistentVolumeClaimList, error) {
	listOptions, err := k.getListOptionsForStatefulSet(ss)
	if err != nil {
		return nil, err
	}

	return k.getPVCsWithListOptions(ss.Namespace, listOptions)
}

func (k *k8sOps) ValidatePVCsForStatefulSet(ss *appsv1.StatefulSet, timeout, retryTimeout time.Duration) error {
	listOptions, err := k.getListOptionsForStatefulSet(ss)
	if err != nil {
		return err
	}

	t := func() (interface{}, bool, error) {
		pvcList, err := k.getPVCsWithListOptions(ss.Namespace, listOptions)
		if err != nil {
			return nil, true, err
		}

		if len(pvcList.Items) < int(*ss.Spec.Replicas) {
			return nil, true, fmt.Errorf("Expected PVCs: %v, Actual: %v", *ss.Spec.Replicas, len(pvcList.Items))
		}

		for _, pvc := range pvcList.Items {
			if err := k.ValidatePersistentVolumeClaim(&pvc, timeout, retryTimeout); err != nil {
				return nil, true, err
			}
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryTimeout); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) getListOptionsForStatefulSet(ss *appsv1.StatefulSet) (meta_v1.ListOptions, error) {
	// TODO: Handle MatchExpressions as well
	labels := ss.Spec.Selector.MatchLabels

	if len(labels) == 0 {
		return meta_v1.ListOptions{}, fmt.Errorf("No labels present to retrieve the PVCs")
	}

	return meta_v1.ListOptions{
		LabelSelector: mapToCSV(labels),
	}, nil
}

// StatefulSet APIs - END

// RBAC APIs - BEGIN

func (k *k8sOps) CreateRole(role *rbac_v1.Role) (*rbac_v1.Role, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.RbacV1().Roles(role.Namespace).Create(role)
}

func (k *k8sOps) UpdateRole(role *rbac_v1.Role) (*rbac_v1.Role, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.RbacV1().Roles(role.Namespace).Update(role)
}

func (k *k8sOps) CreateClusterRole(role *rbac_v1.ClusterRole) (*rbac_v1.ClusterRole, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.RbacV1().ClusterRoles().Create(role)
}

func (k *k8sOps) GetClusterRole(name string) (*rbac_v1.ClusterRole, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.RbacV1().ClusterRoles().Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) UpdateClusterRole(role *rbac_v1.ClusterRole) (*rbac_v1.ClusterRole, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.RbacV1().ClusterRoles().Update(role)
}

func (k *k8sOps) CreateRoleBinding(binding *rbac_v1.RoleBinding) (*rbac_v1.RoleBinding, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.RbacV1().RoleBindings(binding.Namespace).Create(binding)
}

func (k *k8sOps) UpdateRoleBinding(binding *rbac_v1.RoleBinding) (*rbac_v1.RoleBinding, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.RbacV1().RoleBindings(binding.Namespace).Update(binding)
}

func (k *k8sOps) CreateClusterRoleBinding(binding *rbac_v1.ClusterRoleBinding) (*rbac_v1.ClusterRoleBinding, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.RbacV1().ClusterRoleBindings().Create(binding)
}

func (k *k8sOps) UpdateClusterRoleBinding(binding *rbac_v1.ClusterRoleBinding) (*rbac_v1.ClusterRoleBinding, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.RbacV1().ClusterRoleBindings().Update(binding)
}

func (k *k8sOps) GetClusterRoleBinding(name string) (*rbac_v1.ClusterRoleBinding, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.RbacV1().ClusterRoleBindings().Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListClusterRoleBindings() (*rbac_v1.ClusterRoleBindingList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.RbacV1().ClusterRoleBindings().List(meta_v1.ListOptions{})
}

func (k *k8sOps) CreateServiceAccount(account *v1.ServiceAccount) (*v1.ServiceAccount, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().ServiceAccounts(account.Namespace).Create(account)
}

func (k *k8sOps) DeleteRole(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.RbacV1().Roles(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) DeleteClusterRole(roleName string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.RbacV1().ClusterRoles().Delete(roleName, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) DeleteRoleBinding(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.RbacV1().RoleBindings(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) DeleteClusterRoleBinding(bindingName string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.RbacV1().ClusterRoleBindings().Delete(bindingName, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) DeleteServiceAccount(accountName, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.CoreV1().ServiceAccounts(namespace).Delete(accountName, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// RBAC APIs - END

// Pod APIs - BEGIN

func (k *k8sOps) DeletePods(pods []v1.Pod, force bool) error {
	for _, pod := range pods {
		if err := k.DeletePod(pod.Name, pod.Namespace, force); err != nil {
			return err
		}
	}

	return nil
}

func (k *k8sOps) DeletePod(name string, ns string, force bool) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	deleteOptions := meta_v1.DeleteOptions{}
	if force {
		gracePeriodSec := int64(0)
		deleteOptions.GracePeriodSeconds = &gracePeriodSec
	}

	return k.client.CoreV1().Pods(ns).Delete(name, &deleteOptions)
}

func (k *k8sOps) CreatePod(pod *v1.Pod) (*v1.Pod, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().Pods(pod.Namespace).Create(pod)
}

func (k *k8sOps) UpdatePod(pod *v1.Pod) (*v1.Pod, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().Pods(pod.Namespace).Update(pod)
}

func (k *k8sOps) GetPods(namespace string, labelSelector map[string]string) (*v1.PodList, error) {
	return k.getPodsWithListOptions(namespace, meta_v1.ListOptions{
		LabelSelector: mapToCSV(labelSelector),
	})
}

func (k *k8sOps) GetPodsByNode(nodeName, namespace string) (*v1.PodList, error) {
	if len(nodeName) == 0 {
		return nil, fmt.Errorf("node name is required for this API")
	}

	listOptions := meta_v1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	}

	return k.getPodsWithListOptions(namespace, listOptions)
}

func (k *k8sOps) GetPodsByOwner(ownerUID types.UID, namespace string) ([]v1.Pod, error) {
	pods, err := k.GetPods(namespace, nil)
	if err != nil {
		return nil, err
	}

	var result []v1.Pod
	for _, pod := range pods.Items {
		for _, owner := range pod.OwnerReferences {
			if owner.UID == ownerUID {
				result = append(result, pod)
			}
		}
	}

	if len(result) == 0 {
		return nil, ErrPodsNotFound
	}

	return result, nil
}

func (k *k8sOps) GetPodsUsingPV(pvName string) ([]v1.Pod, error) {
	return k.getPodsUsingPVWithListOptions(pvName, meta_v1.ListOptions{})
}

func (k *k8sOps) GetPodsUsingPVByNodeName(pvName, nodeName string) ([]v1.Pod, error) {
	if len(nodeName) == 0 {
		return nil, fmt.Errorf("node name is required for this API")
	}

	listOptions := meta_v1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	}
	return k.getPodsUsingPVWithListOptions(pvName, listOptions)
}

func (k *k8sOps) GetPodsUsingPVC(pvcName, pvcNamespace string) ([]v1.Pod, error) {
	return k.getPodsUsingPVCWithListOptions(pvcName, pvcNamespace, meta_v1.ListOptions{})
}

func (k *k8sOps) GetPodsUsingPVCByNodeName(pvcName, pvcNamespace, nodeName string) ([]v1.Pod, error) {
	if len(nodeName) == 0 {
		return nil, fmt.Errorf("node name is required for this API")
	}

	listOptions := meta_v1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	}
	return k.getPodsUsingPVCWithListOptions(pvcName, pvcNamespace, listOptions)
}

func (k *k8sOps) getPodsWithListOptions(namespace string, opts meta_v1.ListOptions) (*v1.PodList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().Pods(namespace).List(opts)
}

func (k *k8sOps) getPodsUsingPVWithListOptions(pvName string, opts meta_v1.ListOptions) ([]v1.Pod, error) {
	pv, err := k.GetPersistentVolume(pvName)
	if err != nil {
		return nil, err
	}

	if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Kind == "PersistentVolumeClaim" {
		return k.getPodsUsingPVCWithListOptions(pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace, opts)
	}

	return nil, nil
}

func (k *k8sOps) getPodsUsingPVCWithListOptions(pvcName, pvcNamespace string, opts meta_v1.ListOptions) ([]v1.Pod, error) {
	pods, err := k.getPodsWithListOptions(pvcNamespace, opts)
	if err != nil {
		return nil, err
	}

	retList := make([]v1.Pod, 0)
	for _, p := range pods.Items {
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvcName {
				retList = append(retList, p)
				break
			}
		}
	}
	return retList, nil
}

func (k *k8sOps) GetPodsUsingVolumePlugin(plugin string) ([]v1.Pod, error) {
	return k.listPluginPodsWithOptions(meta_v1.ListOptions{}, plugin)
}

func (k *k8sOps) GetPodsUsingVolumePluginByNodeName(nodeName, plugin string) ([]v1.Pod, error) {
	listOptions := meta_v1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	}

	return k.listPluginPodsWithOptions(listOptions, plugin)
}

func (k *k8sOps) listPluginPodsWithOptions(opts meta_v1.ListOptions, plugin string) ([]v1.Pod, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	nodePods, err := k.client.CoreV1().Pods("").List(opts)
	if err != nil {
		return nil, err
	}

	var retList []v1.Pod
	for _, p := range nodePods.Items {
		if ok := k.isAnyVolumeUsingVolumePlugin(p.Spec.Volumes, p.Namespace, plugin); ok {
			retList = append(retList, p)
		}
	}

	return retList, nil
}

func (k *k8sOps) GetPodByName(podName string, namespace string) (*v1.Pod, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	pod, err := k.client.CoreV1().Pods(namespace).Get(podName, meta_v1.GetOptions{})
	if err != nil {
		return nil, ErrPodsNotFound
	}

	return pod, nil
}

func (k *k8sOps) GetPodByUID(uid types.UID, namespace string) (*v1.Pod, error) {
	pods, err := k.GetPods(namespace, nil)
	if err != nil {
		return nil, err
	}

	pUID := types.UID(uid)
	for _, pod := range pods.Items {
		if pod.UID == pUID {
			return &pod, nil
		}
	}

	return nil, ErrPodsNotFound
}

func (k *k8sOps) IsPodRunning(pod v1.Pod) bool {
	// If init containers are running, return false since the actual container would not have started yet
	for _, c := range pod.Status.InitContainerStatuses {
		if c.State.Running != nil {
			return false
		}
	}

	for _, c := range pod.Status.ContainerStatuses {
		if c.State.Running == nil {
			return false
		}
	}

	return true
}

func (k *k8sOps) IsPodReady(pod v1.Pod) bool {
	if pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodSucceeded {
		return false
	}

	// If init containers are running, return false since the actual container would not have started yet
	for _, c := range pod.Status.InitContainerStatuses {
		if c.State.Running != nil {
			return false
		}
	}

	for _, c := range pod.Status.ContainerStatuses {
		if c.State.Terminated != nil &&
			c.State.Terminated.ExitCode == 0 &&
			c.State.Terminated.Reason == "Completed" {
			continue // container has exited successfully
		}

		if c.State.Running == nil {
			return false
		}

		if !c.Ready {
			return false
		}
	}

	return true
}

func (k *k8sOps) IsPodBeingManaged(pod v1.Pod) bool {
	if len(pod.OwnerReferences) == 0 {
		return false
	}

	for _, owner := range pod.OwnerReferences {
		if *owner.Controller {
			// We are assuming that if a pod has a owner who has set itself as
			// a controller, the pod is managed. We are not checking for specific
			// contollers like ReplicaSet, StatefulSet as that is
			// 1) requires changes when new controllers get added
			// 2) not handle customer controllers like operators who create pods
			//    directly
			return true
		}
	}

	return false
}

func (k *k8sOps) ValidatePod(pod *v1.Pod, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		currPod, err := k.GetPodByUID(pod.UID, pod.Namespace)
		if err != nil {
			return "", true, fmt.Errorf("Could not get Pod [%s] %s", pod.Namespace, pod.Name)
		}

		ready := k.IsPodReady(*currPod)
		if !ready {
			return "", true, fmt.Errorf("Pod %s, ID: %s  is not ready. Status %v", currPod.Name, currPod.UID, currPod.Status.Phase)
		}

		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) WatchPods(namespace string, fn WatchFunc, listOptions meta_v1.ListOptions) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	listOptions.Watch = true
	watchInterface, err := k.client.CoreV1().Pods(namespace).Watch(listOptions)
	if err != nil {
		logrus.WithError(err).Error("error invoking the watch api for pods")
		return err
	}

	// fire off watch function
	go k.handleWatch(
		watchInterface,
		&v1.Pod{},
		namespace,
		fn,
		listOptions)

	return nil
}

// Pod APIs - END

// StorageClass APIs - BEGIN

func (k *k8sOps) GetStorageClasses(labelSelector map[string]string) (*storagev1.StorageClassList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.StorageV1().StorageClasses().List(meta_v1.ListOptions{
		LabelSelector: mapToCSV(labelSelector),
	})
}

func (k *k8sOps) GetStorageClass(name string) (*storagev1.StorageClass, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.StorageV1().StorageClasses().Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) CreateStorageClass(sc *storagev1.StorageClass) (*storagev1.StorageClass, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.StorageV1().StorageClasses().Create(sc)
}

func (k *k8sOps) DeleteStorageClass(name string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.StorageV1().StorageClasses().Delete(name, &meta_v1.DeleteOptions{})
}

func (k *k8sOps) GetStorageClassParams(sc *storagev1.StorageClass) (map[string]string, error) {
	sc, err := k.GetStorageClass(sc.Name)
	if err != nil {
		return nil, err
	}

	return sc.Parameters, nil
}

func (k *k8sOps) ValidateStorageClass(name string) (*storagev1.StorageClass, error) {
	return k.GetStorageClass(name)
}

// StorageClass APIs - END

// PVC APIs - BEGIN

func (k *k8sOps) CreatePersistentVolumeClaim(pvc *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ns := pvc.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return k.client.CoreV1().PersistentVolumeClaims(ns).Create(pvc)
}

func (k *k8sOps) UpdatePersistentVolumeClaim(pvc *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ns := pvc.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return k.client.CoreV1().PersistentVolumeClaims(ns).Update(pvc)
}

func (k *k8sOps) DeletePersistentVolumeClaim(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.CoreV1().PersistentVolumeClaims(namespace).Delete(name, &meta_v1.DeleteOptions{})
}

func (k *k8sOps) ValidatePersistentVolumeClaim(pvc *v1.PersistentVolumeClaim, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return "", true, err
		}

		result, err := k.client.CoreV1().
			PersistentVolumeClaims(pvc.Namespace).
			Get(pvc.Name, meta_v1.GetOptions{})
		if err != nil {
			return "", true, err
		}

		if result.Status.Phase == v1.ClaimBound {
			return "", false, nil
		}

		return "", true, &ErrPVCNotReady{
			ID:    result.Name,
			Cause: fmt.Sprintf("PVC expected status: %v PVC actual status: %v", v1.ClaimBound, result.Status.Phase),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) ValidatePersistentVolumeClaimSize(pvc *v1.PersistentVolumeClaim, expectedPVCSize int64, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return "", true, err
		}

		result, err := k.client.CoreV1().
			PersistentVolumeClaims(pvc.Namespace).
			Get(pvc.Name, meta_v1.GetOptions{})
		if err != nil {
			return "", true, err
		}

		capacity, ok := result.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
		if !ok {
			return "", true, fmt.Errorf("failed to get storage size for pvc: %v", pvc.Name)
		}

		if capacity.Value() == expectedPVCSize {
			return "", false, nil
		}

		return "", true, &ErrValidatePVCSize{
			ID:    result.Name,
			Cause: fmt.Sprintf("PVC expected size: %v actual size: %v", expectedPVCSize, capacity.Value()),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) CreatePersistentVolume(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().PersistentVolumes().Create(pv)
}

func (k *k8sOps) GetPersistentVolumeClaim(pvcName string, namespace string) (*v1.PersistentVolumeClaim, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().PersistentVolumeClaims(namespace).
		Get(pvcName, meta_v1.GetOptions{})
}

func (k *k8sOps) GetPersistentVolumeClaims(namespace string, labelSelector map[string]string) (*v1.PersistentVolumeClaimList, error) {
	return k.getPVCsWithListOptions(namespace, meta_v1.ListOptions{
		LabelSelector: mapToCSV(labelSelector),
	})
}

func (k *k8sOps) getPVCsWithListOptions(namespace string, listOpts meta_v1.ListOptions) (*v1.PersistentVolumeClaimList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().PersistentVolumeClaims(namespace).List(listOpts)
}

func (k *k8sOps) GetPersistentVolume(pvName string) (*v1.PersistentVolume, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().PersistentVolumes().Get(pvName, meta_v1.GetOptions{})
}

func (k *k8sOps) DeletePersistentVolume(pvName string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.CoreV1().PersistentVolumes().Delete(pvName, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) GetPersistentVolumes() (*v1.PersistentVolumeList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().PersistentVolumes().List(meta_v1.ListOptions{})
}

func (k *k8sOps) GetVolumeForPersistentVolumeClaim(pvc *v1.PersistentVolumeClaim) (string, error) {
	result, err := k.GetPersistentVolumeClaim(pvc.Name, pvc.Namespace)
	if err != nil {
		return "", err
	}

	return result.Spec.VolumeName, nil
}

func (k *k8sOps) GetPersistentVolumeClaimStatus(pvc *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaimStatus, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	result, err := k.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(pvc.Name, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return &result.Status, nil
}

func (k *k8sOps) GetPersistentVolumeClaimParams(pvc *v1.PersistentVolumeClaim) (map[string]string, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	params := make(map[string]string)

	result, err := k.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(pvc.Name, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}

	capacity, ok := result.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	if !ok {
		return nil, fmt.Errorf("failed to get storage resource for pvc: %v", result.Name)
	}

	// We explicitly send the unit with so the client can compare it with correct units
	requestGB := uint64(roundUpSize(capacity.Value(), 1024*1024*1024))
	params["size"] = fmt.Sprintf("%dG", requestGB)

	sc, err := k.getStorageClassForPVC(result)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage class for pvc: %v", result.Name)
	}

	for key, value := range sc.Parameters {
		params[key] = value
	}

	return params, nil
}

func (k *k8sOps) GetPVCsUsingStorageClass(scName string) ([]v1.PersistentVolumeClaim, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	var retList []v1.PersistentVolumeClaim
	pvcs, err := k.client.CoreV1().PersistentVolumeClaims("").List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, pvc := range pvcs.Items {
		sc, err := k.getStorageClassForPVC(&pvc)
		if err == nil && sc.Name == scName {
			retList = append(retList, pvc)
		}
	}

	return retList, nil
}

func (k *k8sOps) GetStorageProvisionerForPVC(pvc *v1.PersistentVolumeClaim) (string, error) {
	// first try to get the provisioner directly from the annotations
	provisionerName, present := pvc.Annotations[pvcStorageProvisionerKey]
	if present {
		return provisionerName, nil
	}

	sc, err := k.getStorageClassForPVC(pvc)
	if err != nil {
		return "", err
	}

	return sc.Provisioner, nil
}

// isPVCShared returns true if the PersistentVolumeClaim has been configured for use by multiple clients
func (k *k8sOps) isPVCShared(pvc *v1.PersistentVolumeClaim) bool {
	for _, mode := range pvc.Spec.AccessModes {
		if mode == v1.PersistentVolumeAccessMode(v1.ReadOnlyMany) ||
			mode == v1.PersistentVolumeAccessMode(v1.ReadWriteMany) {
			return true
		}
	}

	return false
}

// PVCs APIs - END

// Snapshot APIs - BEGIN

func (k *k8sOps) CreateSnapshot(snap *snap_v1.VolumeSnapshot) (*snap_v1.VolumeSnapshot, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	var result snap_v1.VolumeSnapshot
	if err := k.snapClient.Post().
		Name(snap.Metadata.Name).
		Resource(snap_v1.VolumeSnapshotResourcePlural).
		Namespace(snap.Metadata.Namespace).
		Body(snap).
		Do().Into(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (k *k8sOps) UpdateSnapshot(snap *snap_v1.VolumeSnapshot) (*snap_v1.VolumeSnapshot, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	var result snap_v1.VolumeSnapshot
	if err := k.snapClient.Put().
		Name(snap.Metadata.Name).
		Resource(snap_v1.VolumeSnapshotResourcePlural).
		Namespace(snap.Metadata.Namespace).
		Body(snap).
		Do().Into(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (k *k8sOps) DeleteSnapshot(name string, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}
	return k.snapClient.Delete().
		Name(name).
		Resource(snap_v1.VolumeSnapshotResourcePlural).
		Namespace(namespace).
		Do().Error()
}

func (k *k8sOps) ValidateSnapshot(name string, namespace string, retry bool, timeout, retryInterval time.Duration) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		status, err := k.GetSnapshotStatus(name, namespace)
		if err != nil {
			return "", true, err
		}

		for _, condition := range status.Conditions {
			if condition.Type == snap_v1.VolumeSnapshotConditionReady && condition.Status == v1.ConditionTrue {
				return "", false, nil
			} else if condition.Type == snap_v1.VolumeSnapshotConditionError && condition.Status == v1.ConditionTrue {
				return "", true, &ErrSnapshotFailed{
					ID:    name,
					Cause: fmt.Sprintf("Snapshot Status %v", status),
				}
			}
		}

		return "", true, &ErrSnapshotNotReady{
			ID:    name,
			Cause: fmt.Sprintf("Snapshot Status %v", status),
		}
	}

	if retry {
		if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
			return err
		}
	} else {
		if _, _, err := t(); err != nil {
			return err
		}
	}

	return nil
}

func (k *k8sOps) ValidateSnapshotData(name string, retry bool, timeout, retryInterval time.Duration) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	t := func() (interface{}, bool, error) {
		snapData, err := k.GetSnapshotData(name)
		if err != nil {
			return "", true, err
		}

		for _, condition := range snapData.Status.Conditions {
			if condition.Status == v1.ConditionTrue {
				if condition.Type == snap_v1.VolumeSnapshotDataConditionReady {
					return "", false, nil
				} else if condition.Type == snap_v1.VolumeSnapshotDataConditionError {
					return "", true, &ErrSnapshotDataFailed{
						ID:    name,
						Cause: fmt.Sprintf("SnapshotData Status %v", snapData.Status),
					}
				}
			}
		}

		return "", true, &ErrSnapshotDataNotReady{
			ID:    name,
			Cause: fmt.Sprintf("SnapshotData Status %v", snapData.Status),
		}
	}

	if retry {
		if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
			return err
		}
	} else {
		if _, _, err := t(); err != nil {
			return err
		}
	}

	return nil
}

func (k *k8sOps) GetVolumeForSnapshot(name string, namespace string) (string, error) {
	snapshot, err := k.GetSnapshot(name, namespace)
	if err != nil {
		return "", err
	}

	return snapshot.Metadata.Name, nil
}

func (k *k8sOps) GetSnapshot(name string, namespace string) (*snap_v1.VolumeSnapshot, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	var result snap_v1.VolumeSnapshot
	if err := k.snapClient.Get().
		Name(name).
		Resource(snap_v1.VolumeSnapshotResourcePlural).
		Namespace(namespace).
		Do().Into(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (k *k8sOps) ListSnapshots(namespace string) (*snap_v1.VolumeSnapshotList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	var result snap_v1.VolumeSnapshotList
	if err := k.snapClient.Get().
		Resource(snap_v1.VolumeSnapshotResourcePlural).
		Namespace(namespace).
		Do().Into(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (k *k8sOps) GetSnapshotStatus(name string, namespace string) (*snap_v1.VolumeSnapshotStatus, error) {
	snapshot, err := k.GetSnapshot(name, namespace)
	if err != nil {
		return nil, err
	}

	return &snapshot.Status, nil
}

func (k *k8sOps) GetSnapshotData(name string) (*snap_v1.VolumeSnapshotData, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	var result snap_v1.VolumeSnapshotData
	if err := k.snapClient.Get().
		Name(name).
		Resource(snap_v1.VolumeSnapshotDataResourcePlural).
		Do().Into(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (k *k8sOps) CreateSnapshotData(snapData *snap_v1.VolumeSnapshotData) (*snap_v1.VolumeSnapshotData, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	var result snap_v1.VolumeSnapshotData
	if err := k.snapClient.Post().
		Name(snapData.Metadata.Name).
		Resource(snap_v1.VolumeSnapshotDataResourcePlural).
		Body(snapData).
		Do().Into(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (k *k8sOps) DeleteSnapshotData(name string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}
	return k.snapClient.Delete().
		Name(name).
		Resource(snap_v1.VolumeSnapshotDataResourcePlural).
		Do().Error()
}

// Snapshot APIs - END

// Secret APIs - BEGIN

func (k *k8sOps) GetSecret(name string, namespace string) (*v1.Secret, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().Secrets(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) CreateSecret(secret *v1.Secret) (*v1.Secret, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().Secrets(secret.Namespace).Create(secret)
}

func (k *k8sOps) UpdateSecret(secret *v1.Secret) (*v1.Secret, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().Secrets(secret.Namespace).Update(secret)
}

func (k *k8sOps) UpdateSecretData(name string, ns string, data map[string][]byte) (*v1.Secret, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	secret, err := k.GetSecret(name, ns)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return k.CreateSecret(
				&v1.Secret{
					ObjectMeta: meta_v1.ObjectMeta{
						Name:      name,
						Namespace: ns,
					},
					Data: data,
				})
		}
		return nil, err
	}

	// This only adds/updates the key value pairs; does not remove the existing.
	for k, v := range data {
		secret.Data[k] = v
	}
	return k.UpdateSecret(secret)
}

func (k *k8sOps) DeleteSecret(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.CoreV1().Secrets(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// Secret APIs - END

// ConfigMap APIs - BEGIN

func (k *k8sOps) GetConfigMap(name string, namespace string) (*v1.ConfigMap, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().ConfigMaps(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) CreateConfigMap(configMap *v1.ConfigMap) (*v1.ConfigMap, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ns := configMap.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return k.client.CoreV1().ConfigMaps(ns).Create(configMap)
}

func (k *k8sOps) DeleteConfigMap(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	if len(namespace) == 0 {
		namespace = v1.NamespaceDefault
	}

	return k.client.CoreV1().ConfigMaps(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) UpdateConfigMap(configMap *v1.ConfigMap) (*v1.ConfigMap, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ns := configMap.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return k.client.CoreV1().ConfigMaps(ns).Update(configMap)
}

func (k *k8sOps) WatchConfigMap(configMap *v1.ConfigMap, fn WatchFunc) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	listOptions := meta_v1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", configMap.Name).String(),
		Watch:         true,
	}

	watchInterface, err := k.client.CoreV1().ConfigMaps(configMap.Namespace).Watch(listOptions)
	if err != nil {
		logrus.WithError(err).Error("error invoking the watch api for config maps")
		return err
	}

	// fire off watch function
	go k.handleWatch(watchInterface, configMap, "", fn, listOptions)
	return nil
}

// ConfigMap APIs - END

// Event APIs - BEGIN
// CreateEvent puts an event into k8s etcd
func (k *k8sOps) CreateEvent(event *v1.Event) (*v1.Event, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.client.CoreV1().Events(event.Namespace).Create(event)
}

// ListEvents retrieves all events registered with kubernetes
func (k *k8sOps) ListEvents(namespace string, opts meta_v1.ListOptions) (*v1.EventList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.client.CoreV1().Events(namespace).List(opts)
}

// Event APIs - END

// Object APIs - BEGIN

func (k *k8sOps) getDynamicClient(object runtime.Object) (dynamic.ResourceInterface, error) {

	objectType, err := meta.TypeAccessor(object)
	if err != nil {
		return nil, err
	}

	metadata, err := meta.Accessor(object)
	if err != nil {
		return nil, err
	}

	resourceInterface := k.dynamicInterface.Resource(object.GetObjectKind().GroupVersionKind().GroupVersion().WithResource(strings.ToLower(objectType.GetKind()) + "s"))
	if metadata.GetNamespace() == "" {
		return resourceInterface, nil
	}
	return resourceInterface.Namespace(metadata.GetNamespace()), nil
}

// GetObject returns the latest object given a generic Object
func (k *k8sOps) GetObject(object runtime.Object) (runtime.Object, error) {
	client, err := k.getDynamicClient(object)
	if err != nil {
		return nil, err
	}

	metadata, err := meta.Accessor(object)
	if err != nil {
		return nil, err
	}
	return client.Get(metadata.GetName(), meta_v1.GetOptions{}, "")
}

// UpdateObject updates a generic Object
func (k *k8sOps) UpdateObject(object runtime.Object) (runtime.Object, error) {
	unstructured, ok := object.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("Unable to cast object to unstructured: %v", object)
	}

	client, err := k.getDynamicClient(object)
	if err != nil {
		return nil, err
	}

	return client.Update(unstructured, meta_v1.UpdateOptions{}, "")
}

// Object APIs - END

// VolumeAttachment APIs - START

func (k *k8sOps) ListVolumeAttachments() (*storagev1beta1.VolumeAttachmentList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.StorageV1beta1().VolumeAttachments().List(meta_v1.ListOptions{})
}

func (k *k8sOps) DeleteVolumeAttachment(name string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.StorageV1beta1().VolumeAttachments().Delete(name, &meta_v1.DeleteOptions{})
}

func (k *k8sOps) CreateVolumeAttachment(volumeAttachment *storagev1beta1.VolumeAttachment) (*storagev1beta1.VolumeAttachment, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.StorageV1beta1().VolumeAttachments().Create(volumeAttachment)
}

func (k *k8sOps) UpdateVolumeAttachment(volumeAttachment *storagev1beta1.VolumeAttachment) (*storagev1beta1.VolumeAttachment, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.StorageV1beta1().VolumeAttachments().Update(volumeAttachment)
}

func (k *k8sOps) UpdateVolumeAttachmentStatus(volumeAttachment *storagev1beta1.VolumeAttachment) (*storagev1beta1.VolumeAttachment, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.StorageV1beta1().VolumeAttachments().UpdateStatus(volumeAttachment)
}

// VolumeAttachment APIs - END

// isAnyVolumeUsingVolumePlugin returns true if any of the given volumes is using a storage class for the given plugin
//	In case errors are found while looking up a particular volume, the function ignores the errors as the goal is to
//	find if there is any match or not
func (k *k8sOps) isAnyVolumeUsingVolumePlugin(volumes []v1.Volume, volumeNamespace, plugin string) bool {
	for _, v := range volumes {
		if v.PersistentVolumeClaim != nil {
			pvc, err := k.GetPersistentVolumeClaim(v.PersistentVolumeClaim.ClaimName, volumeNamespace)
			if err == nil && pvc != nil {
				provisioner, err := k.GetStorageProvisionerForPVC(pvc)
				if err == nil {
					if provisioner == plugin {
						return true
					}
				}
			}
		}
	}

	return false
}

func (k *k8sOps) getStorageClassForPVC(pvc *v1.PersistentVolumeClaim) (*storagev1.StorageClass, error) {
	var scName string
	if pvc.Spec.StorageClassName != nil && len(*pvc.Spec.StorageClassName) > 0 {
		scName = *pvc.Spec.StorageClassName
	} else {
		scName = pvc.Annotations[pvcStorageClassKey]
	}

	if len(scName) == 0 {
		return nil, fmt.Errorf("PVC: %s does not have a storage class", pvc.Name)
	}

	return k.GetStorageClass(scName)
}
