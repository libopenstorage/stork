package k8s

import (
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	snap_v1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snap_client "github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	apps_api "k8s.io/api/apps/v1beta2"
	batch_v1 "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
	rbac_v1 "k8s.io/api/rbac/v1"
	storage_api "k8s.io/api/storage/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/typed/apps/v1beta2"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	masterLabelKey           = "node-role.kubernetes.io/master"
	hostnameKey              = "kubernetes.io/hostname"
	pvcStorageClassKey       = "volume.beta.kubernetes.io/storage-class"
	pvcStorageProvisionerKey = "volume.beta.kubernetes.io/storage-provisioner"
	labelUpdateMaxRetries    = 5
	nodeUpdateTimeout        = 1 * time.Minute
	nodeUpdateRetryInterval  = 2 * time.Second
	deploymentReadyTimeout   = 10 * time.Minute
)

var deleteForegroundPolicy = meta_v1.DeletePropagationForeground

var (
	// ErrPodsNotFound error returned when pod or pods could not be found
	ErrPodsNotFound = fmt.Errorf("Pod(s) not found")
)

// Ops is an interface to perform any kubernetes related operations
type Ops interface {
	NamespaceOps
	NodeOps
	ServiceOps
	StatefulSetOps
	DeploymentOps
	JobOps
	DaemonSetOps
	RBACOps
	PodOps
	StorageClassOps
	PersistentVolumeClaimOps
	SnapshotOps
	SecretOps
	ConfigMapOps
}

// NamespaceOps is an interface to perform namespace operations
type NamespaceOps interface {
	// CreateNamespace creates a namespace with given name and metadata
	CreateNamespace(string, map[string]string) (*v1.Namespace, error)
	// DeleteNamespace deletes a namespace with given name
	DeleteNamespace(string) error
}

// NodeOps is an interface to perform k8s node operations
type NodeOps interface {
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
	WatchNode(node *v1.Node, fn NodeWatchFunc) error
	// CordonNode cordons the given node
	CordonNode(nodeName string) error
	// UnCordonNode uncordons the given node
	UnCordonNode(nodeName string) error
	// DrainPodsFromNode drains given pods from given node. If timeout is set to
	// a non-zero value, it waits for timeout duration for each pod to get deleted
	DrainPodsFromNode(nodeName string, pods []v1.Pod, timeout time.Duration) error
}

// ServiceOps is an interface to perform k8s service operations
type ServiceOps interface {
	// GetService gets the service by the name
	GetService(string, string) (*v1.Service, error)
	// CreateService creates the given service
	CreateService(*v1.Service) (*v1.Service, error)
	// DeleteService deletes the given service
	DeleteService(*v1.Service) error
	// ValidateDeletedService validates if given service is deleted
	ValidateDeletedService(string, string) error
	// DescribeService gets the service status
	DescribeService(string, string) (*v1.ServiceStatus, error)
}

// StatefulSetOps is an interface to perform k8s stateful set operations
type StatefulSetOps interface {
	// CreateStatefulSet creates the given statefulset
	CreateStatefulSet(*apps_api.StatefulSet) (*apps_api.StatefulSet, error)
	// DeleteStatefulSet deletes the given statefulset
	DeleteStatefulSet(*apps_api.StatefulSet) error
	// ValidateStatefulSet validates the given statefulset if it's running and healthy
	ValidateStatefulSet(*apps_api.StatefulSet) error
	// ValidateTerminatedStatefulSet validates if given deployment is terminated
	ValidateTerminatedStatefulSet(*apps_api.StatefulSet) error
	// GetStatefulSetPods returns pods for the given statefulset
	GetStatefulSetPods(*apps_api.StatefulSet) ([]v1.Pod, error)
	// DescribeStatefulSet gets status of the statefulset
	DescribeStatefulSet(string, string) (*apps_api.StatefulSetStatus, error)
	// GetStatefulSetsUsingStorageClass returns all statefulsets using given storage class
	GetStatefulSetsUsingStorageClass(scName string) ([]apps_api.StatefulSet, error)
}

// DeploymentOps is an interface to perform k8s deployment operations
type DeploymentOps interface {
	// CreateDeployment creates the given deployment
	CreateDeployment(*apps_api.Deployment) (*apps_api.Deployment, error)
	// DeleteDeployment deletes the given deployment
	DeleteDeployment(*apps_api.Deployment) error
	// ValidateDeployment validates the given deployment if it's running and healthy
	ValidateDeployment(*apps_api.Deployment) error
	// ValidateTerminatedDeployment validates if given deployment is terminated
	ValidateTerminatedDeployment(*apps_api.Deployment) error
	// GetDeploymentPods returns pods for the given deployment
	GetDeploymentPods(*apps_api.Deployment) ([]v1.Pod, error)
	// DescribeDeployment gets the deployment status
	DescribeDeployment(string, string) (*apps_api.DeploymentStatus, error)
	// GetDeploymentsUsingStorageClass returns all deployments using the given storage class
	GetDeploymentsUsingStorageClass(scName string) ([]apps_api.Deployment, error)
}

// DaemonSetOps is an interface to perform k8s daemon set operations
type DaemonSetOps interface {
	// CreateDaemonSet creates the given daemonset
	CreateDaemonSet(ds *apps_api.DaemonSet) (*apps_api.DaemonSet, error)
	// ListDaemonSets lists all daemonsets in given namespace
	ListDaemonSets(namespace string, listOpts meta_v1.ListOptions) ([]apps_api.DaemonSet, error)
	// GetDaemonSet gets the the daemon set with given name
	GetDaemonSet(string, string) (*apps_api.DaemonSet, error)
	// ValidateDaemonSet checks if the given daemonset is ready within given timeout
	ValidateDaemonSet(name, namespace string, timeout time.Duration) error
	// GetDaemonSetPods returns list of pods for the daemonset
	GetDaemonSetPods(*apps_api.DaemonSet) ([]v1.Pod, error)
	// UpdateDaemonSet updates the given daemon set and returns the updated ds
	UpdateDaemonSet(*apps_api.DaemonSet) (*apps_api.DaemonSet, error)
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
	// CreateClusterRole creates the given cluster role
	CreateClusterRole(role *rbac_v1.ClusterRole) (*rbac_v1.ClusterRole, error)
	// UpdateClusterRole updates the given cluster role
	UpdateClusterRole(role *rbac_v1.ClusterRole) (*rbac_v1.ClusterRole, error)
	// CreateClusterRoleBinding creates the given cluster role binding
	CreateClusterRoleBinding(role *rbac_v1.ClusterRoleBinding) (*rbac_v1.ClusterRoleBinding, error)
	// CreateServiceAccount creates the given service account
	CreateServiceAccount(account *v1.ServiceAccount) (*v1.ServiceAccount, error)
	// DeleteClusterRole deletes the given cluster role
	DeleteClusterRole(roleName string) error
	// DeleteClusterRoleBinding deletes the given cluster role binding
	DeleteClusterRoleBinding(roleName string) error
	// DeleteServiceAccount deletes the given service account
	DeleteServiceAccount(accountName, namespace string) error
}

// PodOps is an interface to perform k8s pod operations
type PodOps interface {
	// GetPods returns pods for the given namespace
	GetPods(string) (*v1.PodList, error)
	// GetPodsByOwner returns pods for the given owner and namespace
	GetPodsByOwner(types.UID, string) ([]v1.Pod, error)
	// GetPodsUsingVolumePluginByNodeName returns all pods who use PVCs provided by the given volume plugin
	GetPodsUsingVolumePluginByNodeName(nodeName, plugin string) ([]v1.Pod, error)
	// GetPodByUID returns pod with the given UID, or error if nothing found
	GetPodByUID(types.UID, string) (*v1.Pod, error)
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
}

// StorageClassOps is an interface to perform k8s storage class operations
type StorageClassOps interface {
	// CreateStorageClass creates the given storage class
	CreateStorageClass(*storage_api.StorageClass) (*storage_api.StorageClass, error)
	// GetStorageClass returns the storage class for the given name
	GetStorageClass(string) (*storage_api.StorageClass, error)
	// DeleteStorageClass deletes the given storage class
	DeleteStorageClass(string) error
	// GetStorageClassParams returns the parameters of the given sc in the native map format
	GetStorageClassParams(*storage_api.StorageClass) (map[string]string, error)
	// ValidateStorageClass validates the given storage class
	ValidateStorageClass(string) (*storage_api.StorageClass, error)
}

// PersistentVolumeClaimOps is an interface to perform k8s PVC operations
type PersistentVolumeClaimOps interface {
	// CreatePersistentVolumeClaim creates the given persistent volume claim
	CreatePersistentVolumeClaim(*v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error)
	// DeletePersistentVolumeClaim deletes the given persistent volume claim
	DeletePersistentVolumeClaim(*v1.PersistentVolumeClaim) error
	// ValidatePersistentVolumeClaim validates the given pvc
	ValidatePersistentVolumeClaim(*v1.PersistentVolumeClaim) error
	// GetPersistentVolumeClaim returns the PVC for given name and namespace
	GetPersistentVolumeClaim(pvcName string, namespace string) (*v1.PersistentVolumeClaim, error)
	// GetVolumeForPersistentVolumeClaim returns the volumeID for the given PVC
	GetVolumeForPersistentVolumeClaim(*v1.PersistentVolumeClaim) (string, error)
	// GetPersistentVolumeClaimParams fetches custom parameters for the given PVC
	GetPersistentVolumeClaimParams(*v1.PersistentVolumeClaim) (map[string]string, error)
	// GetPersistentVolumeClaimStatus returns the status of the given pvc
	GetPersistentVolumeClaimStatus(*v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaimStatus, error)
	// GetPVCsUsingStorageClass returns all PVCs that use the given storage class
	GetPVCsUsingStorageClass(scName string) ([]v1.PersistentVolumeClaim, error)
}

type SnapshotOps interface {
	// CreateSnapshot creates the given snapshot
	CreateSnapshot(*snap_v1.VolumeSnapshot) (*snap_v1.VolumeSnapshot, error)
	// DeleteSnapshot deletes the given snapshot
	DeleteSnapshot(name string, namespace string) error
	// ValidateSnapshot validates the given snapshot
	ValidateSnapshot(name string, namespace string) error
	// GetVolumeForSnapshot returns the volumeID for the given snapshot
	GetVolumeForSnapshot(name string, namespace string) (string, error)
	// GetSnapshotStatus returns the status of the given snapshot
	GetSnapshotStatus(name string, namespace string) (*snap_v1.VolumeSnapshotStatus, error)
}

type SecretOps interface {
	// GetSecret gets the secrets object given its name and namespace
	GetSecret(name string, namespace string) (*v1.Secret, error)
	// CreateSecret creates the given secret
	CreateSecret(*v1.Secret) (*v1.Secret, error)
	// UpdateSecret updates the gives secret
	UpdateSecret(*v1.Secret) (*v1.Secret, error)
	// UpdateSecretData updates or creates a new secret with the given data
	UpdateSecretData(string, string, map[string][]byte) (*v1.Secret, error)
}

type ConfigMapOps interface {
	// GetConfigMap gets the config map object for the given name and namespace
	GetConfigMap(name string, namespace string) (*v1.ConfigMap, error)
	// CreateConfigMap creates a new config map object if it does not already exist.
	CreateConfigMap(configMap *v1.ConfigMap) (*v1.ConfigMap, error)
	// UpdateConfigMap updates the given config map object
	UpdateConfigMap(configMap *v1.ConfigMap) (*v1.ConfigMap, error)
}

var (
	instance Ops
	once     sync.Once
)

type k8sOps struct {
	client     *kubernetes.Clientset
	snapClient *rest.RESTClient
}

// Instance returns a singleton instance of k8sOps type
func Instance() Ops {
	once.Do(func() {
		instance = &k8sOps{}
	})
	return instance
}

// Initialize the k8s client if uninitialized
func (k *k8sOps) initK8sClient() error {
	if k.client == nil {
		k8sClient, snapClient, err := getK8sClient()
		if err != nil {
			return err
		}

		// Quick validation if client connection works
		_, err = k8sClient.ServerVersion()
		if err != nil {
			return fmt.Errorf("failed to connect to k8s server: %s", err)
		}

		k.client = k8sClient
		k.snapClient = snapClient
	}
	return nil
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
			if condition.Status != v1.ConditionStatus(v1.ConditionFalse) {
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

// NodeWatchFunc is a callback provided to the WatchNode function
// which is invoked when the v1.Node object is changed.
type NodeWatchFunc func(node *v1.Node) error

// handleWatch is internal function that handles the Node-watch.  On channel shutdown (ie. stop watch),
// it'll attempt to reestablish its watch function.
func (k *k8sOps) handleWatch(watchInterface watch.Interface, node *v1.Node, watchNodeFn NodeWatchFunc) {
	for {
		select {
		case event, more := <-watchInterface.ResultChan():
			if !more {
				logrus.Debug("Kubernetes NodeWatch closed (attempting to reestablish)")

				t := func() (interface{}, bool, error) {
					err := k.WatchNode(node, watchNodeFn)
					return "", true, err
				}
				if _, err := task.DoRetryWithTimeout(t, 10*time.Minute, 10*time.Second); err != nil {
					logrus.WithError(err).Error("Could not reestablish the NodeWatch")
				} else {
					logrus.Debug("NodeWatch reestablished")
				}
				return
			}
			if k8sNode, ok := event.Object.(*v1.Node); ok {
				// CHECKME: handle errors?
				watchNodeFn(k8sNode)
			}
		}
	}
}

func (k *k8sOps) WatchNode(node *v1.Node, watchNodeFn NodeWatchFunc) error {
	if node == nil {
		return fmt.Errorf("no node given to watch")
	}

	if err := k.initK8sClient(); err != nil {
		return err
	}

	nodeHostname, has := node.GetLabels()[hostnameKey]
	if !has || nodeHostname == "" {
		return fmt.Errorf("no hostname label")
	}

	requirement, err := labels.NewRequirement(
		hostnameKey,
		selection.DoubleEquals,
		[]string{nodeHostname},
	)
	if err != nil {
		return fmt.Errorf("Could not create Label requirement: %s", err)
	}

	listOptions := meta_v1.ListOptions{
		LabelSelector: requirement.String(),
		Watch:         true,
	}

	watchInterface, err := k.client.Core().Nodes().Watch(listOptions)
	if err != nil {
		return err
	}

	// fire off watch function
	go k.handleWatch(watchInterface, node, watchNodeFn)
	return nil
}

func (k *k8sOps) CordonNode(nodeName string) error {
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

	if _, err := task.DoRetryWithTimeout(t, nodeUpdateTimeout, nodeUpdateRetryInterval); err != nil {
		return err
	}

	return nil
}

func (k *k8sOps) UnCordonNode(nodeName string) error {
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

	if _, err := task.DoRetryWithTimeout(t, nodeUpdateTimeout, nodeUpdateRetryInterval); err != nil {
		return err
	}

	return nil
}

func (k *k8sOps) DrainPodsFromNode(nodeName string, pods []v1.Pod, timeout time.Duration) error {
	err := k.CordonNode(nodeName)
	if err != nil {
		return err
	}

	err = k.DeletePods(pods, false)
	if err != nil {
		e := k.UnCordonNode(nodeName) // rollback cordon
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

func (k *k8sOps) DeleteService(service *v1.Service) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.CoreV1().Services(service.Namespace).Delete(service.Name, &meta_v1.DeleteOptions{
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
	svc, err := k.client.CoreV1().Services(svcNS).Get(svcName, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return svc, nil

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
		if matched, _ := regexp.MatchString(".+ not found", err.Error()); matched {
			return nil
		}
		return err
	}

	return nil
}

// Service APIs - END

// Deployment APIs - BEGIN

func (k *k8sOps) CreateDeployment(deployment *apps_api.Deployment) (*apps_api.Deployment, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ns := deployment.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return k.appsClient().Deployments(ns).Create(deployment)
}

func (k *k8sOps) DeleteDeployment(deployment *apps_api.Deployment) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.appsClient().Deployments(deployment.Namespace).Delete(deployment.Name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) DescribeDeployment(depName string, depNamespace string) (*apps_api.DeploymentStatus, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	dep, err := k.appsClient().Deployments(depNamespace).Get(depName, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return &dep.Status, err
}

func (k *k8sOps) ValidateDeployment(deployment *apps_api.Deployment) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return "", true, err
		}

		dep, err := k.appsClient().Deployments(deployment.Namespace).Get(deployment.Name, meta_v1.GetOptions{})
		if err != nil {
			return "", true, err
		}

		requiredReplicas := *dep.Spec.Replicas

		if requiredReplicas != 1 {
			shared := false
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

		if requiredReplicas > dep.Status.AvailableReplicas {
			return "", true, &ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Available replicas: %v",
					requiredReplicas, dep.Status.AvailableReplicas),
			}
		}

		if requiredReplicas > dep.Status.ReadyReplicas {
			return "", true, &ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Ready replicas: %v",
					requiredReplicas, dep.Status.ReadyReplicas),
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
				Cause: "Application has 0 pods",
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

		if readyCount == requiredReplicas {
			return "", false, nil
		}

		return "", true, &ErrAppNotReady{
			ID:    dep.Name,
			Cause: fmt.Sprintf("pod(s): %#v not yet ready", notReadyPods),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, deploymentReadyTimeout, 10*time.Second); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) ValidateTerminatedDeployment(deployment *apps_api.Deployment) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return "", true, err
		}

		dep, err := k.appsClient().Deployments(deployment.Namespace).Get(deployment.Name, meta_v1.GetOptions{})
		if err != nil {
			if matched, _ := regexp.MatchString(".+ not found", err.Error()); matched {
				return "", true, nil
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
			return "", true, &ErrAppNotTerminated{
				ID:    dep.Name,
				Cause: fmt.Sprintf("pods: %#v is still present", pods),
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, 10*time.Minute, 10*time.Second); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) GetDeploymentPods(deployment *apps_api.Deployment) ([]v1.Pod, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	rSets, err := k.appsClient().ReplicaSets(deployment.Namespace).List(meta_v1.ListOptions{})
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

func (k *k8sOps) GetDeploymentsUsingStorageClass(scName string) ([]apps_api.Deployment, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	deps, err := k.appsClient().Deployments("").List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var retList []apps_api.Deployment
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

func (k *k8sOps) CreateDaemonSet(ds *apps_api.DaemonSet) (*apps_api.DaemonSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.Apps().DaemonSets(ds.Namespace).Create(ds)
}

func (k *k8sOps) ListDaemonSets(namespace string, listOpts meta_v1.ListOptions) ([]apps_api.DaemonSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	dsList, err := k.client.Apps().DaemonSets(namespace).List(listOpts)
	if err != nil {
		return nil, err
	}

	return dsList.Items, nil
}

func (k *k8sOps) GetDaemonSet(name, namespace string) (*apps_api.DaemonSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	if len(namespace) == 0 {
		namespace = v1.NamespaceDefault
	}

	ds, err := k.appsClient().DaemonSets(namespace).Get(name, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return ds, nil
}

func (k *k8sOps) GetDaemonSetPods(ds *apps_api.DaemonSet) ([]v1.Pod, error) {
	return k.GetPodsByOwner(ds.UID, ds.Namespace)
}

func (k *k8sOps) ValidateDaemonSet(name, namespace string, timeout time.Duration) error {
	t := func() (interface{}, bool, error) {
		ds, err := k.GetDaemonSet(name, namespace)
		if err != nil {
			return "", true, err
		}

		if ds.Status.DesiredNumberScheduled != ds.Status.UpdatedNumberScheduled {
			return "", true, &ErrAppNotReady{
				ID: name,
				Cause: fmt.Sprintf("Not all pods are updated. expected: %v updated: %v",
					ds.Status.DesiredNumberScheduled, ds.Status.UpdatedNumberScheduled),
			}
		}

		if ds.Status.NumberUnavailable > 0 {
			return "", true, &ErrAppNotReady{
				ID: name,
				Cause: fmt.Sprintf("%d pods are not available. available: %d ready: %d updated: %d",
					ds.Status.NumberUnavailable, ds.Status.NumberAvailable,
					ds.Status.NumberReady, ds.Status.UpdatedNumberScheduled),
			}
		}

		if ds.Status.DesiredNumberScheduled != ds.Status.NumberReady {
			return "", true, &ErrAppNotReady{
				ID: name,
				Cause: fmt.Sprintf("expected ready: %v actual ready: %v",
					ds.Status.DesiredNumberScheduled, ds.Status.NumberReady),
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
				Cause: "Application has 0 pods",
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
			Cause: fmt.Sprintf("pod(s): %#v not yet ready", notReadyPods),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, 10*time.Second); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) UpdateDaemonSet(ds *apps_api.DaemonSet) (*apps_api.DaemonSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.appsClient().DaemonSets(ds.Namespace).Update(ds)
}

func (k *k8sOps) DeleteDaemonSet(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	policy := meta_v1.DeletePropagationForeground
	return k.client.Apps().DaemonSets(namespace).Delete(
		name,
		&meta_v1.DeleteOptions{PropagationPolicy: &policy})
}

// DaemonSet APIs - END

// Job APIs - BEGIN
func (k *k8sOps) CreateJob(job *batch_v1.Job) (*batch_v1.Job, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.Batch().Jobs(job.Namespace).Create(job)
}

func (k *k8sOps) GetJob(name, namespace string) (*batch_v1.Job, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.Batch().Jobs(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) DeleteJob(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.Batch().Jobs(namespace).Delete(name, &meta_v1.DeleteOptions{
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

func (k *k8sOps) CreateStatefulSet(statefulset *apps_api.StatefulSet) (*apps_api.StatefulSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ns := statefulset.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return k.appsClient().StatefulSets(ns).Create(statefulset)
}

func (k *k8sOps) DeleteStatefulSet(statefulset *apps_api.StatefulSet) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.appsClient().StatefulSets(statefulset.Namespace).Delete(statefulset.Name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) DescribeStatefulSet(ssetName string, ssetNamespace string) (*apps_api.StatefulSetStatus, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	sset, err := k.appsClient().StatefulSets(ssetNamespace).Get(ssetName, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return &sset.Status, err
}

func (k *k8sOps) ValidateStatefulSet(statefulset *apps_api.StatefulSet) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return "", true, err
		}
		sset, err := k.appsClient().StatefulSets(statefulset.Namespace).Get(statefulset.Name, meta_v1.GetOptions{})
		if err != nil {
			return "", true, err
		}

		if *sset.Spec.Replicas != sset.Status.Replicas { // Not sure if this is even needed but for now let's have one check before
			//readiness check
			return "", true, &ErrAppNotReady{
				ID:    sset.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Observed replicas: %v", *sset.Spec.Replicas, sset.Status.Replicas),
			}
		}

		if *sset.Spec.Replicas != sset.Status.ReadyReplicas {
			return "", true, &ErrAppNotReady{
				ID:    sset.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Ready replicas: %v", *sset.Spec.Replicas, sset.Status.ReadyReplicas),
			}
		}

		pods, err := k.GetStatefulSetPods(statefulset)
		if err != nil || pods == nil {
			return "", true, &ErrAppNotReady{
				ID:    sset.Name,
				Cause: fmt.Sprintf("Failed to get pods for statefulset. Err: %v", err),
			}
		}

		for _, pod := range pods {
			if !k.IsPodReady(pod) {
				return "", true, &ErrAppNotReady{
					ID:    sset.Name,
					Cause: fmt.Sprintf("pod: %v is not yet ready", pod.Name),
				}
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, 10*time.Minute, 10*time.Second); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) GetStatefulSetPods(statefulset *apps_api.StatefulSet) ([]v1.Pod, error) {
	return k.GetPodsByOwner(statefulset.UID, statefulset.Namespace)
}

func (k *k8sOps) ValidateTerminatedStatefulSet(statefulset *apps_api.StatefulSet) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return "", true, err
		}

		sset, err := k.appsClient().StatefulSets(statefulset.Namespace).Get(statefulset.Name, meta_v1.GetOptions{})
		if err != nil {
			if matched, _ := regexp.MatchString(".+ not found", err.Error()); matched {
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
			return "", true, &ErrAppNotTerminated{
				ID:    sset.Name,
				Cause: fmt.Sprintf("pods: %#v is still present", pods),
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, 10*time.Minute, 10*time.Second); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) GetStatefulSetsUsingStorageClass(scName string) ([]apps_api.StatefulSet, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ss, err := k.appsClient().StatefulSets("").List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var retList []apps_api.StatefulSet
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

// StatefulSet APIs - END

func (k *k8sOps) CreateClusterRole(role *rbac_v1.ClusterRole) (*rbac_v1.ClusterRole, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.Rbac().ClusterRoles().Create(role)
}

func (k *k8sOps) UpdateClusterRole(role *rbac_v1.ClusterRole) (*rbac_v1.ClusterRole, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.Rbac().ClusterRoles().Update(role)
}

func (k *k8sOps) CreateClusterRoleBinding(binding *rbac_v1.ClusterRoleBinding) (*rbac_v1.ClusterRoleBinding, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.Rbac().ClusterRoleBindings().Create(binding)
}

func (k *k8sOps) CreateServiceAccount(account *v1.ServiceAccount) (*v1.ServiceAccount, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.Core().ServiceAccounts(account.Namespace).Create(account)
}

func (k *k8sOps) DeleteClusterRole(roleName string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.Rbac().ClusterRoles().Delete(roleName, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) DeleteClusterRoleBinding(bindingName string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.Rbac().ClusterRoleBindings().Delete(bindingName, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) DeleteServiceAccount(accountName, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.Core().ServiceAccounts(namespace).Delete(accountName, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) DeletePods(pods []v1.Pod, force bool) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	deleteOptions := meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	}
	if force {
		gracePeriodSec := int64(0)
		deleteOptions.GracePeriodSeconds = &gracePeriodSec

	}
	for _, pod := range pods {
		if err := k.client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &deleteOptions); err != nil {
			return err
		}
	}

	return nil
}

func (k *k8sOps) GetPods(namespace string) (*v1.PodList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().Pods(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) GetPodsByOwner(ownerUID types.UID, namespace string) ([]v1.Pod, error) {
	pods, err := k.GetPods(namespace)
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

func (k *k8sOps) GetPodsUsingVolumePluginByNodeName(nodeName, plugin string) ([]v1.Pod, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	listOptions := meta_v1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	}

	nodePods, err := k.client.CoreV1().Pods("").List(listOptions)
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

func (k *k8sOps) GetPodByUID(uid types.UID, namespace string) (*v1.Pod, error) {
	pods, err := k.GetPods(namespace)
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

// StorageClass APIs - BEGIN

func (k *k8sOps) CreateStorageClass(sc *storage_api.StorageClass) (*storage_api.StorageClass, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.StorageV1().StorageClasses().Create(sc)
}

func (k *k8sOps) GetStorageClass(scName string) (*storage_api.StorageClass, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	sc, err := k.client.StorageV1().StorageClasses().Get(scName, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return sc, nil
}

func (k *k8sOps) DeleteStorageClass(name string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.StorageV1().StorageClasses().Delete(name, &meta_v1.DeleteOptions{})
}

func (k *k8sOps) GetStorageClassParams(sc *storage_api.StorageClass) (map[string]string, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	sc, err := k.client.StorageV1().StorageClasses().Get(sc.Name, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return sc.Parameters, nil
}

func (k *k8sOps) ValidateStorageClass(name string) (*storage_api.StorageClass, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	sc, err := k.client.StorageV1().StorageClasses().Get(name, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return sc, nil
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

func (k *k8sOps) DeletePersistentVolumeClaim(pvc *v1.PersistentVolumeClaim) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(pvc.Name, &meta_v1.DeleteOptions{})
}

func (k *k8sOps) ValidatePersistentVolumeClaim(pvc *v1.PersistentVolumeClaim) error {
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

	if _, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) GetPersistentVolumeClaim(pvcName string, namespace string) (*v1.PersistentVolumeClaim, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.client.CoreV1().PersistentVolumeClaims(namespace).
		Get(pvcName, meta_v1.GetOptions{})
}

func (k *k8sOps) GetVolumeForPersistentVolumeClaim(pvc *v1.PersistentVolumeClaim) (string, error) {
	if err := k.initK8sClient(); err != nil {
		return "", err
	}

	result, err := k.client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Get(pvc.Name, meta_v1.GetOptions{})
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
	pvcs, err := k.client.Core().PersistentVolumeClaims("").List(meta_v1.ListOptions{})
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

func (k *k8sOps) ValidateSnapshot(name string, namespace string) error {
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
				return "", true, nil
			} else if condition.Type == snap_v1.VolumeSnapshotConditionError && condition.Status == v1.ConditionTrue {
				return "", false, &ErrSnapshotFailed{
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

	if _, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second); err != nil {
		return err
	}

	return nil
}

func (k *k8sOps) GetVolumeForSnapshot(name string, namespace string) (string, error) {
	if err := k.initK8sClient(); err != nil {
		return "", err
	}

	var result snap_v1.VolumeSnapshot
	if err := k.snapClient.Get().
		Name(name).
		Resource(snap_v1.VolumeSnapshotResourcePlural).
		Namespace(namespace).
		Do().Into(&result); err != nil {
		return "", err
	}
	return result.Metadata.Name, nil
}

func (k *k8sOps) GetSnapshotStatus(name string, namespace string) (*snap_v1.VolumeSnapshotStatus, error) {
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
	return &result.Status, nil
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

// ConfigMap APIs - END

func (k *k8sOps) appsClient() v1beta2.AppsV1beta2Interface {
	return k.client.AppsV1beta2()
}

// getK8sClient instantiates a k8s client
func getK8sClient() (*kubernetes.Clientset, *rest.RESTClient, error) {
	var k8sClient *kubernetes.Clientset
	var restClient *rest.RESTClient
	var err error

	kubeconfig := os.Getenv("KUBECONFIG")
	if len(kubeconfig) > 0 {
		k8sClient, restClient, err = loadClientFromKubeconfig(kubeconfig)
	} else {
		k8sClient, restClient, err = loadClientFromServiceAccount()
	}

	if err != nil {
		return nil, nil, err
	}

	if k8sClient == nil {
		return nil, nil, ErrK8SApiAccountNotSet
	}

	return k8sClient, restClient, nil
}

// loadClientFromServiceAccount loads a k8s client from a ServiceAccount specified in the pod running px
func loadClientFromServiceAccount() (*kubernetes.Clientset, *rest.RESTClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, err
	}

	return loadClientFor(config)
}

func loadClientFromKubeconfig(kubeconfig string) (*kubernetes.Clientset, *rest.RESTClient, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, nil, err
	}

	return loadClientFor(config)
}

func loadClientFor(config *rest.Config) (*kubernetes.Clientset, *rest.RESTClient, error) {
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, err
	}

	snapClient, _, err := snap_client.NewClient(config)
	if err != nil {
		return nil, nil, err
	}

	return client, snapClient, nil
}

func roundUpSize(volumeSizeBytes int64, allocationUnitBytes int64) int64 {
	return (volumeSizeBytes + allocationUnitBytes - 1) / allocationUnitBytes
}

// getLocalIPList returns the list of local IP addresses, and optionally includes local hostname.
func getLocalIPList(includeHostname bool) ([]string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	ipList := make([]string, 0, len(ifaces))
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			logrus.WithError(err).Warnf("Error listing address for %s (cont.)", i.Name)
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			// process IP address
			if ip != nil && !ip.IsLoopback() && !ip.IsUnspecified() {
				ipList = append(ipList, ip.String())
			}
		}
	}

	if includeHostname {
		hn, err := os.Hostname()
		if err == nil && hn != "" && !strings.HasPrefix(hn, "localhost") {
			ipList = append(ipList, hn)
		}
	}

	return ipList, nil
}

// getStorageProvisionerForPVC returns storage provisioner for given PVC if it exists
func (k *k8sOps) getStorageProvisionerForPVC(pvc *v1.PersistentVolumeClaim) (string, error) {
	sc, err := k.getStorageClassForPVC(pvc)
	if err != nil {
		return "", err
	}

	return sc.Provisioner, nil
}

// isAnyVolumeUsingVolumePlugin returns true if any of the given volumes is using a storage class for the given plugin
//	In case errors are found while looking up a particular volume, the function ignores the errors as the goal is to
//	find if there is any match or not
func (k *k8sOps) isAnyVolumeUsingVolumePlugin(volumes []v1.Volume, volumeNamespace, plugin string) bool {
	for _, v := range volumes {
		if v.PersistentVolumeClaim != nil {
			pvc, err := k.GetPersistentVolumeClaim(v.PersistentVolumeClaim.ClaimName, volumeNamespace)
			if err == nil && pvc != nil {
				provisioner, err := k.getStorageProvisionerForPVC(pvc)
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

func (k *k8sOps) getStorageClassForPVC(pvc *v1.PersistentVolumeClaim) (*storage_api.StorageClass, error) {
	var scName string
	if pvc.Spec.StorageClassName != nil && len(*pvc.Spec.StorageClassName) > 0 {
		scName = *pvc.Spec.StorageClassName
	} else {
		scName = pvc.Annotations[pvcStorageClassKey]
	}

	if len(scName) == 0 {
		return nil, fmt.Errorf("PVC: %s does not have a storage class", pvc.Name)
	}

	return k.client.StorageV1().StorageClasses().Get(scName, meta_v1.GetOptions{})
}
