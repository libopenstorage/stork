package k8sutils

import (
	"fmt"
	"regexp"
	"time"

	"github.com/portworx/torpedo/pkg/task"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/pkg/apis/apps/v1beta1"
	ext_v1beta1 "k8s.io/client-go/pkg/apis/extensions/v1beta1"
	storage_api "k8s.io/client-go/pkg/apis/storage/v1"
	"k8s.io/client-go/rest"
)

const (
	k8sMasterLabelKey        = "node-role.kubernetes.io/master"
	k8sPVCStorageClassKey    = "volume.beta.kubernetes.io/storage-class"
	k8sLabelUpdateMaxRetries = 5
)

// GetK8sClient instantiates a k8s client
func GetK8sClient() (*kubernetes.Clientset, error) {
	k8sClient, err := loadClientFromServiceAccount()
	if err != nil {
		return nil, err
	}

	if k8sClient == nil {
		return nil, ErrK8SApiAccountNotSet
	}

	return k8sClient, nil
}

// GetNodes talks to the k8s api server and gets the nodes in the cluster
func GetNodes() (*v1.NodeList, error) {
	var err error
	client, err := GetK8sClient()
	if err != nil {
		return nil, err
	}

	nodes, err := client.CoreV1().Nodes().List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

// GetNodeByName returns the k8s node given it's name
func GetNodeByName(name string) (*v1.Node, error) {
	var err error
	client, err := GetK8sClient()
	if err != nil {
		return nil, err
	}

	node, err := client.CoreV1().Nodes().Get(name, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return node, nil
}

// IsNodeReady checks if node with given name is ready. Returns nil is ready.
func IsNodeReady(name string) error {
	node, err := GetNodeByName(name)
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
			v1.NodeConditionType(v1.NodeNetworkUnavailable),
			v1.NodeConditionType(v1.NodeInodePressure):
			if condition.Status != v1.ConditionStatus(v1.ConditionFalse) {
				return fmt.Errorf("node: %v is not ready as condition: %v (%v) is %v. Reason: %v",
					name, condition.Type, condition.Message, condition.Status, condition.Reason)
			}
		}
	}

	return nil
}

// Service APIs - BEGIN

// CreateService creates the given service
func CreateService(service *v1.Service) (*v1.Service, error) {
	client, err := GetK8sClient()
	if err != nil {
		return nil, err
	}

	ns := service.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return client.CoreV1().Services(ns).Create(service)
}

// DeleteService deletes the given service
func DeleteService(service *v1.Service) error {
	client, err := GetK8sClient()
	if err != nil {
		return err
	}

	policy := meta_v1.DeletePropagationForeground
	return client.CoreV1().Services(service.Namespace).Delete(service.Name, &meta_v1.DeleteOptions{
		PropagationPolicy: &policy,
	})
}

// GetService gets the service by the name
func GetService(svcName string, svcNS string) (*v1.Service, error) {
	client, err := GetK8sClient()
	if err != nil {
		return nil, err
	}

	if svcName == "" {
		return nil, fmt.Errorf("Cannot return service obj without service name")
	}
	svc, err := client.CoreV1().Services(svcNS).Get(svcName, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return svc, nil

}

// ValidateDeletedService validates if given service is deleted
func ValidateDeletedService(svcName string, svcNS string) error {
	client, err := GetK8sClient()
	if err != nil {
		return err
	}

	if svcName == "" {
		return fmt.Errorf("cannot validate service without service name")
	}

	_, err = client.CoreV1().Services(svcNS).Get(svcName, meta_v1.GetOptions{})
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

// CreateDeployment creates the given deployment
// TODO change references to v1** to aliases in the import above
func CreateDeployment(deployment *v1beta1.Deployment) (*v1beta1.Deployment, error) {
	client, err := GetK8sClient()
	if err != nil {
		return nil, err
	}

	ns := deployment.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return client.AppsV1beta1().Deployments(ns).Create(deployment)
}

// DeleteDeployment deletes the given deployment
func DeleteDeployment(deployment *v1beta1.Deployment) error {
	client, err := GetK8sClient()
	if err != nil {
		return err
	}

	policy := meta_v1.DeletePropagationForeground
	return client.AppsV1beta1().Deployments(deployment.Namespace).Delete(deployment.Name, &meta_v1.DeleteOptions{
		PropagationPolicy: &policy,
	})
}

// ValidateDeployment validates the given deployment if it's running and healthy
func ValidateDeployment(deployment *v1beta1.Deployment) error {
	var err error
	t := func() error {
		client, err := GetK8sClient()
		if err != nil {
			return err
		}

		// TODO: alias below apps v1beta1
		dep, err := client.AppsV1beta1().Deployments(deployment.Namespace).Get(deployment.Name, meta_v1.GetOptions{})
		if err != nil {
			return err
		}

		if *dep.Spec.Replicas != dep.Status.AvailableReplicas {
			return &ErrAppNotReady{
				ID:    dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Available replicas: %v", *dep.Spec.Replicas, dep.Status.AvailableReplicas),
			}
		}

		if *dep.Spec.Replicas != dep.Status.ReadyReplicas {
			return &ErrAppNotReady{
				ID:    dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Ready replicas: %v", *dep.Spec.Replicas, dep.Status.ReadyReplicas),
			}
		}

		pods, err := GetDeploymentPods(deployment)
		if err != nil || pods == nil {
			return &ErrAppNotReady{
				ID:    dep.Name,
				Cause: fmt.Sprintf("Failed to get pods for deployment. Err: %v", err),
			}
		}

		if len(pods) == 0 {
			return &ErrAppNotReady{
				ID:    dep.Name,
				Cause: "Application has 0 pods",
			}
		}

		for _, pod := range pods {
			if !IsPodRunning(pod) {
				return &ErrAppNotReady{
					ID:    dep.Name,
					Cause: fmt.Sprintf("pod: %v is not yet ready", pod.Name),
				}
			}
		}

		return nil
	}
	if err = task.DoRetryWithTimeout(t, 10*time.Minute, 10*time.Second); err != nil {
		return err
	}
	return err
}

// ValidateTerminatedDeployment validates if given deployment is terminated
func ValidateTerminatedDeployment(deployment *v1beta1.Deployment) error {
	var err error
	t := func() error {
		client, err := GetK8sClient()
		if err != nil {
			return err
		}

		dep, err := client.AppsV1beta1().Deployments(deployment.Namespace).Get(deployment.Name, meta_v1.GetOptions{})
		if err != nil {
			if matched, _ := regexp.MatchString(".+ not found", err.Error()); matched {
				return nil
			}
			return err
		}

		pods, err := GetDeploymentPods(deployment)
		if err != nil {
			return &ErrAppNotTerminated{
				ID:    dep.Name,
				Cause: fmt.Sprintf("Failed to get pods for deployment. Err: %v", err),
			}
		}

		if pods != nil && len(pods) > 0 {
			return &ErrAppNotTerminated{
				ID:    dep.Name,
				Cause: fmt.Sprintf("pods: %#v is still present", pods),
			}
		}

		return nil
	}

	if err = task.DoRetryWithTimeout(t, 10*time.Minute, 10*time.Second); err != nil {
		return err
	}
	return err
}

// GetDeploymentPods returns pods for the given deployment
func GetDeploymentPods(deployment *v1beta1.Deployment) ([]v1.Pod, error) {
	client, err := GetK8sClient()
	if err != nil {
		return nil, err
	}

	rSets, err := client.ReplicaSets(deployment.Namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, rSet := range rSets.Items {
		for _, owner := range rSet.OwnerReferences {
			if owner.Name == deployment.Name {
				return GetReplicaSetPods(rSet)
			}
		}
	}

	return nil, nil
}

// Deployment APIs - END

// StatefulSet APIs - BEGIN

// CreateStatefulSet creates the given statefulset
func CreateStatefulSet(statefulset *v1beta1.StatefulSet) (*v1beta1.StatefulSet, error) {
	client, err := GetK8sClient()
	if err != nil {
		return nil, err
	}

	ns := statefulset.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return client.AppsV1beta1().StatefulSets(ns).Create(statefulset)
}

// DeleteStatefulSet deletes the given statefulset
func DeleteStatefulSet(statefulset *v1beta1.StatefulSet) error {
	client, err := GetK8sClient()
	if err != nil {
		return err
	}

	policy := meta_v1.DeletePropagationForeground
	return client.AppsV1beta1().StatefulSets(statefulset.Namespace).Delete(statefulset.Name, &meta_v1.DeleteOptions{
		PropagationPolicy: &policy,
	})
}

// ValidateStatefulSet validates the given statefulset if it's running and healthy
func ValidateStatefulSet(statefulset *v1beta1.StatefulSet) error {
	var err error
	t := func() error {
		client, err := GetK8sClient()
		if err != nil {
			return err
		}
		sset, err := client.AppsV1beta1().StatefulSets(statefulset.Namespace).Get(statefulset.Name, meta_v1.GetOptions{})
		if err != nil {
			return err
		}

		if *sset.Spec.Replicas != sset.Status.Replicas { // Not sure if this is even needed but for now let's have one check before
			//readiness check
			return &ErrAppNotReady{
				ID:    sset.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Observed replicas: %v", *sset.Spec.Replicas, sset.Status.Replicas),
			}
		}

		if *sset.Spec.Replicas != sset.Status.ReadyReplicas {
			return &ErrAppNotReady{
				ID:    sset.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Ready replicas: %v", *sset.Spec.Replicas, sset.Status.ReadyReplicas),
			}
		}

		pods, err := GetStatefulSetPods(statefulset)
		if err != nil || pods == nil {
			return &ErrAppNotReady{
				ID:    sset.Name,
				Cause: fmt.Sprintf("Failed to get pods for statefulset. Err: %v", err),
			}
		}

		for _, pod := range pods {
			if !IsPodRunning(pod) {
				return &ErrAppNotReady{
					ID:    sset.Name,
					Cause: fmt.Sprintf("pod: %v is not yet ready", pod.Name),
				}
			}
		}

		return nil
	}

	if err = task.DoRetryWithTimeout(t, 10*time.Minute, 10*time.Second); err != nil {
		return err
	}
	return err
}

// GetStatefulSetPods returns pods for the given statefulset
func GetStatefulSetPods(statefulset *v1beta1.StatefulSet) ([]v1.Pod, error) {
	client, err := GetK8sClient()
	if err != nil {
		return nil, err
	}

	rSets, err := client.ReplicaSets(statefulset.Namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, rSet := range rSets.Items {
		for _, owner := range rSet.OwnerReferences {
			if owner.Name == statefulset.Name {
				return GetReplicaSetPods(rSet)
			}
		}
	}

	return nil, nil
}

// ValidateTerminatedStatefulSet validates if given deployment is terminated
func ValidateTerminatedStatefulSet(statefulset *v1beta1.StatefulSet) error {
	var err error
	t := func() error {
		client, err := GetK8sClient()
		if err != nil {
			return err
		}

		sset, err := client.AppsV1beta1().StatefulSets(statefulset.Namespace).Get(statefulset.Name, meta_v1.GetOptions{})
		if err != nil {
			if matched, _ := regexp.MatchString(".+ not found", err.Error()); matched {
				return nil
			}
			return err
		}

		pods, err := GetStatefulSetPods(statefulset)
		if err != nil {
			return &ErrAppNotTerminated{
				ID:    sset.Name,
				Cause: fmt.Sprintf("Failed to get pods for statefulset. Err: %v", err),
			}
		}

		if pods != nil && len(pods) > 0 {
			return &ErrAppNotTerminated{
				ID:    sset.Name,
				Cause: fmt.Sprintf("pods: %#v is still present", pods),
			}
		}

		return nil
	}

	if err = task.DoRetryWithTimeout(t, 10*time.Minute, 10*time.Second); err != nil {
		return err
	}
	return err
}

// StatefulSet APIs - END

// DeletePods deletes the given pods
func DeletePods(pods []v1.Pod) error {
	client, err := GetK8sClient()
	if err != nil {
		return err
	}

	var gracePeriod int64
	gracePeriod = 0

	for _, pod := range pods {
		if err = client.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &meta_v1.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
		}); err != nil {
			return err
		}
	}

	return nil
}

// GetReplicaSetPods returns pods for the given replica set
func GetReplicaSetPods(rSet ext_v1beta1.ReplicaSet) ([]v1.Pod, error) {
	client, err := GetK8sClient()
	if err != nil {
		return nil, err
	}

	pods, err := client.Pods(rSet.Namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var result []v1.Pod
	for _, pod := range pods.Items {
		for _, owner := range pod.OwnerReferences {
			if owner.Name == rSet.Name {
				result = append(result, pod)
			}
		}
	}

	return result, nil
}

// StorageClass APIs - BEGIN

// CreateStorageClass creates the given storage class
func CreateStorageClass(sc *storage_api.StorageClass) (*storage_api.StorageClass, error) {
	client, err := GetK8sClient()
	if err != nil {
		return nil, err
	}

	return client.StorageV1().StorageClasses().Create(sc)
}

// DeleteStorageClass deletes the given storage class
func DeleteStorageClass(sc *storage_api.StorageClass) error {
	client, err := GetK8sClient()
	if err != nil {
		return err
	}

	return client.StorageV1beta1().StorageClasses().Delete(sc.Name, &meta_v1.DeleteOptions{})
}

// ValidateStorageClass validates the given storage class
func ValidateStorageClass(sc *storage_api.StorageClass) error {
	client, err := GetK8sClient()
	if err != nil {
		return err
	}

	_, err = client.StorageV1beta1().StorageClasses().Get(sc.Name, meta_v1.GetOptions{})
	if err != nil {
		return err
	}

	return nil
}

// StorageClass APIs - END

// PVC APIs - BEGIN

// CreatePersistentVolumeClaim creates the given persistent volume claim
func CreatePersistentVolumeClaim(pvc *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error) {
	client, err := GetK8sClient()
	if err != nil {
		return nil, err
	}

	ns := pvc.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return client.PersistentVolumeClaims(ns).Create(pvc)
}

// DeletePersistentVolumeClaim deletes the given persistent volume claim
func DeletePersistentVolumeClaim(pvc *v1.PersistentVolumeClaim) error {
	client, err := GetK8sClient()
	if err != nil {
		return err
	}

	return client.PersistentVolumeClaims(pvc.Namespace).Delete(pvc.Name, &meta_v1.DeleteOptions{})
}

// ValidatePersistentVolumeClaim validates the given pvc
func ValidatePersistentVolumeClaim(pvc *v1.PersistentVolumeClaim) error {
	var err error
	t := func() error {
		client, err := GetK8sClient()
		if err != nil {
			return err
		}

		result, err := client.PersistentVolumeClaims(pvc.Namespace).Get(pvc.Name, meta_v1.GetOptions{})
		if err != nil {
			return err
		}

		if result.Status.Phase == v1.ClaimBound {
			return nil
		}

		return &ErrPVCNotReady{
			ID:    result.Name,
			Cause: fmt.Sprintf("PVC expected status: %v PVC actual status: %v", v1.ClaimBound, result.Status.Phase),
		}
	}

	if err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second); err != nil {
		return err
	}
	return err
}

// GetVolumeForPersistentVolumeClaim returns the back volume for the given PVC
func GetVolumeForPersistentVolumeClaim(pvc *v1.PersistentVolumeClaim) (string, error) {
	client, err := GetK8sClient()
	if err != nil {
		return "", err
	}

	result, err := client.PersistentVolumeClaims(pvc.Namespace).Get(pvc.Name, meta_v1.GetOptions{})
	if err != nil {
		return "", err
	}

	return result.Spec.VolumeName, nil
}

// GetPersistentVolumeClaimParams fetches custom parameters for the given PVC
func GetPersistentVolumeClaimParams(pvc *v1.PersistentVolumeClaim) (map[string]string, error) {
	client, err := GetK8sClient()
	if err != nil {
		return nil, err
	}

	params := make(map[string]string)

	result, err := client.PersistentVolumeClaims(pvc.Namespace).Get(pvc.Name, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}

	capacity, ok := result.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	if !ok {
		return nil, fmt.Errorf("failed to get storage resource for pvc: %v", result.Name)
	}

	requestGB := int(roundUpSize(capacity.Value(), 1024*1024*1024))
	requestSizeInBytes := uint64(requestGB * 1024 * 1024 * 1024)
	params["size"] = fmt.Sprintf("%d", requestSizeInBytes)

	scName, ok := result.Annotations[k8sPVCStorageClassKey]
	if !ok {
		return nil, fmt.Errorf("failed to get storage class for pvc: %v", result.Name)
	}

	sc, err := client.StorageV1beta1().StorageClasses().Get(scName, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}

	for key, value := range sc.Parameters {
		params[key] = value
	}

	return params, nil
}

// PVCs APIs - END

// IsNodeMaster returns true if given node is a kubernetes master node
func IsNodeMaster(node v1.Node) bool {
	_, ok := node.Labels[k8sMasterLabelKey]
	return ok
}

// AddLabelOnNode adds a label key=value on the given node
func AddLabelOnNode(name, key, value string) error {
	var err error
	client, err := GetK8sClient()
	if err != nil {
		return err
	}

	retryCnt := 0
	for retryCnt < k8sLabelUpdateMaxRetries {
		retryCnt++

		node, err := client.CoreV1().Nodes().Get(name, meta_v1.GetOptions{})
		if err != nil {
			return err
		}

		if val, present := node.Labels[key]; present && val == value {
			return nil
		}

		node.Labels[key] = value
		if _, err = client.CoreV1().Nodes().Update(node); err == nil {
			return nil
		}
	}

	return err
}

// RemoveLabelOnNode removes the label with key on given node
func RemoveLabelOnNode(name, key string) error {
	var err error
	client, err := GetK8sClient()
	if err != nil {
		return err
	}

	retryCnt := 0
	for retryCnt < k8sLabelUpdateMaxRetries {
		retryCnt++

		node, err := client.CoreV1().Nodes().Get(name, meta_v1.GetOptions{})
		if err != nil {
			return err
		}

		if _, present := node.Labels[key]; present {
			delete(node.Labels, key)
			if _, err = client.CoreV1().Nodes().Update(node); err == nil {
				return nil
			}
		}
	}

	return err
}

// loadClientFromServiceAccount loads a k8s client from a ServiceAccount specified in the pod running px
func loadClientFromServiceAccount() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return k8sClient, nil
}

func roundUpSize(volumeSizeBytes int64, allocationUnitBytes int64) int64 {
	return (volumeSizeBytes + allocationUnitBytes - 1) / allocationUnitBytes
}

// IsPodRunning checks if all containers in a pod are in running state
func IsPodRunning(pod v1.Pod) bool {
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
