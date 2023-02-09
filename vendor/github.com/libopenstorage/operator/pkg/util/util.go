package util

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-version"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/libopenstorage/operator/pkg/constants"
)

// Reasons for controller events
const (
	// FailedPlacementReason is added to an event when operator can't schedule a Pod to a specified node.
	FailedPlacementReason = "FailedPlacement"
	// FailedStoragePodReason is added to an event when the status of a Pod of a cluster is 'Failed'.
	FailedStoragePodReason = "FailedStoragePod"
	// FailedSyncReason is added to an event when the status of the cluster could not be synced.
	FailedSyncReason = "FailedSync"
	// FailedValidationReason is added to an event when operator validations fail.
	FailedValidationReason = "FailedValidation"
	// FailedComponentReason is added to an event when setting up or removing a component fails.
	FailedComponentReason = "FailedComponent"
	// UpdatePausedReason is added to an event when operator pauses update of the storage cluster.
	UpdatePausedReason = "UpdatePaused"
	// ClusterOnlineReason is added to an event when a cluster comes online
	ClusterOnlineReason = "ClusterOnline"
	// MigrationPendingReason is added to an event when the migration is in pending state.
	MigrationPendingReason = "MigrationPending"
	// MigrationCompletedReason is added to an event when the migration is completed.
	MigrationCompletedReason = "MigrationCompleted"
	// MigrationFailedReason is added to an event when the migration fails.
	MigrationFailedReason = "MigrationFailed"
	// UnevenStorageNodesReason is added to an event when there are uneven number of storage nodes are labelled across zones
	UnevenStorageNodesReason = "UnevenStorageNodes"
	// AllStoragelessNodesReason is added to an event when all the nodes in the cluster is  labelled as storageless
	AllStoragelessNodesReason = "AllStoragelessNodes"

	// MigrationDryRunCompletedReason is added to an event when dry run is completed
	MigrationDryRunCompletedReason = "MigrationDryRunCompleted"
	// MigrationDryRunFailedReason is added to an event when dry run fails.
	MigrationDryRunFailedReason = "MigrationDryRunFailed"

	// DefaultImageRegistry is the default registry when no registry is provided
	DefaultImageRegistry = "docker.io"

	// StorkSchedulerName is the default scheduler for px-csi-ext pods
	StorkSchedulerName = "stork"

	// NodeTypeKey is the key of the label used to set node as storage or storageless
	NodeTypeKey = "portworx.io/node-type"
	// StorageNodeValue is the value for storage node
	StorageNodeValue = "storage"
	// StoragelessNodeValue is the value for storage node
	StoragelessNodeValue = "storageless"
	// StoragePartitioningEnvKey is the storage spec environment variable used to set storage/storageless node type
	StoragePartitioningEnvKey = "ENABLE_ASG_STORAGE_PARTITIONING"
)

var (
	// commonDockerRegistries is a map of commonly used Docker registries
	commonDockerRegistries = map[string]bool{
		"docker.io":                   true,
		"quay.io":                     true,
		"index.docker.io":             true,
		"registry-1.docker.io":        true,
		"registry.connect.redhat.com": true,
		"registry.k8s.io":             true,
	}

	// podTopologySpreadConstraintKeys is a list of topology keys considered for pod spread constraints
	podTopologySpreadConstraintKeys = []string{
		"topology.kubernetes.io/region",
		"topology.kubernetes.io/zone",
	}
)

// AddDefaultRegistryToImage adds default registry to image.
func AddDefaultRegistryToImage(image string) string {
	if image == "" {
		return ""
	}

	for k := range commonDockerRegistries {
		if strings.HasPrefix(image, k) || strings.HasPrefix(image, "gcr.io") || strings.HasPrefix(image, "k8s.gcr.io") {
			return image
		}
	}

	return DefaultImageRegistry + "/" + image
}

func getMergedCommonRegistries(cluster *corev1.StorageCluster) map[string]bool {
	val, ok := cluster.Annotations[constants.AnnotationCommonImageRegistries]

	if !ok {
		return commonDockerRegistries
	}

	mergedCommonRegistries := make(map[string]bool)

	for _, v := range strings.Split(strings.TrimSpace(val), ",") {
		mergedCommonRegistries[v] = true
	}

	for k, v := range commonDockerRegistries {
		mergedCommonRegistries[k] = v
	}

	return mergedCommonRegistries
}

// GetImageURN returns the complete image name based on the registry and repo
func GetImageURN(cluster *corev1.StorageCluster, image string) string {
	if image == "" {
		return ""
	}

	registryAndRepo := cluster.Spec.CustomImageRegistry
	mergedCommonRegistries := getMergedCommonRegistries(cluster)
	preserveFullCustomImageRegistry := cluster.Spec.PreserveFullCustomImageRegistry

	omitRepo := false
	if strings.HasSuffix(registryAndRepo, "//") {
		omitRepo = true
	}

	registryAndRepo = strings.TrimRight(registryAndRepo, "/")
	if registryAndRepo == "" {
		// no registry/repository specifed, return image
		return AddDefaultRegistryToImage(image)
	}

	imgParts := strings.Split(image, "/")
	if len(imgParts) > 1 {
		// advance imgParts to swallow the common registry
		if _, present := mergedCommonRegistries[imgParts[0]]; present {
			imgParts = imgParts[1:]
		}
	}

	if !preserveFullCustomImageRegistry {
		// if we have '/' in the registryAndRepo, return <registry/repository/><only-image>
		// else (registry only) -- return <registry/><image-with-repository>
		if strings.Contains(registryAndRepo, "/") || omitRepo {
			// advance to the last element, skipping image's repository
			imgParts = imgParts[len(imgParts)-1:]
		}
	}

	return registryAndRepo + "/" + path.Join(imgParts...)
}

// GetImageMajorVersion returns the major version for a given image.
// This allows you to make decisions based on the major version.
func GetImageMajorVersion(image string) int {
	if !strings.Contains(image, ":") {
		return -1
	}

	parts := strings.Split(image, ":")
	tag := parts[len(parts)-1]
	if tag == "" {
		return -1
	}

	ver, err := version.NewVersion(tag)
	if err != nil {
		return -1
	}

	return ver.Segments()[0]
}

// HasResourcesChanged compares two resources, reflect.DeepEqual does not work due to the format
// may change, for example CPU 0.1 and 100m should be the same.
func HasResourcesChanged(r1, r2 v1.ResourceRequirements) bool {
	return HasResourceListChanged(r1.Requests, r2.Requests) || HasResourceListChanged(r1.Limits, r2.Limits)
}

// HasResourceListChanged compares two resource lists.
func HasResourceListChanged(l1, l2 v1.ResourceList) bool {
	if len(l1) != len(l2) {
		return true
	}

	for k, v := range l1 {
		if v2, ok := l2[k]; !ok || v2.Cmp(v) != 0 {
			return true
		}
	}

	return false
}

// HasPullSecretChanged checks if the imagePullSecret in the cluster is the only one
// in the given list of pull secrets
func HasPullSecretChanged(
	cluster *corev1.StorageCluster,
	existingPullSecrets []v1.LocalObjectReference,
) bool {
	return len(existingPullSecrets) > 1 ||
		(len(existingPullSecrets) == 1 &&
			cluster.Spec.ImagePullSecret != nil && existingPullSecrets[0].Name != *cluster.Spec.ImagePullSecret) ||
		(len(existingPullSecrets) == 0 &&
			cluster.Spec.ImagePullSecret != nil && *cluster.Spec.ImagePullSecret != "")
}

// HaveTolerationsChanged checks if the tolerations in the cluster are same as the
// given list of tolerations
func HaveTolerationsChanged(
	cluster *corev1.StorageCluster,
	existingTolerations []v1.Toleration,
) bool {
	if cluster.Spec.Placement == nil {
		return len(existingTolerations) != 0
	}
	return !reflect.DeepEqual(cluster.Spec.Placement.Tolerations, existingTolerations)
}

// DeepEqualObject compare two objects
func DeepEqualObject(obj1, obj2 interface{}) error {
	if !reflect.DeepEqual(obj1, obj2) {
		return fmt.Errorf("two objects are different, first object %+v, second object %+v", obj1, obj2)
	}
	return nil
}

// DeepEqualObjects compares two arrays of objects
func DeepEqualObjects(
	objs1, objs2 []interface{},
	funcGetKey func(obj interface{}) string,
	funcDeepEqualObject func(obj1, obj2 interface{}) error) error {

	map1 := make(map[string]interface{})
	map2 := make(map[string]interface{})
	for _, obj := range objs1 {
		map1[funcGetKey(obj)] = obj
	}
	for _, obj := range objs2 {
		map2[funcGetKey(obj)] = obj
	}

	var msg string
	for k, v := range map1 {
		v2, ok := map2[k]

		if !ok {
			msg += fmt.Sprintf("object \"%s\" exists in first array but does not exist in second array.\n", k)
		} else if err := funcDeepEqualObject(v, v2); err != nil {
			msg += err.Error()
			msg += "\n"
		}
	}

	for k := range map2 {
		if _, ok := map1[k]; !ok {
			msg += fmt.Sprintf("object \"%s\" exists in second array but does not exist in first array.\n", k)
		}
	}

	if msg != "" {
		return fmt.Errorf(msg)
	}
	return nil
}

// DeepEqualPodTemplate compares if two pod template specs are same.
func DeepEqualPodTemplate(t1, t2 *v1.PodTemplateSpec) (bool, error) {
	// DeepDerivative will return true if first argument is nil, hence check the length of volumes.
	// The reason we don't2 use deepEqual for volumes is k8s API server may add defaultMode to it.
	if !equality.Semantic.DeepDerivative(t1.Spec.Containers, t2.Spec.Containers) {
		return false, fmt.Errorf("containers not equal, first: %+v, second: %+v", t1.Spec.Containers, t2.Spec.Containers)
	}

	if !(len(t1.Spec.Volumes) == len(t2.Spec.Volumes) &&
		equality.Semantic.DeepDerivative(t1.Spec.Volumes, t2.Spec.Volumes)) {
		return false, fmt.Errorf("volumes not equal, first: %+v, second: %+v", t1.Spec.Volumes, t2.Spec.Volumes)
	}

	if !equality.Semantic.DeepEqual(t1.Spec.ImagePullSecrets, t2.Spec.ImagePullSecrets) {
		return false, fmt.Errorf("image pull secrets not equal, first: %+v, second: %+v", t1.Spec.ImagePullSecrets, t2.Spec.ImagePullSecrets)
	}

	if !equality.Semantic.DeepEqual(t1.Spec.Affinity, t2.Spec.Affinity) {
		return false, fmt.Errorf("affinity not equal, first: %+v, second: %+v", t1.Spec.Affinity, t2.Spec.Affinity)
	}

	if !equality.Semantic.DeepEqual(t1.Spec.Tolerations, t2.Spec.Tolerations) {
		return false, fmt.Errorf("tolerations not equal, first: %+v, second: %+v", t1.Spec.Tolerations, t2.Spec.Tolerations)
	}

	if !equality.Semantic.DeepEqual(t1.Spec.ServiceAccountName, t2.Spec.ServiceAccountName) {
		return false, fmt.Errorf("service account name not equal, first: %s, second: %s", t1.Spec.ServiceAccountName, t2.Spec.ServiceAccountName)
	}

	return true, nil
}

// HasNodeAffinityChanged checks if the nodeAffinity in the cluster is same as the
// node affinity in the given affinity
func HasNodeAffinityChanged(
	cluster *corev1.StorageCluster,
	existingAffinity *v1.Affinity,
) bool {
	if cluster.Spec.Placement == nil {
		return existingAffinity != nil && existingAffinity.NodeAffinity != nil
	} else if existingAffinity == nil {
		return cluster.Spec.Placement.NodeAffinity != nil
	}
	return !reflect.DeepEqual(cluster.Spec.Placement.NodeAffinity, existingAffinity.NodeAffinity)
}

// HasSchedulerStateChanged checks if the stork has been enabled/disabled in the StorageCluster
func HasSchedulerStateChanged(
	cluster *corev1.StorageCluster,
	previousSchedulerName string,
) bool {
	if cluster.Spec.Stork == nil {
		return v1.DefaultSchedulerName != previousSchedulerName
	}
	currentSchedulerName := v1.DefaultSchedulerName
	if cluster.Spec.Stork.Enabled {
		currentSchedulerName = StorkSchedulerName
	}
	return currentSchedulerName != previousSchedulerName
}

// ExtractVolumesAndMounts returns a list of Kubernetes volumes and volume mounts from the
// given StorageCluster volume specs
func ExtractVolumesAndMounts(volumeSpecs []corev1.VolumeSpec) ([]v1.Volume, []v1.VolumeMount) {
	volumes := make([]v1.Volume, 0)
	volumeMounts := make([]v1.VolumeMount, 0)

	// Set volume defaults. Makes it easier to compare with
	// actual deployment volumes to see if they have changed.
	for i := range volumeSpecs {
		if volumeSpecs[i].ConfigMap != nil {
			defaultMode := v1.ConfigMapVolumeSourceDefaultMode
			volumeSpecs[i].ConfigMap.DefaultMode = &defaultMode
		} else if volumeSpecs[i].Secret != nil {
			defaultMode := v1.SecretVolumeSourceDefaultMode
			volumeSpecs[i].Secret.DefaultMode = &defaultMode
		} else if volumeSpecs[i].Projected != nil {
			defaultMode := v1.ProjectedVolumeSourceDefaultMode
			volumeSpecs[i].Projected.DefaultMode = &defaultMode
		} else if volumeSpecs[i].HostPath != nil {
			hostPathType := v1.HostPathUnset
			volumeSpecs[i].HostPath.Type = &hostPathType
		}
	}

	for _, volumeSpec := range volumeSpecs {
		volumes = append(volumes, v1.Volume{
			Name:         volumeSpec.Name,
			VolumeSource: volumeSpec.VolumeSource,
		})
		volumeMounts = append(volumeMounts, v1.VolumeMount{
			Name:             volumeSpec.Name,
			MountPath:        volumeSpec.MountPath,
			MountPropagation: volumeSpec.MountPropagation,
			ReadOnly:         volumeSpec.ReadOnly,
		})
	}

	return volumes, volumeMounts
}

// IsPartialSecretRef is a helper method that checks if a SecretRef is partially specified (i.e. only one of the needed cert name and key specified)
func IsPartialSecretRef(sref *corev1.SecretRef) bool {
	if sref == nil {
		return false
	}
	x := len(sref.SecretName) > 0
	y := len(sref.SecretKey) > 0
	// X xor Y -> (X || Y) && !(X && Y)
	return (x || y) && !(x && y)
}

// GetCustomAnnotations returns custom annotations for different StorageCluster components from spec
func GetCustomAnnotations(
	cluster *corev1.StorageCluster,
	k8sObjKind string,
	componentName string,
) map[string]string {
	if cluster.Spec.Metadata == nil || cluster.Spec.Metadata.Annotations == nil {
		return nil
	}
	// Use kind/component to locate the custom annotation, e.g. deployment/stork
	key := fmt.Sprintf("%s/%s", k8sObjKind, componentName)
	if annotations, ok := cluster.Spec.Metadata.Annotations[key]; ok && len(annotations) != 0 {
		return annotations
	}
	return nil
}

// GetCustomLabels returns custom labels for different StorageCluster components from spec
func GetCustomLabels(
	cluster *corev1.StorageCluster,
	k8sObjKind string,
	componentName string,
) map[string]string {
	if cluster.Spec.Metadata == nil || cluster.Spec.Metadata.Labels == nil {
		return nil
	}
	// Use kind/component to locate the custom labels, e.g. service/portworx-api
	key := fmt.Sprintf("%s/%s", k8sObjKind, componentName)
	if labels, ok := cluster.Spec.Metadata.Labels[key]; ok && len(labels) != 0 {
		return labels
	}
	return nil
}

// ComponentsPausedForMigration returns true if the daemonset migration is going on and
// the components are waiting for storage pods to migrate first
func ComponentsPausedForMigration(cluster *corev1.StorageCluster) bool {
	_, migrating := cluster.Annotations[constants.AnnotationMigrationApproved]
	componentsPaused, err := strconv.ParseBool(cluster.Annotations[constants.AnnotationPauseComponentMigration])
	return migrating && err == nil && componentsPaused
}

// HaveTopologySpreadConstraintsChanged checks if the deployment has pod topology spread constraints changed
func HaveTopologySpreadConstraintsChanged(
	updatedTopologySpreadConstraints []v1.TopologySpreadConstraint,
	existingTopologySpreadConstraints []v1.TopologySpreadConstraint,
) bool {
	return !reflect.DeepEqual(updatedTopologySpreadConstraints, existingTopologySpreadConstraints)
}

// GetTopologySpreadConstraints returns pod topology spread constraints spec
func GetTopologySpreadConstraints(
	k8sClient client.Client,
	labels map[string]string,
) ([]v1.TopologySpreadConstraint, error) {
	nodeList := &v1.NodeList{}
	err := k8sClient.List(context.TODO(), nodeList)
	if err != nil {
		return nil, err
	}
	return GetTopologySpreadConstraintsFromNodes(nodeList, labels)
}

// GetTopologySpreadConstraintsFromNodes returns pod topology spread constraints spec
func GetTopologySpreadConstraintsFromNodes(
	nodeList *v1.NodeList,
	labels map[string]string,
) ([]v1.TopologySpreadConstraint, error) {
	topologyKeySet := make(map[string]bool)
	for _, key := range podTopologySpreadConstraintKeys {
		for _, node := range nodeList.Items {
			if _, ok := node.Labels[key]; ok {
				topologyKeySet[key] = true
			}
		}
	}
	var keys []string
	for k := range topologyKeySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// Construct topology spread constraints
	var constraints []v1.TopologySpreadConstraint
	for _, key := range keys {
		constraints = append(constraints, v1.TopologySpreadConstraint{
			MaxSkew:           1,
			TopologyKey:       key,
			WhenUnsatisfiable: v1.ScheduleAnyway,
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
		})
	}
	return constraints, nil
}

// UpdateStorageClusterCondition update condition based on source and type
func UpdateStorageClusterCondition(
	cluster *corev1.StorageCluster,
	toUpdate *corev1.ClusterCondition,
) {
	if toUpdate == nil || toUpdate.Source == "" || toUpdate.Type == "" {
		logrus.Warn("empty or invalid storage cluster condition provided")
		return
	}

	// Search for condition by source and type
	foundIndex := -1
	for i, condition := range cluster.Status.Conditions {
		if toUpdate.Source == condition.Source && toUpdate.Type == condition.Type {
			foundIndex = i
			// No update needed but populate the last transition time
			if toUpdate.Status == condition.Status &&
				toUpdate.Message == condition.Message {
				toUpdate.LastTransitionTime = condition.LastTransitionTime
				return
			}
			break
		}
	}

	// Create a new condition or overwrite existing one
	var sortedConditions []corev1.ClusterCondition
	if foundIndex == -1 {
		sortedConditions = make([]corev1.ClusterCondition, len(cluster.Status.Conditions)+1)
	} else {
		sortedConditions = make([]corev1.ClusterCondition, len(cluster.Status.Conditions))
	}

	if toUpdate.LastTransitionTime.Equal(&metav1.Time{}) {
		// Set timestamp if not provided, truncate to second
		toUpdate.LastTransitionTime = metav1.NewTime(time.Now().Truncate(time.Second))
	}
	sortedConditions[0] = *toUpdate.DeepCopy()

	for index, indexSorted := 0, 1; index < len(cluster.Status.Conditions) && indexSorted < len(sortedConditions); index++ {
		if index == foundIndex {
			continue
		}
		sortedConditions[indexSorted] = cluster.Status.Conditions[index]
		indexSorted++
	}
	cluster.Status.Conditions = sortedConditions
}

// GetStorageClusterCondition returns the condition based on source and type
func GetStorageClusterCondition(
	cluster *corev1.StorageCluster,
	conditionSource string,
	conditionType corev1.ClusterConditionType,
) *corev1.ClusterCondition {
	for _, condition := range cluster.Status.Conditions {
		if condition.Source == conditionSource && condition.Type == conditionType {
			return condition.DeepCopy()
		}
	}
	return nil
}
