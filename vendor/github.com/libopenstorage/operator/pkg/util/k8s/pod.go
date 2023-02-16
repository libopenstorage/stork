package k8s

import (
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulehelper "k8s.io/component-helpers/scheduling/corev1"
	affinityhelper "k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"

	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/libopenstorage/operator/pkg/constants"
)

// AddOrUpdateStoragePodTolerations adds tolerations to the given pod spec that are required for running storage pods
// as they need to tolerate built-in taints in the system
// TODO: make the storage cluster pod a critical pod to guarantee scheduling
func AddOrUpdateStoragePodTolerations(podSpec *v1.PodSpec) {
	// StorageCluster pods shouldn't be deleted by NodeController in case of node problems.
	// Add infinite toleration for taint notReady:NoExecute here to survive taint-based
	// eviction enforced by NodeController when node turns not ready.
	helper.AddOrUpdateTolerationInPodSpec(podSpec, &v1.Toleration{
		Key:      v1.TaintNodeNotReady,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoExecute,
	})

	// StorageCluster pods shouldn't be deleted by NodeController in case of node problems.
	// Add infinite toleration for taint unreachable:NoExecute here to survive taint-based
	// eviction enforced by NodeController when node turns unreachable.
	helper.AddOrUpdateTolerationInPodSpec(podSpec, &v1.Toleration{
		Key:      v1.TaintNodeUnreachable,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoExecute,
	})

	// All StorageCluster pods should tolerate MemoryPressure, DiskPressure, Unschedulable
	// and NetworkUnavailable and OutOfDisk taints.
	helper.AddOrUpdateTolerationInPodSpec(podSpec, &v1.Toleration{
		Key:      v1.TaintNodeDiskPressure,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoSchedule,
	})

	helper.AddOrUpdateTolerationInPodSpec(podSpec, &v1.Toleration{
		Key:      v1.TaintNodeMemoryPressure,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoSchedule,
	})

	helper.AddOrUpdateTolerationInPodSpec(podSpec, &v1.Toleration{
		Key:      v1.TaintNodePIDPressure,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoSchedule,
	})

	helper.AddOrUpdateTolerationInPodSpec(podSpec, &v1.Toleration{
		Key:      v1.TaintNodeUnschedulable,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoSchedule,
	})

	helper.AddOrUpdateTolerationInPodSpec(podSpec, &v1.Toleration{
		Key:      v1.TaintNodeNetworkUnavailable,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoSchedule,
	})
}

// EnvByName date interface type to sort Kubernetes environment variables by name
type EnvByName []v1.EnvVar

func (e EnvByName) Len() int      { return len(e) }
func (e EnvByName) Swap(i, j int) { e[i], e[j] = e[j], e[i] }
func (e EnvByName) Less(i, j int) bool {
	return e[i].Name < e[j].Name
}

// VolumeByName date interface type to sort Kubernetes volumes by name
type VolumeByName []v1.Volume

func (e VolumeByName) Len() int      { return len(e) }
func (e VolumeByName) Swap(i, j int) { e[i], e[j] = e[j], e[i] }
func (e VolumeByName) Less(i, j int) bool {
	return e[i].Name < e[j].Name
}

// VolumeMountByName date interface type to sort Kubernetes volume mounts by name
type VolumeMountByName []v1.VolumeMount

func (e VolumeMountByName) Len() int      { return len(e) }
func (e VolumeMountByName) Swap(i, j int) { e[i], e[j] = e[j], e[i] }
func (e VolumeMountByName) Less(i, j int) bool {
	return e[i].Name < e[j].Name
}

// newSimulationPod returns a pod template given storage cluster spec
func newSimulationPod(
	cluster *corev1.StorageCluster,
	nodeName string,
	selectorLabels map[string]string,
) (*v1.Pod, error) {
	newPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cluster.Namespace,
			Labels:    selectorLabels,
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
		},
	}

	if cluster.Spec.Placement != nil {
		if cluster.Spec.Placement.NodeAffinity != nil {
			newPod.Spec.Affinity = &v1.Affinity{
				NodeAffinity: cluster.Spec.Placement.NodeAffinity.DeepCopy(),
			}
		}
		if len(cluster.Spec.Placement.Tolerations) > 0 {
			newPod.Spec.Tolerations = make([]v1.Toleration, 0)
			for _, t := range cluster.Spec.Placement.Tolerations {
				newPod.Spec.Tolerations = append(newPod.Spec.Tolerations, *(t.DeepCopy()))
			}
		}
	}

	// Add constraints that are used for migration. We should remove this whenever we
	// remove the migration code
	addMigrationConstraints(&newPod.Spec)

	// Add default tolerations for StorageCluster pods
	AddOrUpdateStoragePodTolerations(&newPod.Spec)
	return newPod, nil
}

func addMigrationConstraints(podSpec *v1.PodSpec) {
	if podSpec.Affinity == nil {
		podSpec.Affinity = &v1.Affinity{}
	}
	if podSpec.Affinity.NodeAffinity == nil {
		podSpec.Affinity.NodeAffinity = &v1.NodeAffinity{}
	}
	if podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{}
	}
	if podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms == nil {
		podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = []v1.NodeSelectorTerm{{}}
	}

	selectorTerms := podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	for i, term := range selectorTerms {
		if term.MatchExpressions == nil {
			term.MatchExpressions = make([]v1.NodeSelectorRequirement, 0)
		}
		selectorTerms[i].MatchExpressions = append(term.MatchExpressions, v1.NodeSelectorRequirement{
			Key:      constants.LabelPortworxDaemonsetMigration,
			Operator: v1.NodeSelectorOpNotIn,
			Values: []string{
				constants.LabelValueMigrationPending,
				constants.LabelValueMigrationStarting,
			},
		})
	}
	podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = selectorTerms
}

// CheckPredicatesForStoragePod checks if a StorageCluster pod can run on a node
// Returned booleans are:
//   - shouldRun:
//     Returns true when a pod should run on the node if a storage pod is not already
//     running on that node.
//   - shouldContinueRunning:
//     Returns true when the pod should continue running on a node if a storage pod is
//     already running on that node.
func CheckPredicatesForStoragePod(
	node *v1.Node,
	cluster *corev1.StorageCluster,
	selectorLabels map[string]string,
) (bool, bool, error) {
	pod, err := newSimulationPod(cluster, node.Name, selectorLabels)
	if err != nil {
		logrus.Infof("Failed to create a pod spec for node %v: %v", node.Name, err)
		return false, false, err
	}

	taints := node.Spec.Taints
	fitsNodeName := len(pod.Spec.NodeName) == 0 || pod.Spec.NodeName == node.Name
	fitsNodeAffinity, err := affinityhelper.GetRequiredNodeAffinity(pod).Match(node)
	if err != nil {
		logrus.Warnf("Failed to match node affinity of the pod to node %s. %v", node.Name, err)
		return false, false, err
	}

	_, taintsUntolerated := schedulehelper.FindMatchingUntoleratedTaint(taints, pod.Spec.Tolerations, func(t *v1.Taint) bool {
		return t.Effect == v1.TaintEffectNoExecute || t.Effect == v1.TaintEffectNoSchedule
	})
	if !fitsNodeName || !fitsNodeAffinity {
		logrus.WithFields(logrus.Fields{
			"nodeName":         node.Name,
			"fitsNodeName":     fitsNodeName,
			"fitsNodeAffinity": fitsNodeAffinity,
		}).Debug("pod does not fit")
		return false, false, nil
	}
	if taintsUntolerated {
		// Scheduled storage pods should continue running if they tolerate NoExecute taint
		_, shouldStopRunning := schedulehelper.FindMatchingUntoleratedTaint(taints, pod.Spec.Tolerations, func(t *v1.Taint) bool {
			return t.Effect == v1.TaintEffectNoExecute
		})
		logrus.WithFields(logrus.Fields{
			"nodeName":              node.Name,
			"shouldContinueRunning": !shouldStopRunning,
		}).Debug("pod does not fit taints")
		return false, !shouldStopRunning, nil
	}
	return true, true, nil
}
