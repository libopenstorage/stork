package constants

const (
	// OperatorPrefix is a prefix to use for annotations added by the operator
	OperatorPrefix = "operator.libopenstorage.org"
	// LabelKeyClusterName is the name of the label key for the cluster name
	LabelKeyClusterName = OperatorPrefix + "/name"
	// LabelKeyDriverName is the name of the label key for the storage driver for the cluster
	LabelKeyDriverName = OperatorPrefix + "/driver"
	// LabelKeyStoragePod is the name of the key for the label on the pod that indicates it's on a storage node
	LabelKeyStoragePod = "storage"
	// LabelKeyKVDBPod is the name of the key for the label on the pod that indicates it's on a KVDB node
	LabelKeyKVDBPod = "kvdb"
	// LabelValueTrue is the constant for a "true" label value
	LabelValueTrue = "true"
	// AnnotationGarbageCollection is the annotation to let operator clean up external objects on uninstallation.
	AnnotationGarbageCollection = OperatorPrefix + "/garbage-collection"
	// AnnotationNodeLabels is the storage pod annotation that contains node labels
	AnnotationNodeLabels = OperatorPrefix + "/node-labels"
	// AnnotationDisableStorage annotation to disable the storage pods from running (default: false)
	AnnotationDisableStorage = OperatorPrefix + "/disable-storage"
	// AnnotationReconcileObject annotation to toggle reconciliation of operator created objects
	AnnotationReconcileObject = OperatorPrefix + "/reconcile"
	// AnnotationClusterAPIMachine is the annotation key name for the name of the
	// machine that's backing the k8s node
	AnnotationClusterAPIMachine = "cluster.k8s.io/machine"
	// AnnotationCordonedRestartDelay is the annotation key name for the duration
	// (in seconds) to wait before restarting the storage pods
	AnnotationCordonedRestartDelay = OperatorPrefix + "/cordoned-restart-delay-secs"
	// AnnotationPodSafeToEvict annotation tells cluster autoscaler whether the
	// pod is safe to be evicted when scaling down a node
	AnnotationPodSafeToEvict = "cluster-autoscaler.kubernetes.io/safe-to-evict"
	// AnnotationForceContinueUpdate annotation to force continue paused updates of storage pods (default: false)
	AnnotationForceContinueUpdate = OperatorPrefix + "/force-continue-update"
	// AnnotationCommonImageRegistries annotation contains the common image registries, separated by comma.
	// When custom image registry is provided, we will replace any image with common registry with
	// the custom registry, there is a list of hardcoded common registries, however the list
	// may not be complete, users can use this annotation to add more.
	AnnotationCommonImageRegistries = OperatorPrefix + "/common-image-registries"
)

const (
	// PrivilegedPSPName is a pod security policy used for portworx deployments which use privileged security context.
	PrivilegedPSPName = "px-privileged"
	// RestrictedPSPName is a pod security policy used by portworx deployments which require no special privileges and
	// capabilities.
	RestrictedPSPName = "px-restricted"
)

var (
	// KnownStoragePodAnnotations contains annotations that will be retained when adding custom storage pod annotations
	KnownStoragePodAnnotations = []string{
		AnnotationNodeLabels,
		AnnotationPodSafeToEvict,
	}
)
