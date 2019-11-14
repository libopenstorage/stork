package v1alpha1

type (
	// PolicyConditionName is the type for policy condition names
	PolicyConditionName string
)

const (
	openstorageDomain = "openstorage.io"
	// PolicyObjectPrefix is the key for any openstorage object used for policies
	PolicyObjectPrefix = openstorageDomain + ".object"
	// PolicyConditionPrefix is the key for any openstorage condition used for policies
	PolicyConditionPrefix = openstorageDomain + ".condition"
	// PolicyActionPrefix is the key for any openstorage action used for policies
	PolicyActionPrefix = openstorageDomain + ".action"

	// PolicyObjectTypeVolume is the key for volume objects
	PolicyObjectTypeVolume = PolicyObjectPrefix + ".volume"
	// PolicyObjectTypeStoragePool is the key for storagepool objects
	PolicyObjectTypeStoragePool = PolicyObjectPrefix + ".storagepool"
	// PolicyObjectTypeNode  is the key for node objects
	PolicyObjectTypeNode = PolicyObjectPrefix + ".node"
	// PolicyObjectTypeDisk is the key for disk objects
	PolicyObjectTypeDisk = PolicyObjectPrefix + ".disk"

	// PolicyActionVolume is the key for volume actions for policies
	PolicyActionVolume = PolicyActionPrefix + ".volume"
	// PolicyActionStoragePool is the key for storagepool actions for policies
	PolicyActionStoragePool = PolicyActionPrefix + ".storagepool"
	// PolicyActionNode is the key for node actions for policies
	PolicyActionNode = PolicyActionPrefix + ".node"
	// PolicyActionDisk is the key for disk actions for policies
	PolicyActionDisk = PolicyActionPrefix + ".disk"
)

const (
	latencyMS       = "/latency_ms"
	actionRebalance = "/rebalance"
)

// EnforcementType Defines the types of enforcement on the given policy
type EnforcementType string

const (
	// EnforcementRequired specifies that the policy is required and must be strictly enforced
	EnforcementRequired EnforcementType = "required"
	// EnforcementPreferred specifies that the policy is preferred and can be best effort
	EnforcementPreferred EnforcementType = "preferred"
)

const (
	/***** Volume conditions *****/

	// PolicyConditionVolumeLatencyMS is the latency (reads + writes) for a volume in milliseconds
	PolicyConditionVolumeLatencyMS PolicyConditionName = PolicyObjectTypeVolume + latencyMS

	/***** Storage pool conditions *****/

	// PolicyConditionStoragePoolLatencyMS is the latency (reads + writes) for a storage pool in milliseconds
	PolicyConditionStoragePoolLatencyMS PolicyConditionName = PolicyObjectTypeStoragePool + latencyMS

	/***** Disk conditions *****/

	// PolicyConditionDiskLatencyMS is the latency (reads + writes) for a disk in milliseconds
	PolicyConditionDiskLatencyMS PolicyConditionName = PolicyObjectTypeDisk + latencyMS
)

const (
	/***** Volume actions *****/

	// PolicyActionVolumeResize is an action to resize volumes
	PolicyActionVolumeResize = "resize"

	/***** Node actions *****/

	// PolicyActionNodeRebalance is an action to rebalance a node
	PolicyActionNodeRebalance = PolicyActionNode + actionRebalance
)
