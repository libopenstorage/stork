package kubevirtdynamic

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	vmiResource = schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachineinstances"}
)

type ownerRef struct {
	apiVersion string
	kind       string
	name       string
	uid        string
}

// VMIPhaseTransition shows when VMI transitioned into a certain phase
type VMIPhaseTransition struct {
	// Phase of the VMI
	Phase string
	// TransitionTime indicates when the VMI transitioned into this phase
	TransitionTime time.Time
}

// VirtualMachineInstance represents an instance of KubeVirt VirtualMachine
type VirtualMachineInstance struct {
	// Name if the VMI
	Name string
	// Namespace of the VMI
	NameSpace string
	// UID from the VMI metadata
	UID string
	// RootDisk is the name of the volume that is used as a root disk in the VMI.
	RootDisk string
	// RootDiskPVC is the name of the PVC corresponding to the root disk.
	RootDiskPVC string
	// LiveMigratable indicates if VMI can be live migrated.
	LiveMigratable bool
	// Ready indicates if VMI is ready.
	Ready bool
	// Paused indicates if VMI is paused.
	Paused bool
	// NodeName where VMI is currently running
	NodeName string
	// Phase VMI is in e.g. Running
	Phase string
	// PhaseTransitions has list of phase transitions
	PhaseTransitions []*VMIPhaseTransition
	// OwnerVMUID is the UID of the VirtualMachine object that owns this VMI
	OwnerVMUID string
}

// VirtualMachineInstanceOps is an interface to manage VirtualMachineInstance objects
type VirtualMachineInstanceOps interface {
	// GetVirtualMachineInstance retrieves some info about the specified VMI
	GetVirtualMachineInstance(ctx context.Context, namespace, name string) (*VirtualMachineInstance, error)
}

// GetVirtualMachineInstance returns the VirtualMachineInstance
func (c *Client) GetVirtualMachineInstance(
	ctx context.Context, namespace, name string,
) (*VirtualMachineInstance, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	vmiRaw, err := c.client.Resource(vmiResource).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	// Find name of the root disk (one with bootOrder=1) in VMI. Sample yaml:
	//spec:
	//  domain:
	//    devices:
	//      disks:
	//      - bootOrder: 1
	//        disk:
	//          bus: virtio
	//        name: rootdisk
	//      - bootOrder: 2
	//        disk:
	//          bus: virtio
	//        name: cloudinitdisk
	//      - disk:
	//          bus: virtio
	//        name: disk-efficient-seahorse
	//
	disks, found, err := unstructured.NestedSlice(vmiRaw.Object, "spec", "domain", "devices", "disks")
	if err != nil || !found || len(disks) == 0 {
		return nil, fmt.Errorf("failed to find vmi disks: %w", err)
	}
	rootDiskName := ""
	bootDisk, err := c.unstructuredFindKeyValInt64(disks, "bootOrder", 1)
	if err != nil {
		return nil, fmt.Errorf("failed to find boot disk in vmi: %w", err)
	}
	if bootDisk == nil {
		// No "bootOrder" present in the VM spec. Assume that the first disk is the boot disk.
		var ok bool
		rawMap := disks[0]
		bootDisk, ok = rawMap.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("wrong type of element in slice: expected map[string]interface{}, actual %T", rawMap)
		}
	}
	rootDiskName, found, err = c.unstructuredGetValString(bootDisk, "name")
	if err != nil || !found || rootDiskName == "" {
		return nil, fmt.Errorf("failed to find rootDisk name: %w", err)
	}
	// Find name of the PVC in VMI. Sample yaml when dataVolume was used by the VMI:
	// NOTE: the dataVolume may have been garbage collected after pvc was ready.
	//  spec:
	//    volumes:
	//    - dataVolume:
	//        name: fedora-communist-toucan
	//      name: rootdisk
	//
	volumes, found, err := unstructured.NestedSlice(vmiRaw.Object, "spec", "volumes")
	if err != nil || !found {
		return nil, fmt.Errorf("failed to find vmi volumes: %w", err)
	}
	pvcName := ""
	rootVolume, err := c.unstructuredFindKeyValString(volumes, "name", rootDiskName)
	if err != nil || rootVolume == nil {
		return nil, fmt.Errorf("failed to find root volume %q in the vmi: %w", rootDiskName, err)
	}

	// Check if this is a dataVolume or a pvc
	if dataVolumeRaw, ok := rootVolume["dataVolume"]; ok {
		dataVolume, ok := dataVolumeRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf(
				"wrong type for vmi dataVolume: expected map[string]interface{}, actual %T", dataVolumeRaw)
		}
		name, found, err := c.unstructuredGetValString(dataVolume, "name")
		if err != nil || !found {
			return nil, fmt.Errorf("failed to get name of the rootdisk data volume: %w", err)
		}
		// pvc name is always same as the data volume name
		pvcName = name
	} else if pvcRaw, ok := rootVolume["persistentVolumeClaim"]; ok {
		pvc, ok := pvcRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("wrong type for vmi pvc: expected map[string]interface{}, actual %T", pvcRaw)
		}
		name, found, err := c.unstructuredGetValString(pvc, "claimName")
		if err != nil || !found {
			return nil, fmt.Errorf("failed to get name of the rootdisk pvc: %w", err)
		}
		pvcName = name
	} else {
		return nil, fmt.Errorf("root volume is neither a dataVolume nor a pvc")
	}
	if pvcName == "" {
		return nil, fmt.Errorf("empty pvc name for the root disk")
	}

	// check if the VMI is live migratable and ready
	// Sample yaml:
	//  status:
	//    conditions:
	//    - lastProbeTime: null
	//      lastTransitionTime: null
	//      status: "True"
	//      type: LiveMigratable
	//    - lastProbeTime: null
	//      lastTransitionTime: "2024-05-05T14:15:51Z"
	//      status: "True"
	//      type: Ready

	//
	liveMigratable := false
	ready := false
	paused := false
	conditions, found, err := unstructured.NestedSlice(vmiRaw.Object, "status", "conditions")
	if err != nil {
		return nil, fmt.Errorf("failed to find conditions in vmi: %w", err)
	}
	if found {
		liveMigratable, _, err = c.getBoolCondition(conditions, "LiveMigratable")
		if err != nil {
			return nil, err
		}
		ready, _, err = c.getBoolCondition(conditions, "Ready")
		if err != nil {
			return nil, err
		}
		paused, _, err = c.getBoolCondition(conditions, "Paused")
		if err != nil {
			return nil, err
		}
	}

	// get the node where VMI is currently running
	nodeName, _, err := unstructured.NestedString(vmiRaw.Object, "status", "nodeName")
	if err != nil {
		return nil, fmt.Errorf("failed to get vmi nodeName: %w", err)
	}

	// get UID
	// metadata:
	//   uid: ed990548-5f16-4d6e-8b26-6e0acbc1a944
	uid, found, err := unstructured.NestedString(vmiRaw.Object, "metadata", "uid")
	if err != nil || !found {
		return nil, fmt.Errorf("failed to find vmi uid: %w", err)
	}

	// phase
	currentPhase, _, err := unstructured.NestedString(vmiRaw.Object, "status", "phase")
	if err != nil {
		return nil, fmt.Errorf("failed to find vmi phase: %w", err)
	}

	// phase transition timestamps
	// status:
	//   phaseTransitionTimestamps:
	//   - phase: Pending
	//     phaseTransitionTimestamp: "2023-10-11T01:07:56Z"
	//   - phase: Scheduling
	//     phaseTransitionTimestamp: "2023-10-11T01:07:56Z"
	//   - phase: Scheduled
	//     phaseTransitionTimestamp: "2023-10-11T01:08:05Z"
	//   - phase: Running
	//     phaseTransitionTimestamp: "2023-10-11T01:08:07Z"
	var phaseTransitions []*VMIPhaseTransition
	phaseTransitionTimestampsRaw, _, err := unstructured.NestedSlice(vmiRaw.Object, "status", "phaseTransitionTimestamps")
	if err != nil {
		return nil, fmt.Errorf("failed to find phaseTransitionTimestamps in vmi: %w", err)
	}
	for _, rawMap := range phaseTransitionTimestampsRaw {
		typedMap, ok := rawMap.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("wrong type for phaseTransition in slice: expected map[string]interface{}, actual %T", rawMap)
		}
		entryPhase, found, err := c.unstructuredGetValString(typedMap, "phase")
		if err != nil || !found {
			return nil, fmt.Errorf("failed to get key 'phase' in phaseTransitionTimestamp map")
		}
		entryTime, found, err := c.unstructuredGetValTime(typedMap, "phaseTransitionTimestamp")
		if err != nil || !found {
			return nil, fmt.Errorf("failed to get key 'phaseTransitionTimestamp' in phaseTransitionTimestamp map")
		}
		phaseTransitions = append(phaseTransitions, &VMIPhaseTransition{Phase: entryPhase, TransitionTime: entryTime})
	}

	// UID of the owner VM
	// metadata:
	//   ownerReferences:
	//    - apiVersion: kubevirt.io/v1
	//      blockOwnerDeletion: true
	//      controller: true
	//      kind: VirtualMachine
	//      name: test-vm-csi
	//      uid: e5e72d46-382d-4501-9e25-b0dc589e6759
	var vmUID string
	vmOwnerRef, err := c.findOwnerRefByKind(vmiRaw, "kubevirt.io/v1", "VirtualMachine")
	if err != nil {
		return nil, fmt.Errorf("failed to find VM owner of VMI: %w", err)
	}
	if vmOwnerRef != nil {
		vmUID = vmOwnerRef.uid
	}
	return &VirtualMachineInstance{
		Name:             name,
		NameSpace:        namespace,
		UID:              uid,
		RootDisk:         rootDiskName,
		RootDiskPVC:      pvcName,
		LiveMigratable:   liveMigratable,
		Ready:            ready,
		Paused:           paused,
		NodeName:         nodeName,
		Phase:            currentPhase,
		PhaseTransitions: phaseTransitions,
		OwnerVMUID:       vmUID,
	}, nil
}

func (c *Client) findOwnerRefByKind(objRaw *unstructured.Unstructured, apiVersion, kind string) (*ownerRef, error) {
	ownerRefsRaw, _, err := unstructured.NestedSlice(objRaw.Object, "metadata", "ownerReferences")
	if err != nil {
		return nil, fmt.Errorf("failed to find ownerReferences in vmi: %w", err)
	}
	for _, rawMap := range ownerRefsRaw {
		typedMap, ok := rawMap.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("wrong type for ownerReference in slice: expected map[string]interface{}, actual %T", rawMap)
		}
		entryAPIVersion, found, err := c.unstructuredGetValString(typedMap, "apiVersion")
		if err != nil || !found {
			return nil, fmt.Errorf("failed to get key 'apiVersion' in ownerReferences map")
		}
		entryKind, found, err := c.unstructuredGetValString(typedMap, "kind")
		if err != nil || !found {
			return nil, fmt.Errorf("failed to get key 'kind' in ownerReference map")
		}
		if entryAPIVersion != apiVersion || entryKind != kind {
			continue
		}
		entryName, found, err := c.unstructuredGetValString(typedMap, "name")
		if err != nil || !found {
			return nil, fmt.Errorf("failed to get key 'name' in ownerReferences map")
		}
		entryUID, found, err := c.unstructuredGetValString(typedMap, "uid")
		if err != nil || !found {
			return nil, fmt.Errorf("failed to get key 'uid' in ownerReferences map")
		}
		return &ownerRef{
			apiVersion: entryAPIVersion,
			kind:       entryKind,
			name:       entryName,
			uid:        entryUID,
		}, nil
	}
	return nil, nil
}
