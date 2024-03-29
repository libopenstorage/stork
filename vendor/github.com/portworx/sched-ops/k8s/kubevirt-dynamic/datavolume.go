package kubevirtdynamic

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	dvResource = schema.GroupVersionResource{Group: "cdi.kubevirt.io", Version: "v1beta1", Resource: "datavolumes"}
)

// DataVolume represents an instance of KubeVirt CDI DataVolume
type DataVolume struct {
	// Name of the data volume
	Name string
	// Namespace of the data volume
	NameSpace string
	// UID from the data volume metadata
	UID string
	// ClaimName is the name of the PersistentVolumeClaim for this data volume
	ClaimName string
	// Phase of the data volume is in e.g. Pending, ImportScheduled
	Phase string
	// OwnerVMUID is the UID of the VirtualMachine object that owns this data volume
	OwnerVMUID string
}

// DataVolumeOps is an interface to manage DataVolume objects
type DataVolumeOps interface {
	// GetDataVolume retrieves some info about the specified data volume
	GetDataVolume(ctx context.Context, namespace, name string) (*DataVolume, error)
	// ListDataVolumes lists data volumes in the specified namespace
	ListDataVolumes(ctx context.Context, namespace string, opts metav1.ListOptions) ([]*DataVolume, error)
}

// GetDataVolume returns the data volume
func (c *Client) GetDataVolume(
	ctx context.Context, namespace, name string,
) (*DataVolume, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	dvRaw, err := c.client.Resource(dvResource).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return c.unstructuredGetDataVolume(dvRaw)
}

// ListDataVolumes lists data volumes in the specified namespace
func (c *Client) ListDataVolumes(
	ctx context.Context, namespace string, opts metav1.ListOptions,
) ([]*DataVolume, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	result, err := c.client.Resource(dvResource).Namespace(namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	var dvs []*DataVolume
	for _, item := range result.Items {
		dv, err := c.unstructuredGetDataVolume(&item)
		if err != nil {
			return nil, err
		}
		dvs = append(dvs, dv)
	}
	return dvs, nil
}

func (c *Client) unstructuredGetDataVolume(
	dvRaw *unstructured.Unstructured,
) (*DataVolume, error) {
	var err error
	ret := DataVolume{}

	//- apiVersion: cdi.kubevirt.io/v1beta1
	//  kind: DataVolume
	//  metadata:
	//    annotations:
	//      cdi.kubevirt.io/storage.deleteAfterCompletion: "true"
	//    creationTimestamp: "2024-01-21T01:42:39Z"
	//    generation: 1
	//    name: fedora-sour-goat-disk-far-rattlesnake
	//    namespace: default
	//    ownerReferences:
	//    - apiVersion: kubevirt.io/v1
	//      blockOwnerDeletion: false
	//      kind: VirtualMachine
	//      name: fedora-sour-goat
	//      uid: fe8608a6-08b2-4e1c-a495-2db447a3e345
	//    resourceVersion: "68835014"
	//    uid: 21ebc92f-dcb4-4a4a-b965-31a44a61b5b5
	//  spec:
	//    preallocation: false
	//    source:
	//      blank: {}
	//    storage:
	//      accessModes:
	//      - ReadWriteMany
	//      resources:
	//        requests:
	//          storage: 3Gi
	//      storageClassName: sc-sharedv4svc-nolock
	//      volumeMode: Filesystem
	//  status:
	//    claimName: fedora-sour-goat-disk-far-rattlesnake
	//    conditions:
	//    - lastHeartbeatTime: "2024-01-21T01:42:42Z"
	//      lastTransitionTime: "2024-01-21T01:42:42Z"
	//      message: PVC fedora-sour-goat-disk-far-rattlesnake Pending
	//      reason: Pending
	//      status: "False"
	//      type: Bound
	//    - lastHeartbeatTime: "2024-01-21T01:42:42Z"
	//      lastTransitionTime: "2024-01-21T01:42:42Z"
	//      status: "False"
	//      type: Ready
	//    - lastHeartbeatTime: "2024-01-21T01:42:42Z"
	//      lastTransitionTime: "2024-01-21T01:42:42Z"
	//      status: "False"
	//      type: Running
	//    phase: Pending
	//    progress: N/A

	// name
	ret.Name, _, err = unstructured.NestedString(dvRaw.Object, "metadata", "name")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'name' from the data volume metadata: %w", err)
	}
	// namespace
	ret.NameSpace, _, err = unstructured.NestedString(dvRaw.Object, "metadata", "namespace")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'namespace' from the data volume metadata: %w", err)
	}
	// get UID
	ret.UID, _, err = unstructured.NestedString(dvRaw.Object, "metadata", "uid")
	if err != nil {
		return nil, fmt.Errorf("failed to find data volume uid: %w", err)
	}
	// claimName
	ret.ClaimName, _, err = unstructured.NestedString(dvRaw.Object, "status", "claimName")
	if err != nil {
		return nil, fmt.Errorf("failed to find data volume claimName: %w", err)
	}
	// phase
	ret.Phase, _, err = unstructured.NestedString(dvRaw.Object, "status", "phase")
	if err != nil {
		return nil, fmt.Errorf("failed to find data volume phase: %w", err)
	}
	// UID of the owner VM
	vmOwnerRef, err := c.findOwnerRefByKind(dvRaw, "kubevirt.io/v1", "VirtualMachine")
	if err != nil {
		return nil, fmt.Errorf("failed to find VM owner of data volume %s/%s: %w", ret.NameSpace, ret.Name, err)
	}
	if vmOwnerRef != nil {
		ret.OwnerVMUID = vmOwnerRef.uid
	}
	return &ret, nil
}
