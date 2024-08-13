package kubevirtdynamic

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// migrationPhaseFailed is the phase when the migration has failed
	migrationPhaseFailed = "Failed"
	// migrationPhaseSucceeded is the phase when the migration has succeeded
	migrationPhaseSucceeded = "Succeeded"
)

var (
	migrationResource = schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachineinstancemigrations"}
)

// VirtualMachineInstanceMigration represents "live migration" of KubeVirt VirtualMachineInstance
type VirtualMachineInstanceMigration struct {
	// Namespace of the migration
	NameSpace string
	// Name of the migration
	Name string
	// VMIName is name of the VirtualMachineInstance being migrated
	VMIName string
	// Annotations on the migration obj
	Annotations map[string]string
	// Labels on the migration obj
	Labels map[string]string
	// Completed indicates if the migration has completed
	Completed bool
	// StartTimestamp has the time the migration action started
	StartTimestamp time.Time
	// EndTimestamp has the time the migration action ended
	EndTimestamp time.Time
	// Failed indicates if the migration failed.
	Failed bool
	// Phase the migration currently is in.
	Phase string
	// SourceNode that the VMI is moving from
	SourceNode string
	// TargetNode that the VMI is moving to
	TargetNode string
	// TargetNodeAddress is the address of the target node to use for the migration
	TargetNodeAddress string
	// TargetPod that the VMI is moving to
	TargetPod string
}

// VirtualMachineInstanceMigrationOps is an interface to manage VirtualMachineInstanceMigration objects
type VirtualMachineInstanceMigrationOps interface {
	// CreateVirtualMachineInstanceMigration starts live migration of the specified VMI
	CreateVirtualMachineInstanceMigration(ctx context.Context, vmiNamespace, vmiName string) (
		*VirtualMachineInstanceMigration, error)

	// CreateVirtualMachineInstanceMigrationWithParams starts live migration of the specified VMI
	CreateVirtualMachineInstanceMigrationWithParams(ctx context.Context, vmiNamespace, vmiName string,
		migrationName, generateNamePrefix string, annotations, labels map[string]string) (*VirtualMachineInstanceMigration, error)

	// GetVirtualMachineInstanceMigration retrieves some info about the specified VMI
	GetVirtualMachineInstanceMigration(ctx context.Context, namespace, name string) (
		*VirtualMachineInstanceMigration, error)

	// ListVirtualMachineInstanceMigrations lists migrations in the specified namespace
	ListVirtualMachineInstanceMigrations(ctx context.Context, namespace string, opts metav1.ListOptions) (
		[]*VirtualMachineInstanceMigration, error)
}

// CreateVirtualMachineInstanceMigration starts live migration of the specified VMI
func (c *Client) CreateVirtualMachineInstanceMigration(
	ctx context.Context, vmiNamespace, vmiName string,
) (*VirtualMachineInstanceMigration, error) {
	return c.CreateVirtualMachineInstanceMigrationWithParams(ctx, vmiNamespace, vmiName, "", "", nil, nil)
}

// CreateVirtualMachineInstanceMigrationWithParams starts live migration of the specified VMI
func (c *Client) CreateVirtualMachineInstanceMigrationWithParams(
	ctx context.Context, vmiNamespace, vmiName string, migrationName, generateNamePrefix string,
	annotations, labels map[string]string,
) (*VirtualMachineInstanceMigration, error) {

	if err := c.initClient(); err != nil {
		return nil, err
	}

	migration := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "kubevirt.io/v1",
			"kind":       "VirtualMachineInstanceMigration",
			"metadata": map[string]interface{}{
				"annotations": annotations,
				"labels":      labels,
			},
			"spec": map[string]interface{}{
				"vmiName": vmiName,
			},
		},
	}
	if migrationName != "" {
		migration.Object["metadata"].(map[string]interface{})["name"] = migrationName
	} else if generateNamePrefix != "" {
		migration.Object["metadata"].(map[string]interface{})["generateName"] = generateNamePrefix
	} else {
		migration.Object["metadata"].(map[string]interface{})["generateName"] = vmiName + "-px-"
	}

	result, err := c.client.Resource(migrationResource).Namespace(vmiNamespace).Create(
		ctx, migration, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return c.unstructuredGetVMIMigration(result)
}

// GetVirtualMachineInstanceMigration returns the VirtualMachineInstanceMigration
func (c *Client) GetVirtualMachineInstanceMigration(
	ctx context.Context, namespace, name string,
) (*VirtualMachineInstanceMigration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	migration, err := c.client.Resource(migrationResource).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return c.unstructuredGetVMIMigration(migration)
}

// ListVirtualMachineInstanceMigrations lists migrations in the specified namespace
func (c *Client) ListVirtualMachineInstanceMigrations(
	ctx context.Context, namespace string, opts metav1.ListOptions,
) ([]*VirtualMachineInstanceMigration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	result, err := c.client.Resource(migrationResource).Namespace(namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	var migrations []*VirtualMachineInstanceMigration
	for _, item := range result.Items {
		migration, err := c.unstructuredGetVMIMigration(&item)
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, migration)
	}
	return migrations, nil
}

func (c *Client) unstructuredGetVMIMigration(
	migration *unstructured.Unstructured,
) (*VirtualMachineInstanceMigration, error) {
	var err error
	ret := VirtualMachineInstanceMigration{}

	//spec:
	//  vmiName: fedora-communist-toucan
	//status:
	//  migrationState:
	//    completed: true
	//    endTimestamp: "2023-09-27T17:42:11Z"
	//    migrationConfiguration:
	//      allowAutoConverge: false
	//      allowPostCopy: false
	//      bandwidthPerMigration: "0"
	//      completionTimeoutPerGiB: 800
	//      nodeDrainTaintKey: kubevirt.io/drain
	//      parallelMigrationsPerCluster: 5
	//      parallelOutboundMigrationsPerNode: 2
	//      progressTimeout: 150
	//      unsafeMigrationOverride: false
	//    migrationUid: 71ff0907-28de-4859-8c42-07f539fec571
	//    mode: PreCopy
	//    sourceNode: stork-integ-src-2pjbr-worker-0-5m8pf
	//    startTimestamp: "2023-09-27T17:42:02Z"
	//    targetDirectMigrationNodePorts:
	//      "38199": 49153
	//      "40705": 0
	//      "45289": 49152
	//    targetNode: stork-integ-src-2pjbr-worker-0-zt9ll
	//    targetNodeAddress: 10.131.0.41
	//    targetNodeDomainDetected: true
	//    targetPod: virt-launcher-fedora-communist-toucan-dtddt
	//  phase: Succeeded

	// name
	ret.Name, _, err = unstructured.NestedString(migration.Object, "metadata", "name")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'name' from the vmi migration metadata: %w", err)
	}
	// namespace
	ret.NameSpace, _, err = unstructured.NestedString(migration.Object, "metadata", "namespace")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'namespace' from the vmi migration metadata: %w", err)
	}
	// vmiName
	ret.VMIName, _, err = unstructured.NestedString(migration.Object, "spec", "vmiName")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'vmiName' from the vmi migration spec: %w", err)
	}
	// annotations
	ret.Annotations, _, err = unstructured.NestedStringMap(migration.Object, "metadata", "annotations")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'annotations' from the vmi migration spec: %w", err)
	}
	// labels
	ret.Labels, _, err = unstructured.NestedStringMap(migration.Object, "metadata", "labels")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'labels' from the vmi migration spec: %w", err)
	}
	// phase
	ret.Phase, _, err = unstructured.NestedString(migration.Object, "status", "phase")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'phase' from the vmi migration status: %w", err)
	}
	// completed
	var found bool
	ret.Completed, found, err = unstructured.NestedBool(migration.Object, "status", "migrationState", "completed")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'completed' from the vmi migration status: %w", err)
	}
	// sometimes migrationState is not present; we need to look at the Phase
	if !found && (ret.Phase == migrationPhaseFailed || ret.Phase == migrationPhaseSucceeded) {
		ret.Completed = true
	}
	// failed
	ret.Failed, found, err = unstructured.NestedBool(migration.Object, "status", "migrationState", "failed")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'failed' from the vmi migration status: %w", err)
	}
	// migrationState is not present if the pod fails to get scheduled; we need to look at the Phase
	if !found && ret.Phase == migrationPhaseFailed {
		ret.Failed = true
	}
	// startTimestamp
	ret.StartTimestamp, _, err = c.unstructuredGetTimestamp(migration.Object, "status", "migrationState", "startTimestamp")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'startTimestamp' from the vmi migration status: %w", err)
	}
	// endTimestamp
	ret.EndTimestamp, _, err = c.unstructuredGetTimestamp(migration.Object, "status", "migrationState", "endTimestamp")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'endTimestamp' from the vmi migration status: %w", err)
	}
	// sourceNode
	ret.SourceNode, _, err = unstructured.NestedString(migration.Object, "status", "migrationState", "sourceNode")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'sourceNode' from the vmi migration status: %w", err)
	}
	// targetNode
	ret.TargetNode, _, err = unstructured.NestedString(migration.Object, "status", "migrationState", "targetNode")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'targetNode' from the vmi migration status: %w", err)
	}
	// targetNodeAddress
	ret.TargetNodeAddress, _, err = unstructured.NestedString(migration.Object, "status", "migrationState", "targetNodeAddress")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'targetNodeAddress' from the vmi migration status: %w", err)
	}
	// targetPod
	ret.TargetPod, _, err = unstructured.NestedString(migration.Object, "status", "migrationState", "targetPod")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'targetPod' from the vmi migration status: %w", err)
	}

	return &ret, nil
}
