package storkctl

import (
	"fmt"

	"github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	"github.com/libopenstorage/stork/pkg/snapshot"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var defaultStrokSnapshotStorageClass = "stork-snapshot-sc"
var pvcSubcommand = "persistentvolumeclaims"
var pvcAliases = []string{"persistentvolumeclaim", "volume", "pvc"}

func newCreatePVCCommand(cmdFactory Factory) *cobra.Command {
	var snapName string
	var pvcName string
	var sourceNamespace string
	var accessMode string
	var size string
	createPVCCommand := &cobra.Command{
		Use:     pvcSubcommand,
		Aliases: pvcAliases,
		Short:   "Create persistent volume claims (PVCs) from snapshots",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				handleError(fmt.Errorf("Exactly one argument needs to be provided for pvc name"))
			} else {
				pvcName = args[0]
			}
			if len(snapName) == 0 {
				handleError(fmt.Errorf("Snapshot name needs to be given"))
			}
			if len(size) == 0 {
				handleError(fmt.Errorf("Size needs to be provided"))
			}
			quantity, err := resource.ParseQuantity(size)
			if err != nil {
				handleError(fmt.Errorf("Invalid size: %v", err))
			}

			namespace := cmdFactory.GetNamespace()

			pvc := &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pvcName,
					Namespace: namespace,
					Annotations: map[string]string{
						client.SnapshotPVCAnnotation: snapName,
					},
				},
				Spec: v1.PersistentVolumeClaimSpec{
					StorageClassName: &defaultStrokSnapshotStorageClass,
					AccessModes:      []v1.PersistentVolumeAccessMode{v1.PersistentVolumeAccessMode(accessMode)},
					Resources: v1.ResourceRequirements{
						Requests: map[v1.ResourceName]resource.Quantity{
							v1.ResourceStorage: quantity,
						},
					},
				},
			}
			if len(sourceNamespace) != 0 {
				pvc.Annotations[snapshotcontroller.StorkSnapshotSourceNamespaceAnnotation] = sourceNamespace
			}
			_, err = k8s.Instance().CreatePersistentVolumeClaim(pvc)
			if err != nil {
				handleError(err)
			}
			fmt.Printf("PersistentVolumeClaim %v created successfully\n", pvcName)
		},
	}
	createPVCCommand.Flags().StringVarP(&snapName, "snapshot", "s", "", "Name of the snapshot to use to create the PVC")
	createPVCCommand.Flags().StringVar(&sourceNamespace, "source-ns", "", "The source namespace if the snapshot was created in a different namespace")
	createPVCCommand.Flags().StringVarP(&accessMode, "acccess-mode", "a", string(v1.ReadWriteOnce), "Access mode for the new PVC")
	createPVCCommand.Flags().StringVar(&size, "size", "", "Size for the new PVC (example 2Gi)")

	return createPVCCommand
}
