package storkctl

import (
	"fmt"

	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

func newVersionCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	versionCommands := &cobra.Command{
		Use:   "version",
		Short: "Print the version of storkctl",
		Run: func(cmd *cobra.Command, args []string) {
			_, err := fmt.Fprintf(ioStreams.Out, "storkctl Version: %v\n", version.Version)
			if err != nil {
				panic("Failed to print: " + err.Error())
			}

			deployments, err := k8s.Instance().ListDeployments("", metav1.ListOptions{LabelSelector: "name=stork"})
			if err == nil && len(deployments.Items) == 1 && len(deployments.Items[0].Spec.Template.Spec.Containers) == 1 {
				_, err := fmt.Fprintf(ioStreams.Out, "stork Image: %v\n", deployments.Items[0].Spec.Template.Spec.Containers[0].Image)
				if err != nil {
					panic("Failed to print: " + err.Error())
				}
			}
		},
	}
	return versionCommands
}
