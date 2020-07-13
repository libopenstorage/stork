package storkctl

import (
	"context"
	"io"

	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/kubectl/pkg/util/interrupt"
)

// printObjectsWithWatch prints  by listening to even updates
func printObjectsWithWatch(cmd *cobra.Command, object runtime.Object, cmdFactory Factory, columns []string, printerFunc interface{}, out io.Writer) error {
	var watchObject watch.Interface
	var err error
	watchObject, err = storkops.Instance().WatchStorkResources(cmdFactory.GetNamespace(), object)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	intr := interrupt.New(nil, cancel)
	return intr.Run(func() error {
		_, err := watchtools.UntilWithoutRetry(ctx, watchObject, func(e watch.Event) (bool, error) {
			if err := printObjects(cmd, object, cmdFactory, columns, printerFunc, out); err != nil {
				return false, err
			}
			return false, nil
		})
		return err
	})
}
