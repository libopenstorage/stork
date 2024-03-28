package k8s

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	cluster_v1alpha1 "sigs.k8s.io/cluster-api/pkg/apis/deprecated/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/libopenstorage/operator/pkg/constants"
	"github.com/libopenstorage/operator/pkg/util/maps"
)

/*
file contains node level k8s utility functions
*/

// NodeInfo contains information of a k8s node, consumed by controllers
type NodeInfo struct {
	NodeName             string
	LastPodSeenTime      time.Time
	CordonedRestartDelay time.Duration
}

// IsNodeBeingDeleted returns true if the underlying machine for the Kubernetes node is being deleted.
// This method is only supported on platforms that use the cluster-api (https://github.com/kubernetes-sigs/cluster-api)
func IsNodeBeingDeleted(node *v1.Node, cl client.Client) (bool, error) {
	// check if node is managed by a cluster API machine and if the machine is marked for deletion
	if machineName, present := node.Annotations[constants.AnnotationClusterAPIMachine]; present && len(machineName) > 0 {
		machine := &cluster_v1alpha1.Machine{}
		err := cl.Get(context.TODO(), client.ObjectKey{Name: machineName, Namespace: "default"}, machine)
		if err != nil {
			return false, fmt.Errorf("failed to get machine: default/%s due to: %v", machineName, err)
		}

		if machine.GetDeletionTimestamp() != nil {
			logrus.Infof("machine: %s is being deleted. timestamp set: %v.",
				machineName, machine.GetDeletionTimestamp())
			return true, nil
		}
	}
	return false, nil
}

// IsPodRecentlyCreatedAfterNodeCordoned returns true if the given node is cordoned and the pod has been created
// within the delay, exponential backoff is applied here.
func IsPodRecentlyCreatedAfterNodeCordoned(
	node *v1.Node,
	nodeInfoMap maps.SyncMap[string, *NodeInfo],
	cluster *corev1.StorageCluster,
) bool {
	nodeInfo, ok := nodeInfoMap.Load(node.Name)
	// The pod is not present in the cluster
	if !ok || nodeInfo == nil {
		// We don't set the last seen time here, as it will get updated in their respective
		// reconcile loops, if the pod is present or when it is created.
		nodeInfo = &NodeInfo{
			NodeName:             node.Name,
			CordonedRestartDelay: constants.DefaultCordonedRestartDelay,
		}
		nodeInfoMap.Store(node.Name, nodeInfo)
	}

	cordoned, cordonTime := IsNodeCordoned(node)
	if !cordoned || cordonTime.IsZero() {
		// Pod created but node not cordoned, reset the delay time
		nodeInfo.CordonedRestartDelay = constants.DefaultCordonedRestartDelay
		return false
	}

	// When the node is cordoned and the pod's last seen time is empty, it could either mean that the pod
	// was never present or it got deleted when the operator was down. As the operator has no knowledge,
	// we assume that the pod was deleted during a cordon+drain and set the last seen time to the cordon time.
	if nodeInfo.LastPodSeenTime.IsZero() {
		nodeInfo.LastPodSeenTime = cordonTime
	}

	var waitDuration time.Duration
	overwriteRestartDelay := false
	if duration, err := strconv.Atoi(cluster.Annotations[constants.AnnotationCordonedRestartDelay]); err == nil {
		waitDuration = time.Duration(duration) * time.Second
		overwriteRestartDelay = true
	} else {
		waitDuration = nodeInfo.CordonedRestartDelay
	}

	cutOffTime := time.Now().Add(-waitDuration)

	// If node recently cordoned, return true.
	if cutOffTime.Before(cordonTime) {
		return true
	}

	// The pod won't get deleted if recently created, keep the restart delay unchanged.
	// Otherwise the pod will get deleted, increase the restart delay for the next pod creation.
	recentlyCreated := cutOffTime.Before(nodeInfo.LastPodSeenTime)
	if !recentlyCreated && !overwriteRestartDelay {
		nodeInfo.CordonedRestartDelay = waitDuration * 2
		if nodeInfo.CordonedRestartDelay > constants.MaxCordonedRestartDelay {
			nodeInfo.CordonedRestartDelay = constants.MaxCordonedRestartDelay
		}
		logrus.Infof("node %s is cordoned, next pod restart is scheduled in %s", nodeInfo.NodeName, nodeInfo.CordonedRestartDelay)
	}
	return recentlyCreated
}

// IsNodeCordoned returns true if the given noode is marked unschedulable. It
// also returns the time when the node was cordoned if available in the node
// taints.
func IsNodeCordoned(node *v1.Node) (bool, time.Time) {
	if node.Spec.Unschedulable {
		for _, taint := range node.Spec.Taints {
			if taint.Key == v1.TaintNodeUnschedulable {
				if taint.TimeAdded != nil {
					return true, taint.TimeAdded.Time
				}
				break
			}
		}
		return true, time.Time{}
	}
	return false, time.Time{}
}
