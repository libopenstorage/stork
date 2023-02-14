package k8s

import (
	"context"

	ocp_configv1 "github.com/openshift/api/config/v1"
	metaerrors "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IsClusterBeingUpgraded checks if the Kubernetes cluster is being upgraded.
// Currently this only checks for OpenShift clusters.
func IsClusterBeingUpgraded(k8sClient client.Client) (bool, error) {
	cvList := &ocp_configv1.ClusterVersionList{}
	err := k8sClient.List(context.TODO(), cvList, &client.ListOptions{})
	if metaerrors.IsNoMatchError(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if len(cvList.Items) == 0 {
		// Cannot detect upgrade as there is no ClusterVersion
		return false, nil
	}

	cv := cvList.Items[0].DeepCopy()
	if cv.Spec.DesiredUpdate == nil || cv.Spec.DesiredUpdate.Version == "" {
		// No upgrade has been started
		return false, nil
	}

	for _, h := range cv.Status.History {
		if h.Version == cv.Spec.DesiredUpdate.Version {
			return h.State != ocp_configv1.CompletedUpdate, nil
		}
	}

	// Upgrade may have been started, but it is too early as the ClusterVersion
	// history status has not been updated.
	return false, nil
}
