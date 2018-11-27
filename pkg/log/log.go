package log

import (
	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/sirupsen/logrus"
	appv1 "k8s.io/api/apps/v1"
	appv1beta1 "k8s.io/api/apps/v1beta1"
	appv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
)

// PodLog Format a log message with pod information
func PodLog(pod *v1.Pod) *logrus.Entry {
	if pod != nil {
		fields := logrus.Fields{
			"PodName":   pod.Name,
			"Namespace": pod.Namespace,
		}
		for _, owner := range pod.OwnerReferences {
			if owner.Controller != nil && *owner.Controller {
				fields["Owner"] = owner.Kind + "/" + owner.Name
				break
			}
		}
		return logrus.WithFields(fields)
	}
	return logrus.WithFields(logrus.Fields{
		"Pod": pod,
	})
}

// DeploymentV1Log Format a log message with deployment information
func DeploymentV1Log(deployment *appv1.Deployment) *logrus.Entry {
	if deployment != nil {
		return logrus.WithFields(logrus.Fields{
			"DeploymentName": deployment.Name,
			"Namespace":      deployment.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{
		"Deployment": deployment,
	})
}

// DeploymentV1Beta1Log Format a log message with deployment information
func DeploymentV1Beta1Log(deployment *appv1beta1.Deployment) *logrus.Entry {
	if deployment != nil {
		return logrus.WithFields(logrus.Fields{
			"DeploymentName": deployment.Name,
			"Namespace":      deployment.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{
		"Deployment": deployment,
	})
}

// DeploymentV1Beta2Log Format a log message with deployment information
func DeploymentV1Beta2Log(deployment *appv1beta2.Deployment) *logrus.Entry {
	if deployment != nil {
		return logrus.WithFields(logrus.Fields{
			"DeploymentName": deployment.Name,
			"Namespace":      deployment.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{
		"Deployment": deployment,
	})
}

// StatefulSetV1Log Format a log message with statefulset information
func StatefulSetV1Log(ss *appv1.StatefulSet) *logrus.Entry {
	if ss != nil {
		return logrus.WithFields(logrus.Fields{
			"StatefulSetName": ss.Name,
			"Namespace":       ss.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{
		"StatefulSet": ss,
	})
}

// StatefulSetV1Beta1Log Format a log message with statefulset information
func StatefulSetV1Beta1Log(ss *appv1beta1.StatefulSet) *logrus.Entry {
	if ss != nil {
		return logrus.WithFields(logrus.Fields{
			"StatefulSetName": ss.Name,
			"Namespace":       ss.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{
		"StatefulSet": ss,
	})
}

// StatefulSetV1Beta2Log Format a log message with statefulset information
func StatefulSetV1Beta2Log(ss *appv1beta2.StatefulSet) *logrus.Entry {
	if ss != nil {
		return logrus.WithFields(logrus.Fields{
			"StatefulSetName": ss.Name,
			"Namespace":       ss.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{
		"StatefulSet": ss,
	})
}

// SnapshotLog formats a log message with snapshot information
func SnapshotLog(snap *crdv1.VolumeSnapshot) *logrus.Entry {
	if snap != nil {
		return logrus.WithFields(logrus.Fields{
			"SnapshotName": snap.Metadata.Name,
			"Namespace":    snap.Metadata.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{
		"Snapshot": snap,
	})
}
