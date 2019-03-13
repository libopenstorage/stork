package log

import (
	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/sirupsen/logrus"
	appv1 "k8s.io/api/apps/v1"
	appv1beta1 "k8s.io/api/apps/v1beta1"
	appv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
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
	return logrus.WithFields(logrus.Fields{})
}

// DeploymentV1Log Format a log message with deployment information
func DeploymentV1Log(deployment *appv1.Deployment) *logrus.Entry {
	if deployment != nil {
		return logrus.WithFields(logrus.Fields{
			"DeploymentName": deployment.Name,
			"Namespace":      deployment.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// DeploymentV1Beta1Log Format a log message with deployment information
func DeploymentV1Beta1Log(deployment *appv1beta1.Deployment) *logrus.Entry {
	if deployment != nil {
		return logrus.WithFields(logrus.Fields{
			"DeploymentName": deployment.Name,
			"Namespace":      deployment.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// DeploymentV1Beta2Log Format a log message with deployment information
func DeploymentV1Beta2Log(deployment *appv1beta2.Deployment) *logrus.Entry {
	if deployment != nil {
		return logrus.WithFields(logrus.Fields{
			"DeploymentName": deployment.Name,
			"Namespace":      deployment.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// StatefulSetV1Log Format a log message with statefulset information
func StatefulSetV1Log(ss *appv1.StatefulSet) *logrus.Entry {
	if ss != nil {
		return logrus.WithFields(logrus.Fields{
			"StatefulSetName": ss.Name,
			"Namespace":       ss.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// StatefulSetV1Beta1Log Format a log message with statefulset information
func StatefulSetV1Beta1Log(ss *appv1beta1.StatefulSet) *logrus.Entry {
	if ss != nil {
		return logrus.WithFields(logrus.Fields{
			"StatefulSetName": ss.Name,
			"Namespace":       ss.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// StatefulSetV1Beta2Log Format a log message with statefulset information
func StatefulSetV1Beta2Log(ss *appv1beta2.StatefulSet) *logrus.Entry {
	if ss != nil {
		return logrus.WithFields(logrus.Fields{
			"StatefulSetName": ss.Name,
			"Namespace":       ss.Namespace,
		})
	}
	return logrus.WithFields(logrus.Fields{})
}

// SnapshotLog formats a log message with snapshot information
func SnapshotLog(snap *crdv1.VolumeSnapshot) *logrus.Entry {
	if snap != nil {
		return logrus.WithFields(logrus.Fields{
			"VolumeSnapshotName": snap.Metadata.Name,
			"Namespace":          snap.Metadata.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// VolumeSnapshotScheduleLog formats a log message with snapshotschedule information
func VolumeSnapshotScheduleLog(snapshotSchedule *storkv1.VolumeSnapshotSchedule) *logrus.Entry {
	if snapshotSchedule != nil {
		return logrus.WithFields(logrus.Fields{
			"VolumeSnapshotScheduleName": snapshotSchedule.Name,
			"Namespace":                  snapshotSchedule.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// RuleLog formats a log message with Rule information
func RuleLog(
	rule *storkv1.Rule,
	object runtime.Object,
) *logrus.Entry {
	fields := logrus.Fields{}
	metadata, err := meta.Accessor(object)
	if err == nil {
		fields["Name"] = metadata.GetName()
		namespace := metadata.GetNamespace()
		if namespace != "" {
			fields["Namespace"] = namespace
		}
	}
	objectType, err := meta.TypeAccessor(object)
	if err == nil {
		fields["Kind"] = objectType.GetKind()
	}
	if rule != nil {
		fields["Rule"] = rule.Name
	}
	return logrus.WithFields(fields)

}

// MigrationLog formats a log message with migration information
func MigrationLog(migration *storkv1.Migration) *logrus.Entry {
	if migration != nil {
		return logrus.WithFields(logrus.Fields{
			"MigrationName":      migration.Name,
			"MigrationNamespace": migration.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// MigrationScheduleLog formats a log message with migrationschedule information
func MigrationScheduleLog(migrationSchedule *storkv1.MigrationSchedule) *logrus.Entry {
	if migrationSchedule != nil {
		return logrus.WithFields(logrus.Fields{
			"MigrationScheduleName":      migrationSchedule.Name,
			"MigrationScheduleNamespace": migrationSchedule.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}

// GroupSnapshotLog formats a log message with groupsnapshot information
func GroupSnapshotLog(groupsnapshot *storkv1.GroupVolumeSnapshot) *logrus.Entry {
	if groupsnapshot != nil {
		return logrus.WithFields(logrus.Fields{
			"GroupSnapshotName":      groupsnapshot.Name,
			"GroupSnapshotNamespace": groupsnapshot.Namespace,
		})
	}

	return logrus.WithFields(logrus.Fields{})
}
