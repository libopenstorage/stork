package log

import (
	"github.com/sirupsen/logrus"
	"k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
)

// PodLog Format a log message with pod information
func PodLog(pod *v1.Pod) *logrus.Entry {
	if pod != nil {
		return logrus.WithFields(logrus.Fields{
			"PodName": pod.Name,
		})
	}
	return logrus.WithFields(logrus.Fields{
		"Pod": pod,
	})
}

// DeploymentLog Format a log message with deployment information
func DeploymentLog(deployment *v1beta1.Deployment) *logrus.Entry {
	if deployment != nil {
		return logrus.WithFields(logrus.Fields{
			"DeploymentName": deployment.Name,
		})
	}
	return logrus.WithFields(logrus.Fields{
		"Deployment": deployment,
	})
}

// StatefulSetLog Format a log message with deployment information
func StatefulSetLog(ss *v1beta1.StatefulSet) *logrus.Entry {
	if ss != nil {
		return logrus.WithFields(logrus.Fields{
			"StatefulSetName": ss.Name,
		})
	}
	return logrus.WithFields(logrus.Fields{
		"StatefulSet": ss,
	})
}
