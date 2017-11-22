package log

import (
	"github.com/sirupsen/logrus"
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
