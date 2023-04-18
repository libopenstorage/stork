package k8s

import (
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

// InfoEvent logs and records a info event for the given object on the given recorder
func InfoEvent(
	recorder record.EventRecorder,
	object runtime.Object,
	reason, message string,
) {
	logrus.Info(message)
	recorder.Event(object, v1.EventTypeNormal, reason, message)
}

// WarningEvent logs and records a warning event for the given object on the given recorder
func WarningEvent(
	recorder record.EventRecorder,
	object runtime.Object,
	reason, message string,
) {
	logrus.Warn(message)
	recorder.Event(object, v1.EventTypeWarning, reason, message)
}
