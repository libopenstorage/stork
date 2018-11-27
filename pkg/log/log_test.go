// +build unittest

package log

import (
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLog(t *testing.T) {
	t.Run("podLogTest", podLogTest)
}

func podLogTest(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testpod",
			Namespace: "testnamespace",
		},
	}
	PodLog(pod).Infof("valid pod info")
	pod = nil
	PodLog(pod).Infof("nil pod info")

	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "testpod",
			Namespace:       "testnamespace",
			OwnerReferences: []metav1.OwnerReference{},
		},
	}
	PodLog(pod).Infof("empty pod owner")
	pod.OwnerReferences = append(pod.OwnerReferences,
		metav1.OwnerReference{
			Kind:       "testkind",
			Name:       "testname",
			Controller: nil,
		})
	PodLog(pod).Infof("pod owner nil controller")
	controller := true
	pod.OwnerReferences[0].Controller = &controller
	PodLog(pod).Infof("pod owner controller")
}
