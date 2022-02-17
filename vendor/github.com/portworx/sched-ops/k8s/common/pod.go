package common

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	schederrors "github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

// GetPodsByOwner returns pods for the given owner and namespace
func GetPodsByOwner(client v1.CoreV1Interface, ownerUID types.UID, namespace string) ([]corev1.Pod, error) {
	podList, err := client.Pods(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var result []corev1.Pod
	for _, pod := range podList.Items {
		for _, owner := range pod.OwnerReferences {
			if owner.UID == ownerUID {
				result = append(result, pod)
			}
		}
	}

	if len(result) == 0 {
		return nil, schederrors.ErrPodsNotFound
	}

	return result, nil
}

// GeneratePodsOverviewString returns a string description of pods.
func GeneratePodsOverviewString(pods []corev1.Pod) string {
	var buffer bytes.Buffer
	for _, p := range pods {
		running := IsPodRunning(p)
		ready := IsPodReady(p)
		podString := fmt.Sprintf("  pod name:%s namespace:%s running:%v ready:%v node:%s\n", p.Name, p.Namespace, running, ready, p.Status.HostIP)
		buffer.WriteString(podString)
	}

	return buffer.String()
}

// DeletePods deletes the given pods
func DeletePods(client v1.CoreV1Interface, pods []corev1.Pod, force bool) error {
	deleteOptions := metav1.DeleteOptions{}
	if force {
		gracePeriodSec := int64(0)
		deleteOptions.GracePeriodSeconds = &gracePeriodSec
	}

	for _, pod := range pods {
		if err := client.Pods(pod.Namespace).Delete(context.TODO(), pod.Name, deleteOptions); err != nil {
			return err
		}
	}

	return nil
}

// WaitForPodsToBeDeleted waits for pods to be deleted
func WaitForPodsToBeDeleted(client v1.CoreV1Interface, podsToDelete []corev1.Pod, timeout time.Duration) error {
	var wg sync.WaitGroup
	errChan := make(chan error)

	// Wait for pods to be deleted
	for index, pod := range podsToDelete {
		wg.Add(1)
		go func(pod corev1.Pod, index int) {
			defer wg.Done()
			if err := WaitForPodDeletion(client, pod, timeout); err != nil {
				errChan <- fmt.Errorf("Failed to delete pod %s, Err: %v", pod.Name, err)
			}
		}(pod, index)

	}
	wg.Wait()
	close(errChan)

	ok := true
	var errorString string
	for err := range errChan {
		errorString += fmt.Sprintf("%v\n", err)
		ok = false
	}

	if !ok {
		return fmt.Errorf("Failed to delete pods:\n%s", errorString)
	}
	return nil
}

// WaitForPodDeletion waits for given timeout for given pod to be deleted
func WaitForPodDeletion(client v1.CoreV1Interface, pod corev1.Pod, timeout time.Duration) error {
	t := func() (interface{}, bool, error) {
		p, err := GetPodByName(client, pod.Name, pod.Namespace)
		if err != nil {
			if err == schederrors.ErrPodsNotFound {
				return nil, false, nil
			}

			return nil, true, err
		}

		if p != nil && p.UID == pod.UID {
			return nil, true, fmt.Errorf("pod %s:%s is still present in the system", p.Namespace, p.Name)
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, 5*time.Second); err != nil {
		return err
	}

	return nil
}

// GetPodByName returns pod for the given pod name and namespace
func GetPodByName(client v1.CoreV1Interface, podName string, namespace string) (*corev1.Pod, error) {
	pod, err := client.Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		return nil, schederrors.ErrPodsNotFound
	}

	return pod, nil
}

// IsPodReady checks if all containers in a pod are ready (passed readiness probe).
func IsPodReady(pod corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodSucceeded {
		return false
	}

	// If init containers are running, return false since the actual container would not have started yet
	for _, c := range pod.Status.InitContainerStatuses {
		if c.State.Running != nil {
			return false
		}
	}

	for _, c := range pod.Status.ContainerStatuses {
		if c.State.Terminated != nil &&
			c.State.Terminated.ExitCode == 0 &&
			c.State.Terminated.Reason == "Completed" {
			continue // container has exited successfully
		}

		if c.State.Running == nil {
			return false
		}

		if !c.Ready {
			return false
		}
	}

	return true
}

// IsPodRunning checks if all containers in a pod are in running state.
func IsPodRunning(pod corev1.Pod) bool {
	// If init containers are running, return false since the actual container would not have started yet
	for _, c := range pod.Status.InitContainerStatuses {
		if c.State.Running != nil {
			return false
		}
	}

	for _, c := range pod.Status.ContainerStatuses {
		if c.State.Running == nil {
			return false
		}
	}

	return true
}
