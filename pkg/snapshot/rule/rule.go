package rule

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apis_stork "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/cmdexecutor"
	"github.com/libopenstorage/stork/pkg/cmdexecutor/status"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"github.com/skyrings/skyring-common/tools/uuid"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/apis/core"
)

const (
	defaultCmdExecutorImage     = "openstorage/cmdexecutor:0.1"
	cmdExecutorImageOverrideKey = "stork/cmdexecutor-image"
	storkServiceAccount         = "stork-account"
	podsWithRunningCommandsKey  = "stork/pods-with-running-cmds"

	// constants
	perPodCommandExecTimeout = 900 // 15 minutes

	execPodCmdRetryInterval = 5 * time.Second
	execPodCmdRetryFactor   = 1
	execPodStepLow          = 12
	execPodStepMed          = 36
	execPodStepsHigh        = math.MaxInt32
)

type ruleType string

const (
	preSnapRule               ruleType = "preSnapRule"
	postSnapRule              ruleType = "postSnapRule"
	storkRuleAnnotationPrefix          = "stork.rule"
	preSnapRuleAnnotationKey           = storkRuleAnnotationPrefix + "/pre-snapshot"
	postSnapRuleAnnotationKey          = storkRuleAnnotationPrefix + "/post-snapshot"
)

var ruleAnnotationKeys = map[ruleType]string{
	preSnapRule:  preSnapRuleAnnotationKey,
	postSnapRule: postSnapRuleAnnotationKey,
}

// pod is a simple type to encapsulate a pod's uid and namespace
type pod struct {
	uid       string
	namespace string
}

type podErrorResponse struct {
	pod v1.Pod
	err error
}

// commandTask tracks pods where commands for a taskID might still be running
type commandTask struct {
	taskID string
	pods   []pod
}

var execCmdBackoff = wait.Backoff{
	Duration: execPodCmdRetryInterval,
	Factor:   execPodCmdRetryFactor,
	Steps:    execPodStepsHigh,
}

var snapAPICallBackoff = wait.Backoff{
	Duration: 2 * time.Second,
	Factor:   1.5,
	Steps:    20,
}

// ValidateSnapRule validates the rules if they are present in the given snapshot's annotations
func ValidateSnapRule(snap *crdv1.VolumeSnapshot) error {
	if snap.Metadata.Annotations != nil {
		rTypes := []ruleType{preSnapRule, postSnapRule}
		for _, rType := range rTypes {
			ruleName, present := snap.Metadata.Annotations[ruleAnnotationKeys[rType]]
			if present && len(ruleName) > 0 {
				rule, err := k8s.Instance().GetRule(ruleName, snap.Metadata.Namespace)
				if err != nil {
					return err
				}

				for _, item := range rule.Spec {
					for _, action := range item.Actions {
						if action.Type == apis_stork.RuleActionCommand {
							if action.Background && rType == postSnapRule {
								return fmt.Errorf("background actions are not supported for post snapshot rules")
							}
						} else {
							return fmt.Errorf("unsupported action type: %s in rule: [%s] %s",
								action.Type, rule.GetNamespace(), rule.GetName())
						}
					}
				}
			}
		}
	}

	return nil
}

// ExecutePreSnapRule executes the pre snapshot rule. pvcs is a list of PVCs that are associated
// with the snapshot. It returns a channel which the caller can trigger to delete the termination of background commands
func ExecutePreSnapRule(pvcs []v1.PersistentVolumeClaim, snap *crdv1.VolumeSnapshot) (chan bool, error) {
	return executeSnapRule(pvcs, snap, preSnapRule)
}

// ExecutePostSnapRule executes the post snapshot rule for the given snapshot. pvcs is a list of PVCs
// that are associated with the snapshot
func ExecutePostSnapRule(pvcs []v1.PersistentVolumeClaim, snap *crdv1.VolumeSnapshot) error {
	_, err := executeSnapRule(pvcs, snap, postSnapRule)
	return err
}

// terminateCommandInPods terminates a previously running background command on given pods for given task ID
func terminateCommandInPods(snap *crdv1.VolumeSnapshot, pods []v1.Pod, taskID string) error {
	killFile := fmt.Sprintf(cmdexecutor.KillFileFormat, taskID)
	failedPods, err := runCommandOnPods(pods, fmt.Sprintf("touch %s", killFile), execPodStepsHigh, false)

	updateErr := updateRunningCommandPodListInSnap(snap, failedPods, taskID)
	if updateErr != nil {
		log.SnapshotLog(snap).Warnf("Failed to update list of pods with running command in snap due to: %v", updateErr)
	}

	return err
}

// PerformRuleRecovery terminates potential background commands running pods for the given snapshot
func PerformRuleRecovery() error {
	allSnaps, err := k8s.Instance().ListSnapshots(v1.NamespaceAll)
	if err != nil {
		logrus.Errorf("Failed to list all snapshots due to: %v. Will retry.", err)
		return err
	}

	if allSnaps == nil {
		return nil
	}

	var lastError error
	for _, snap := range allSnaps.Items {
		if snap.Metadata.Annotations != nil {
			value := snap.Metadata.Annotations[podsWithRunningCommandsKey]
			if len(value) > 0 {
				backgroundPodList := make([]v1.Pod, 0)
				log.SnapshotLog(&snap).Infof("Performing recovery to terminate commands tracker: %v", value)
				taskTracker := commandTask{}

				err := json.Unmarshal([]byte(value), &taskTracker)
				if err != nil {
					err = fmt.Errorf("failed to parse annotation to track running commands on pods due to: %v", err)
					lastError = err
					continue
				}

				if len(taskTracker.pods) == 0 {
					continue
				}

				for _, pod := range taskTracker.pods {
					p, err := k8s.Instance().GetPodByUID(types.UID(pod.uid), pod.namespace)
					if err != nil {
						if err == k8s.ErrPodsNotFound {
							continue
						}

						log.SnapshotLog(&snap).Warnf("Failed to get pod with uid: %s due to: %v", pod.uid, err)
						continue // best effort
					}

					backgroundPodList = append(backgroundPodList, *p)
				}

				err = terminateCommandInPods(&snap, backgroundPodList, taskTracker.taskID)
				if err != nil {
					err = fmt.Errorf("failed to terminate running commands in pods due to: %v", err)
					lastError = err
					continue
				}
			}
		}
	}

	return lastError
}

// executeSnapRule executes rules in the given snap. pvcs are used to figure out the pods on which the rule actions will be
// run on
func executeSnapRule(pvcs []v1.PersistentVolumeClaim, snap *crdv1.VolumeSnapshot, rType ruleType) (chan bool, error) {
	// Validate the snap rule. Don't depend on callers to invoke this
	if err := ValidateSnapRule(snap); err != nil {
		return nil, err
	}

	if snap.Metadata.Annotations != nil {
		ruleName, present := snap.Metadata.Annotations[ruleAnnotationKeys[rType]]
		if present && len(ruleName) > 0 {
			log.SnapshotLog(snap).Infof("Running snap rule: %s", ruleName)
			taskID, err := uuid.New()
			if err != nil {
				err = fmt.Errorf("failed to generate uuid for snapshot rule tasks due to: %v", err)
				return nil, err
			}

			pods := make([]v1.Pod, 0)
			for _, pvc := range pvcs {
				pvcPods, err := k8s.Instance().GetPodsUsingPVC(pvc.GetName(), pvc.GetNamespace())
				if err != nil {
					return nil, err
				}

				pods = append(pods, pvcPods...)
			}

			if len(pods) > 0 {
				// Get the rule and based on the rule execute it
				rule, err := k8s.Instance().GetRule(ruleName, snap.Metadata.Namespace)
				if err != nil {
					return nil, err
				}

				// start a watcher thread that will accumulate pods which have background commands to
				// terminate and also watch a signal channel that indicates when to terminate them
				backgroundCommandTermChan := make(chan bool, 1)
				backgroundPodListChan := make(chan v1.Pod)
				go cmdTerminationWatcher(backgroundPodListChan, backgroundCommandTermChan, snap, taskID.String())

				// backgroundActionPresent is used to track if there is atleast one background action
				backgroundActionPresent := false
				for _, item := range rule.Spec {
					filteredPods := make([]v1.Pod, 0)
					// filter pods and only uses the ones that match this selector
					for _, pod := range pods {
						if hasSubset(pod.GetObjectMeta().GetLabels(), item.PodSelector) {
							filteredPods = append(filteredPods, pod)
						}
					}

					if len(filteredPods) == 0 {
						log.SnapshotLog(snap).Warnf("None of the pods matched selectors for rule spec: %v", rule.Spec)
						continue
					}

					for _, action := range item.Actions {
						if action.Background {
							backgroundActionPresent = true
						}

						if action.Type == apis_stork.RuleActionCommand {
							err := executeCommandAction(filteredPods, rule, snap, action, backgroundPodListChan, rType, taskID)
							if err != nil {
								// if any action fails, terminate all background jobs and don't depend on caller
								// to clean them up
								if backgroundActionPresent {
									backgroundCommandTermChan <- true
									return nil, err
								}

								backgroundCommandTermChan <- false
								return nil, err
							}
						}
					}
				}

				if backgroundActionPresent {
					return backgroundCommandTermChan, nil
				}

				backgroundCommandTermChan <- false
				return nil, nil
			}
		}
	}

	return nil, nil
}

// executeCommandAction executes the command type action on given pods:
func executeCommandAction(
	pods []v1.Pod,
	rule *apis_stork.Rule,
	snap *crdv1.VolumeSnapshot,
	action apis_stork.RuleAction,
	backgroundPodNotifyChan chan v1.Pod,
	rType ruleType, taskID *uuid.UUID) error {
	if len(pods) == 0 {
		return nil
	}

	podsForAction := make([]v1.Pod, 0)
	if action.RunInSinglePod {
		podsForAction = []v1.Pod{pods[0]}
	} else {
		podsForAction = append(podsForAction, pods...)
	}

	cmdExecutorImage := defaultCmdExecutorImage
	ruleAnnotations := rule.GetAnnotations()
	if ruleAnnotations != nil {
		if imageOverride, ok := ruleAnnotations[cmdExecutorImageOverrideKey]; ok && len(imageOverride) > 0 {
			cmdExecutorImage = imageOverride
		}
	}

	if action.Background {
		for _, podToTerminate := range podsForAction {
			backgroundPodNotifyChan <- podToTerminate
		}

		// regardless of the outcome of running the background command, we first update the
		// snapshot to track pods which might have a running background command
		updateErr := updateRunningCommandPodListInSnap(snap, podsForAction, taskID.String())
		if updateErr != nil {
			log.SnapshotLog(snap).Warnf("Failed to update list of pods with running command in snap due to: %v", updateErr)
		}

		err := runBackgroundCommandOnPods(podsForAction, action.Value, taskID.String(), cmdExecutorImage)
		if err != nil {
			return err
		}
	} else {
		_, err := runCommandOnPods(podsForAction, action.Value, execPodStepLow, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// podsToString is a helper function to create a user-friendly single string from a list of pods
func podsToString(pods []v1.Pod) string {
	var podList []string
	for _, p := range pods {
		podList = append(podList, fmt.Sprintf("%s/%s", p.GetNamespace(), p.GetName()))
	}

	return strings.Join(podList, ",")
}

// updateRunningCommandPodListInSnap updates the snapshot annotation to track pods which might have a
// running command. This allows recovery if we crash while running the commands. One can parse these annotations
// to terminate the running commands
func updateRunningCommandPodListInSnap(snap *crdv1.VolumeSnapshot, pods []v1.Pod, taskID string) error {
	podsWithNs := make([]pod, 0)
	for _, p := range pods {
		podsWithNs = append(podsWithNs, pod{
			namespace: p.GetNamespace(),
			uid:       string(p.GetUID())})
	}

	tracker := &commandTask{
		taskID: taskID,
		pods:   podsWithNs,
	}

	trackerBytes, err := json.Marshal(tracker)
	if err != nil {
		return fmt.Errorf("failed to update running command pod list in snap due to: %v", err)
	}

	err = wait.ExponentialBackoff(snapAPICallBackoff, func() (bool, error) {
		snap, err := k8s.Instance().GetSnapshot(snap.Metadata.Name, snap.Metadata.Namespace)
		if err != nil {
			logrus.Warnf("Failed to get latest snapshot object due to: %v. Will retry.", err)
			return false, nil
		}

		snapCopy := snap.DeepCopy()
		if len(podsWithNs) == 0 {
			delete(snapCopy.Metadata.Annotations, podsWithRunningCommandsKey)
		} else {
			snapCopy.Metadata.Annotations[podsWithRunningCommandsKey] = string(trackerBytes)
		}

		if _, err := k8s.Instance().UpdateSnapshot(snapCopy); err != nil {
			log.SnapshotLog(snap).Warnf("Failed to update snapshot due to: %v. Will retry.", err)
			return false, nil
		}

		return true, nil
	})
	return err
}

// runCommandOnPods runs cmd on given pods. If failFast is true, it will return on the first failure. It will
// return a list of pods that failed.
func runCommandOnPods(pods []v1.Pod, cmd string, numRetries int, failFast bool) ([]v1.Pod, error) {
	var wg sync.WaitGroup
	backOff := wait.Backoff{
		Duration: execPodCmdRetryInterval,
		Factor:   execPodCmdRetryFactor,
		Steps:    numRetries,
	}
	errChannel := make(chan podErrorResponse)
	finished := make(chan bool, 1)

	for _, pod := range pods {
		wg.Add(1)
		go func(pod v1.Pod, errRespChan chan podErrorResponse) {
			defer wg.Done()
			err := wait.ExponentialBackoff(backOff, func() (bool, error) {
				ns, name := pod.GetNamespace(), pod.GetName()
				pod, err := k8s.Instance().GetPodByUID(pod.GetUID(), ns)
				if err != nil {
					if err == k8s.ErrPodsNotFound {
						logrus.Infof("Pod with uuid: %s in namespace: %s is no longer present", string(pod.GetUID()), ns)
						return true, nil
					}

					logrus.Warnf("Failed to get pod: [%s] %s due to: %v", ns, string(pod.GetUID()), err)
					return false, nil
				}

				_, err = k8s.Instance().RunCommandInPod([]string{"sh", "-c", cmd}, name, "", ns)
				if err != nil {
					logrus.Warnf("Failed to run command: %s on pod: [%s] %s due to: %v", cmd, ns, name, err)
					return false, nil
				}

				logrus.Infof("Command: %s succeeded on pod: [%s] %s", cmd, ns, name)
				return true, nil
			})
			if err != nil {
				errChannel <- podErrorResponse{
					pod: pod,
					err: err,
				}
			}
		}(pod, errChannel)
	}

	// Put the wait group in a go routine.
	// By putting the wait group in the go routine we ensure either all pass
	// and we close the "finished" channel or we wait forever for the wait group
	// to finish.
	//
	// Waiting forever is okay because of the blocking select below.
	go func() {
		wg.Wait()
		close(finished)
	}()

	failed := make([]v1.Pod, 0)
	select {
	case <-finished:
		if len(failed) > 0 {
			err := fmt.Errorf("command: %s failed on pods: %s", cmd, podsToString(failed))
			return failed, err
		}

		logrus.Infof("Command: %s finished successfully on all pods", cmd)
		return nil, nil
	case errResp := <-errChannel:
		failed = append(failed, errResp.pod) // TODO also accumulate atleast the last error
		if failFast {
			return failed, fmt.Errorf("command: %s failed in pod: [%s] %s due to: %s",
				cmd, errResp.pod.GetNamespace(), errResp.pod.GetName(), errResp.err)
		}
	}

	return nil, nil
}

// runBackgroundCommandOnPods will start the given "cmd" on all the given "pods". The taskID is given to
// the executor pod so it can have unique status files in the target pods where it runs the actual commands
func runBackgroundCommandOnPods(pods []v1.Pod, cmd, taskID, cmdExecutorImage string) error {
	executorArgs := []string{
		"/cmdexecutor",
		"-timeout", strconv.FormatInt(perPodCommandExecTimeout, 10),
		"-cmd", cmd,
		"-taskid", taskID,
	}

	for _, pod := range pods {
		executorArgs = append(executorArgs, []string{"-pod", fmt.Sprintf("%s/%s", pod.GetNamespace(), pod.GetName())}...)
	}

	// start async cmd executor pod
	labels := map[string]string{
		"app": "cmdexecutor",
	}
	executorPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("pod-cmd-executor-%s", taskID),
			Namespace: core.NamespaceSystem,
			Labels:    labels,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				v1.Container{
					Name:            "cmdexecutor",
					Image:           cmdExecutorImage,
					ImagePullPolicy: v1.PullAlways,
					Args:            executorArgs,
					// Below ReadinessProbe checks if the command is ready after finishing all it's tasks. The
					// cmdExecutorImage will touch this file once it's done and hence having the below probe will
					// allow the status to get reflected in the pod readiness probe
					ReadinessProbe: &v1.Probe{
						Handler: v1.Handler{
							Exec: &v1.ExecAction{
								Command: []string{"cat", "/tmp/cmdexecutor-status"},
							},
						},
					},
				},
			},
			RestartPolicy:      v1.RestartPolicyNever,
			ServiceAccountName: storkServiceAccount,
		},
	}

	createdPod, err := k8s.Instance().CreatePod(executorPod)
	if err != nil {
		return err
	}

	defer func() {
		if createdPod != nil {
			err := k8s.Instance().DeletePods([]v1.Pod{*createdPod}, false)
			if err != nil {
				logrus.Warnf("Failed to delete command executor pod: [%s] %s due to: %v",
					createdPod.GetNamespace(), createdPod.GetName(), err)
			}
		}
	}()

	logrus.Infof("Created pod command executor: [%s] %s", createdPod.GetNamespace(), createdPod.GetName())
	err = waitForExecPodCompletion(createdPod)
	if err != nil {
		// Since the command executor failed, fetch it's status using the pod's name as the key. The fetched status
		// will have more details on why it failed (for e.g what commands failed to run and why)
		status, statusFetchErr := status.Get(createdPod.GetName())
		if statusFetchErr != nil {
			logrus.Warnf("Failed to fetch status of command executor due to: %v", statusFetchErr)
			return err
		}

		err = fmt.Errorf("%s. cmd executor failed because: %s", err.Error(), status)
		return err
	}

	return nil
}

// waitForExecPodCompletion waits until the pod has completed (success or failure)
func waitForExecPodCompletion(pod *v1.Pod) error {
	logrus.Infof("Waiting for pod: [%s] %s readiness with backoff: %v", pod.GetNamespace(), pod.GetName(), execCmdBackoff)
	return wait.ExponentialBackoff(execCmdBackoff, func() (bool, error) {
		p, err := k8s.Instance().GetPodByUID(pod.GetUID(), pod.GetNamespace())
		if err != nil {
			return false, nil
		}

		if p.Status.Phase == v1.PodFailed {
			errMsg := fmt.Sprintf("Pod: [%s] %s failed", p.GetNamespace(), p.GetName())
			logrus.Errorf(errMsg)
			return true, fmt.Errorf(errMsg)
		}

		if p.Status.Phase == v1.PodSucceeded {
			logrus.Infof("Pod: [%s] %s succeeded", pod.GetNamespace(), pod.GetName())
			return true, nil
		}

		return false, nil
	})
}

// cmdTerminationWatcher accumulates pods supplied to the given podListChan and when
// the terminationSignalChan is sent a true signal, it terminates commands on the accumulated
// pods
func cmdTerminationWatcher(
	podListChan chan v1.Pod,
	terminationSignalChan chan bool,
	snap *crdv1.VolumeSnapshot,
	id string) {
	// For tracking, use a map/set keyed by uid to handle duplicates
	podsToTerminate := make(map[string]v1.Pod)
	for {
		select {
		case pod := <-podListChan:
			podsToTerminate[string(pod.GetUID())] = pod
		case terminate := <-terminationSignalChan:
			if terminate {
				podList := make([]v1.Pod, 0)
				for _, pod := range podsToTerminate {
					podList = append(podList, pod)
				}

				if err := terminateCommandInPods(snap, podList, id); err != nil {
					log.SnapshotLog(snap).Warnf("failed to terminate background command in pods due to: %v", err)
				}
			}
			break
		}
	}
}

func hasSubset(set map[string]string, subset map[string]string) bool {
	for k := range subset {
		v, ok := set[k]
		if !ok || v != subset[k] {
			return false
		}
	}
	return true
}
