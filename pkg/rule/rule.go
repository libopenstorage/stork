package rule

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/cmdexecutor"
	"github.com/libopenstorage/stork/pkg/cmdexecutor/status"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/dynamic"
	errors "github.com/portworx/sched-ops/k8s/errors"
	"github.com/sirupsen/logrus"
	"github.com/skyrings/skyring-common/tools/uuid"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	coreapi "k8s.io/kubernetes/pkg/apis/core"
)

const (
	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute
	// environment variable for command executor image registry and image registry secret.
	cmdExecutorImageRegistryEnvVar       = "CMD-EXECUTOR-IMAGE-REGISTRY"
	cmdExecutorImageRegistrySecretEnvVar = "CMD-EXECUTOR-IMAGE-REGISTRY-SECRET"
	defaultCmdExecutorImage              = "openstorage/cmdexecutor:0.1"
	// annotation key value for command executor image registry and image registry secret.
	cmdExecutorImageOverrideKey          = "stork.libopenstorage.org/cmdexecutor-image"
	cmdExecutorImageOverrideSecretKey    = "stork.libopenstorage.org/cmdexecutor-image-secret"
	storkServiceAccount                  = "stork-account"
	podsWithRunningCommandsKeyDeprecated = "stork/pods-with-running-cmds"
	podsWithRunningCommandsKey           = "stork.libopenstorage.org/pods-with-running-cmds"

	// constants
	perPodCommandExecTimeout = 900 // 15 minutes

	execPodCmdRetryInterval = 5 * time.Second
	execPodCmdRetryFactor   = 1
	execPodStepLow          = 12
	execPodStepMed          = 36
	execPodStepsHigh        = math.MaxInt32
	maxRetry                = 10
	retrySleep              = 30 * time.Second
)

// Type The type of rule to be executed
type Type string

const (
	// PreExecRule This type of rule is to be run before an operation
	PreExecRule Type = "preExecRule"
	// PostExecRule This type of rule is to be run after an operation
	PostExecRule Type = "postExecRule"
)

// Pod is a simple type to encapsulate a Pod's uid and namespace
type Pod struct {
	UID       string `json:"uid"`
	Namespace string `json:"namespace"`
}

type podErrorResponse struct {
	Pod v1.Pod
	err error
}

// commandTask tracks pods where commands for a taskID might still be running
type commandTask struct {
	TaskID    string `json:"taskID"`
	Pods      []Pod  `json:"pods"`
	Container string `json:"container"`
}

var execCmdBackoff = wait.Backoff{
	Duration: execPodCmdRetryInterval,
	Factor:   execPodCmdRetryFactor,
	Steps:    execPodStepsHigh,
}

var ownerAPICallBackoff = wait.Backoff{
	Duration: 2 * time.Second,
	Factor:   1.5,
	Steps:    20,
}

// Init initializes the rule executor
func Init() error {
	storkRuleResource := apiextensions.CustomResource{
		Name:    "rule",
		Plural:  "rules",
		Group:   stork_api.SchemeGroupVersion.Group,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.Rule{}).Name(),
	}

	ok, err := version.RequiresV1Registration()
	if err != nil {
		return err
	}
	if ok {
		err := k8sutils.CreateCRD(storkRuleResource)
		if err != nil && !k8s_errors.IsAlreadyExists(err) {
			return err
		}
		return apiextensions.Instance().ValidateCRD(storkRuleResource.Plural+"."+storkRuleResource.Group, validateCRDTimeout, validateCRDInterval)
	}
	err = apiextensions.Instance().CreateCRDV1beta1(storkRuleResource)
	if err != nil && !k8s_errors.IsAlreadyExists(err) {
		return err
	}
	return apiextensions.Instance().ValidateCRDV1beta1(storkRuleResource, validateCRDTimeout, validateCRDInterval)
}

// ValidateRule validates a rule
func ValidateRule(rule *stork_api.Rule, ruleType Type) error {
	for _, item := range rule.Rules {
		for _, action := range item.Actions {
			if action.Type == stork_api.RuleActionCommand {
				if action.Background && ruleType == PostExecRule {
					return fmt.Errorf("background actions are not supported for post exec rules")
				}
			} else {
				return fmt.Errorf("unsupported action type: %s in rule: [%s] %s",
					action.Type, rule.GetNamespace(), rule.GetName())
			}
		}
	}

	return nil
}

// terminateCommandInPods terminates a previously running background command on given pods for given task ID
func terminateCommandInPods(owner runtime.Object, pods []v1.Pod, container, taskID string) error {
	killFile := fmt.Sprintf(cmdexecutor.KillFileFormat, taskID)
	failedPods, err := runCommandOnPods(pods, container, fmt.Sprintf("touch %s", killFile), execPodStepsHigh, false)

	updateErr := updateRunningCommandPodListInOwner(owner, failedPods, container, taskID)
	if updateErr != nil {
		log.RuleLog(nil, owner).Warnf("Failed to update list of pods with running command in owner due to: %v", updateErr)
	}

	return err
}

// PerformRuleRecovery terminates potential background commands running pods for
// the given owner
func PerformRuleRecovery(
	owner runtime.Object,
) error {
	taskTracker, err := getPodsTrackerForOwner(owner)
	if err != nil {
		return err
	}

	if taskTracker != nil && len(taskTracker.Pods) > 0 {
		backgroundPodList := make([]v1.Pod, 0)
		log.RuleLog(nil, owner).Infof("Performing recovery to terminate commands tracker: %v", taskTracker)
		for _, pod := range taskTracker.Pods {
			p, err := core.Instance().GetPodByUID(types.UID(pod.UID), pod.Namespace)
			if err != nil {
				if err == errors.ErrPodsNotFound {
					continue
				}

				metadata, err := meta.Accessor(owner)
				if err != nil {
					log.RuleLog(nil, owner).Warnf(err.Error())
					continue
				}

				err = fmt.Errorf("failed to get pod with uid: %s due to: %v", pod.UID, err)
				log.RuleLog(nil, owner).Warnf(err.Error())

				ev := &v1.Event{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("pod-not-found-%s", string(pod.UID)),
						Namespace: metadata.GetNamespace(),
					},
					InvolvedObject: v1.ObjectReference{
						Name:       metadata.GetName(),
						Namespace:  metadata.GetNamespace(),
						UID:        metadata.GetUID(),
						Kind:       owner.GetObjectKind().GroupVersionKind().Kind,
						APIVersion: owner.GetObjectKind().GroupVersionKind().Version,
					},
					Reason:  "FailedToGetPod",
					Message: err.Error(),
					Source: v1.EventSource{
						Component: "stork",
					},
				}
				if _, err = core.Instance().CreateEvent(ev); err != nil {
					log.RuleLog(nil, owner).Warnf("failed to create event for missing pod err: %v", err)
				}

				continue // best effort
			}

			backgroundPodList = append(backgroundPodList, *p)
		}

		err = terminateCommandInPods(owner, backgroundPodList, taskTracker.Container, taskTracker.TaskID)
		if err != nil {
			return fmt.Errorf("failed to terminate running commands in pods due to: %v", err)
		}
	}

	return nil
}

// ExecuteRule executes rules for the given owner. PVCs are used to figure out the pods on which the rule actions will be
// run on
func ExecuteRule(
	rule *stork_api.Rule,
	rType Type,
	owner runtime.Object,
	podNamespace string,
) (chan bool, error) {
	// Validate the rule. Don't depend on callers to invoke this
	if err := ValidateRule(rule, rType); err != nil {
		return nil, err
	}

	log.RuleLog(rule, owner).Infof("Running %v", rType)
	taskID, err := uuid.New()
	if err != nil {
		err = fmt.Errorf("failed to generate uuid for rule tasks due to: %v", err)
		return nil, err
	}

	pods := make([]v1.Pod, 0)
	for _, item := range rule.Rules {
		p, err := core.Instance().GetPods(podNamespace, item.PodSelector)
		if err != nil {
			return nil, err
		}

		pods = append(pods, p.Items...)
	}

	if len(pods) > 0 {
		// start a watcher thread that will accumulate pods which have background commands to
		// terminate and also watch a signal channel that indicates when to terminate them
		backgroundCommandTermChan := make(chan bool, 1)
		backgroundPodListChan := make(chan v1.Pod)
		var container string
		go cmdTerminationWatcher(backgroundPodListChan, &container, backgroundCommandTermChan, owner, taskID.String())

		// backgroundActionPresent is used to track if there is atleast one background action
		backgroundActionPresent := false
		for _, item := range rule.Rules {
			container = item.Container
			filteredPods := make([]v1.Pod, 0)
			// filter pods and only uses the ones that match this selector
			for _, pod := range pods {
				if hasSubset(pod.GetObjectMeta().GetLabels(), item.PodSelector) {
					filteredPods = append(filteredPods, pod)
				}
			}

			if len(filteredPods) == 0 {
				log.RuleLog(rule, owner).Warnf("None of the pods matched selectors for rule spec: %v", item)
				continue
			}

			for _, action := range item.Actions {
				if action.Background {
					backgroundActionPresent = true
				}

				if action.Type == stork_api.RuleActionCommand {
					err := executeCommandAction(filteredPods, item.Container, rule, owner, action, backgroundPodListChan, rType, taskID)
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

	return nil, nil
}

// executeCommandAction executes the command type action on given pods:
func executeCommandAction(
	pods []v1.Pod,
	container string,
	rule *stork_api.Rule,
	owner runtime.Object,
	action stork_api.RuleAction,
	backgroundPodNotifyChan chan v1.Pod,
	rType Type, taskID *uuid.UUID) error {
	if len(pods) == 0 {
		return nil
	}

	podsForAction := make([]v1.Pod, 0)
	if action.RunInSinglePod {
		podsForAction = []v1.Pod{pods[0]}
	} else {
		podsForAction = append(podsForAction, pods...)
	}
	var cmdExecutorImage string
	var cmdExecutorImageSecret string
	// The order of priority for image location value is Job annotation, environmental variable
	// and then if both of them are missing, default to stork image repo
	if len(os.Getenv(cmdExecutorImageRegistryEnvVar)) == 0 {
		// If env is not set get the values from stork deployment spec.
		registry, registrySecret, err := k8sutils.GetImageRegistryFromDeployment(
			k8sutils.StorkDeploymentName,
			k8sutils.DefaultAdminNamespace,
		)
		if err != nil {
			return err
		}
		if len(registry) != 0 {
			cmdExecutorImage = registry + "/" + defaultCmdExecutorImage
		} else {
			cmdExecutorImage = defaultCmdExecutorImage
		}
		cmdExecutorImageSecret = registrySecret
	} else {
		// if env is set get it from env variable.
		cmdExecutorImage = os.Getenv(cmdExecutorImageRegistryEnvVar) + "/" + defaultCmdExecutorImage
		cmdExecutorImageSecret = os.Getenv(cmdExecutorImageRegistrySecretEnvVar)
	}
	ruleAnnotations := rule.GetAnnotations()
	if ruleAnnotations != nil {
		// get the image registry name, if the annotation is present.
		if imageOverride, ok := ruleAnnotations[cmdExecutorImageOverrideKey]; ok && len(imageOverride) > 0 {
			cmdExecutorImage = imageOverride
		}
		// get the image registry secret, if the annotation is present.
		if imageOverrideSecret, ok := ruleAnnotations[cmdExecutorImageOverrideSecretKey]; ok && len(imageOverrideSecret) > 0 {
			cmdExecutorImageSecret = imageOverrideSecret
		}
	}

	if action.Background {
		for _, podToTerminate := range podsForAction {
			backgroundPodNotifyChan <- podToTerminate
		}

		// regardless of the outcome of running the background command, we first update the
		// owner to track pods which might have a running background command
		podsForTracker := make(map[string]v1.Pod)
		for _, pod := range podsForAction {
			podsForTracker[string(pod.UID)] = pod
		}

		// Get pods already existing in tracker so we don't lose them
		existingTracker, err := getPodsTrackerForOwner(owner)
		if err != nil {
			return err
		}

		if existingTracker != nil && len(existingTracker.Pods) > 0 {
			for _, existingPod := range existingTracker.Pods {
				// Check if pod exists in cluster
				existingPodObject, err := core.Instance().GetPodByUID(types.UID(existingPod.UID), existingPod.Namespace)
				if err != nil {
					if err == errors.ErrPodsNotFound {
						continue
					}

					return err
				}

				podsForTracker[existingPod.UID] = *existingPodObject
			}
		}

		podsForTrackerList := make([]v1.Pod, 0)
		for _, pod := range podsForTracker {
			podsForTrackerList = append(podsForTrackerList, pod)
		}

		updateErr := updateRunningCommandPodListInOwner(owner, podsForTrackerList, container, taskID.String())
		if updateErr != nil {
			log.RuleLog(rule, owner).Warnf("Failed to update list of pods with running command in owner due to: %v", updateErr)
		}

		err = runBackgroundCommandOnPods(podsForAction, container, action.Value, taskID.String(), cmdExecutorImage, cmdExecutorImageSecret)
		if err != nil {
			return err
		}
	} else {
		_, err := runCommandOnPods(podsForAction, container, action.Value, execPodStepLow, true)
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

// updateRunningCommandPodListInOwner updates the owner annotation to track pods which might have a
// running command. This allows recovery if we crash while running the commands. One can parse these annotations
// to terminate the running commands
func updateRunningCommandPodListInOwner(
	owner runtime.Object,
	pods []v1.Pod,
	container string,
	taskID string,
) error {
	podsWithNs := make([]Pod, 0)
	for _, p := range pods {
		podsWithNs = append(podsWithNs, Pod{
			Namespace: p.GetNamespace(),
			UID:       string(p.GetUID())})
	}

	tracker := &commandTask{
		TaskID:    taskID,
		Pods:      podsWithNs,
		Container: container,
	}

	trackerBytes, err := json.Marshal(tracker)
	if err != nil {
		return fmt.Errorf("failed to update running command pod list in owner due to: %v", err)
	}

	err = wait.ExponentialBackoff(ownerAPICallBackoff, func() (bool, error) {
		ownerCopy, err := dynamic.Instance().GetObject(owner)
		if err != nil {
			log.RuleLog(nil, owner).Warnf("Failed to get latest owner due to: %v. Will retry.", err)
			return false, nil
		}

		metadata, err := meta.Accessor(ownerCopy)
		if err != nil {
			return false, err
		}

		annotations := metadata.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string)
		}

		if len(podsWithNs) == 0 {
			delete(annotations, podsWithRunningCommandsKey)
		} else {
			annotations[podsWithRunningCommandsKey] = string(trackerBytes)
		}

		metadata.SetAnnotations(annotations)
		if _, err := dynamic.Instance().UpdateObject(ownerCopy); err != nil {
			log.RuleLog(nil, owner).Warnf("Failed to update owner due to: %v. Will retry.", err)
			return false, nil
		}

		return true, nil
	})
	return err
}

// runCommandOnPods runs cmd on given pods. If failFast is true, it will return on the first failure. It will
// return a list of pods that failed.
func runCommandOnPods(pods []v1.Pod, container string, cmd string, numRetries int, failFast bool) ([]v1.Pod, error) {
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
				_, err := core.Instance().GetPodByUID(pod.GetUID(), ns)
				if err != nil {
					if err == errors.ErrPodsNotFound {
						logrus.Infof("Pod with uuid: %s in namespace: %s is no longer present", string(pod.GetUID()), ns)
						return true, nil
					}

					err = fmt.Errorf("failed to get pod: [%s] %s due to: %v", ns, string(pod.GetUID()), err)

					ev := &v1.Event{
						ObjectMeta: metav1.ObjectMeta{
							Name:      fmt.Sprintf("pod-not-found-%s", string(pod.GetUID())),
							Namespace: pod.GetNamespace(),
						},
						InvolvedObject: v1.ObjectReference{
							Name:       pod.GetName(),
							Namespace:  pod.GetNamespace(),
							UID:        pod.GetUID(),
							Kind:       pod.GetObjectKind().GroupVersionKind().Kind,
							APIVersion: pod.GetObjectKind().GroupVersionKind().Version,
						},
						Reason:  "FailedToGetPod",
						Message: err.Error(),
						Source: v1.EventSource{
							Component: "stork",
						},
					}
					if _, err = core.Instance().CreateEvent(ev); err != nil {
						logrus.Warnf("failed to create event for missing pod err: %v", err)
					}

					return false, nil
				}

				_, err = core.Instance().RunCommandInPod([]string{"sh", "-c", cmd}, name, container, ns)
				if err != nil {
					logrus.Warnf("Failed to run command: %s on pod: [%s] %s due to: %v", cmd, ns, name, err)
					return false, nil
				}

				logrus.Infof("Command: %s succeeded on pod: [%s] %s", cmd, ns, name)
				return true, nil
			})
			if err != nil {
				errChannel <- podErrorResponse{
					Pod: pod,
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
		failed = append(failed, errResp.Pod) // TODO also accumulate atleast the last error
		if failFast {
			return failed, fmt.Errorf("command: %s failed in pod: [%s] %s due to: %s",
				cmd, errResp.Pod.GetNamespace(), errResp.Pod.GetName(), errResp.err)
		}
	}

	return nil, nil
}

// ToImagePullSecret converts a secret name to the ImagePullSecret struct.
func ToImagePullSecret(name string) []v1.LocalObjectReference {
	if name == "" {
		return nil
	}
	return []v1.LocalObjectReference{
		{
			Name: name,
		},
	}

}

// runBackgroundCommandOnPods will start the given "cmd" on all the given "pods". The taskID is given to
// the executor pod so it can have unique status files in the target pods where it runs the actual commands
func runBackgroundCommandOnPods(pods []v1.Pod, container, cmd, taskID, cmdExecutorImage, cmdExecutorImageSecret string) error {
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
			Namespace: coreapi.NamespaceSystem,
			Labels:    labels,
		},
		Spec: v1.PodSpec{
			ImagePullSecrets: ToImagePullSecret(cmdExecutorImageSecret),
			Containers: []v1.Container{
				{
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

	createdPod, err := core.Instance().CreatePod(executorPod)
	if err != nil {
		return err
	}

	defer func() {
		if createdPod != nil {
			err := core.Instance().DeletePods([]v1.Pod{*createdPod}, false)
			if err != nil {
				logrus.Warnf("Failed to delete command executor pod: [%s] %s due to: %v",
					createdPod.GetNamespace(), createdPod.GetName(), err)
			}
		}
	}()

	logrus.Infof("Created pod command executor: [%s] %s", createdPod.GetNamespace(), createdPod.GetName())
	// Check whether the cmd executor pod is struck in pending state for more than five mintues
	// If struck, delete the pod and return error.
	for i := 0; i < maxRetry; i++ {
		p, err := core.Instance().GetPodByUID(createdPod.GetUID(), createdPod.GetNamespace())
		if err != nil {
			return err
		}
		if p.Status.Phase == v1.PodPending {
			if i == (maxRetry - 1) {
				var err error
				if len(p.Status.ContainerStatuses) != 0 {
					err = fmt.Errorf("rule command executor is struck in pending state, reason: %v", p.Status.ContainerStatuses[0].State.Waiting.Reason)
				} else {
					err = fmt.Errorf("rule command executor is struck in pending state")
				}
				logrus.Errorf("%v", err)
				return err
			}
			time.Sleep(retrySleep)
			continue
		}
	}

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
		p, err := core.Instance().GetPodByUID(pod.GetUID(), pod.GetNamespace())
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
	container *string,
	terminationSignalChan chan bool,
	owner runtime.Object,
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

				if err := terminateCommandInPods(owner, podList, *container, id); err != nil {
					log.RuleLog(nil, owner).Warnf("failed to terminate background command in pods due to: %v", err)
				}
			}
			return
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

func getPodsTrackerForOwner(owner runtime.Object) (*commandTask, error) {
	metadata, err := meta.Accessor(owner)
	if err != nil {
		return nil, err
	}
	var taskTracker commandTask

	annotations := metadata.GetAnnotations()
	if annotations != nil {
		value := annotations[podsWithRunningCommandsKey]
		if len(value) > 0 {
			err := json.Unmarshal([]byte(value), &taskTracker)
			if err != nil {
				return nil, fmt.Errorf("failed to parse annotation to track running commands on pods due to: %v", err)
			}

			return &taskTracker, nil
		}
	}

	return nil, nil
}
