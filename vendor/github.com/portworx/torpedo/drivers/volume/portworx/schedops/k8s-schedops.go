package schedops

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/autopilot"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	k8serrors "github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/k8s/rbac"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	k8sdriver "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
)

const (
	// PXServiceName is the name of the portworx service in kubernetes
	PXServiceName = "portworx-service"
	// PXNamespace is the kubernetes namespace in which portworx daemon set runs
	PXNamespace = "kube-system"
	// PXDaemonSet is the name of portworx daemon set in k8s deployment
	PXDaemonSet = "portworx"
	// PXServiceLabelKey is the label key used for px systemd service control
	PXServiceLabelKey = "px/service"
	// k8sServiceOperationStart is label value for starting Portworx service
	k8sServiceOperationStart = "start"
	// k8sServiceOperationStop is label value for stopping Portworx service
	k8sServiceOperationStop = "stop"
	// k8sServiceOperationRestart is label value for restarting Portworx service
	k8sServiceOperationRestart = "restart"
	// k8sPodsRootDir is the directory under which k8s keeps all pods data
	k8sPodsRootDir = "/var/lib/kubelet/pods"
	// snapshotAnnotation is the annotation used to get the parent of a PVC
	snapshotAnnotation = "px/snapshot-source-pvc"
	// storkSnapshotAnnotation is the annotation used get the snapshot of Stork created clone
	storkSnapshotAnnotation = "snapshot.alpha.kubernetes.io/snapshot"
	// storkSnapshotNameKey is the key name of the label on a portworx volume snapshot that identifies
	//   the name of the stork volume snapshot
	storkSnapshotNameKey = "stork-snap"
	// pvcLabel is the label used on volume to identify the pvc name
	pvcLabel = "pvc"
	// PXEnabledLabelKey is the label used to check whether px installation is enabled/disabled on node
	PXEnabledLabelKey = "px/enabled"
	// k8sRoleNodeInfraLabelKey is the label used to check whether node has infra=true label on OpenShift Enterprise environment
	k8sRoleNodeInfraLabelKey = "node-role.kubernetes.io/infra"
	// k8sRoleNodeComputeLabelKey is the label used to check whether node has compute=true label on OpenShift Enterprise environment
	k8sRoleNodeComputeLabelKey = "node-role.kubernetes.io/compute"

	// nodeType is label used to check kubernetes node-type
	dcosNodeType                = "kubernetes.dcos.io/node-type"
	talismanServiceAccount      = "talisman-account"
	talismanImage               = "portworx/talisman:latest"
	rancherControlPlaneLabelKey = "node-role.kubernetes.io/controlplane"
)

const (
	defaultRetryInterval = 5 * time.Second
	defaultTimeout       = 10 * time.Minute
)

var (
	pxDisabledConditions = map[string]string{
		PXEnabledLabelKey:           "false",
		rancherControlPlaneLabelKey: "true",
	}
)

var (
	k8sCore      = core.Instance()
	k8sBatch     = batch.Instance()
	k8sRbac      = rbac.Instance()
	k8sAutopilot = autopilot.Instance()
	k8sApps      = apps.Instance()
)

// errLabelPresent error type for a label being present on a node
type errLabelPresent struct {
	// label is the label key
	label string
	// node is the k8s node where the label is present
	node string
}

func (e *errLabelPresent) Error() string {
	return fmt.Sprintf("label %s is present on node %s", e.label, e.node)
}

// errLabelAbsent error type for a label absent on a node
type errLabelAbsent struct {
	// label is the label key
	label string
	// node is the k8s node where the label is absent
	node string
}

func (e *errLabelAbsent) Error() string {
	return fmt.Sprintf("label %s is absent on node %s", e.label, e.node)
}

type k8sSchedOps struct{}

func (k *k8sSchedOps) GetKubernetesVersion() (*version.Info, error) {
	return k8sCore.GetVersion()
}

func (k *k8sSchedOps) StopPxOnNode(n node.Node) error {
	return k8sCore.AddLabelOnNode(n.Name, PXServiceLabelKey, k8sServiceOperationStop)
}

func (k *k8sSchedOps) StartPxOnNode(n node.Node) error {
	return k8sCore.AddLabelOnNode(n.Name, PXServiceLabelKey, k8sServiceOperationStart)
}

func (k *k8sSchedOps) RestartPxOnNode(n node.Node) error {
	return k8sCore.AddLabelOnNode(n.Name, PXServiceLabelKey, k8sServiceOperationRestart)
}

func (k *k8sSchedOps) ValidateOnNode(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateOnNode",
	}
}

func (k *k8sSchedOps) ValidateAddLabels(replicaNodes []api.StorageNode, vol *api.Volume) error {
	pvc, ok := vol.Locator.VolumeLabels[pvcLabel]
	if !ok {
		return nil
	}

	var missingLabelNodes []string
	for _, rs := range replicaNodes {
		t := func() (interface{}, bool, error) {
			n, err := k8sCore.GetNodeByName(rs.Id)
			if err != nil || n == nil {
				addrs := []string{rs.DataIp, rs.MgmtIp}
				n, err = k8sCore.SearchNodeByAddresses(addrs)
				if err != nil || n == nil {
					return nil, true, fmt.Errorf("failed to locate node using id: %s and addresses: %v",
						rs.Id, addrs)
				}
			}

			if _, ok := n.Labels[pvc]; !ok {
				return nil, true, &errLabelAbsent{
					node:  n.Name,
					label: pvc,
				}
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, 2*time.Minute, 10*time.Second); err != nil {
			if _, ok := err.(*errLabelAbsent); ok {
				missingLabelNodes = append(missingLabelNodes, rs.Id)
			} else {
				return err
			}
		}
	}

	if len(missingLabelNodes) > 0 {
		return &ErrLabelMissingOnNode{
			Label: pvc,
			Nodes: missingLabelNodes,
		}
	}
	return nil
}

func (k *k8sSchedOps) ValidateRemoveLabels(vol *volume.Volume) error {
	pvcLabel := vol.Name
	var staleLabelNodes []string
	for _, n := range node.GetWorkerNodes() {
		t := func() (interface{}, bool, error) {
			nodeLabels, err := k8sCore.GetLabelsOnNode(n.Name)
			if err != nil {
				return nil, true, err
			}

			if _, ok := nodeLabels[pvcLabel]; ok {
				return nil, true, &errLabelPresent{
					node:  n.Name,
					label: pvcLabel,
				}
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second); err != nil {
			if _, ok := err.(*errLabelPresent); ok {
				staleLabelNodes = append(staleLabelNodes, n.Name)
			} else {
				return err
			}
		}
	}

	if len(staleLabelNodes) > 0 {
		return &ErrLabelNotRemovedFromNode{
			Label: pvcLabel,
			Nodes: staleLabelNodes,
		}
	}

	return nil
}

func (k *k8sSchedOps) ValidateVolumeSetup(vol *volume.Volume, d node.Driver) error {
	pvName := k.GetVolumeName(vol)
	if len(pvName) == 0 {
		return fmt.Errorf("failed to get PV name for : %v", vol)
	}

	t := func() (interface{}, bool, error) {
		pods, err := k8sCore.GetPodsUsingPV(pvName)
		printStatus(pods...)
		if err != nil {
			return nil, true, err
		}
		resp, err := k.validateMountsInPods(vol, pvName, pods, d)
		if err != nil {
			logrus.Errorf("failed to validate mount in pod. Cause: %v", err)
			return nil, true, err
		}
		lenValidatedPods := len(resp)
		lenExpectedPods := len(pods)
		// in case we have a Deployment/ReplicaSet or StatefulSet the expected pods are the same as set in
		// .Spec.Replicas field
		if lenExpectedPods > 0 {
			for _, ownerref := range pods[0].OwnerReferences {
				switch ownerref.Kind {
				case "ReplicaSet":
					rs, err := k8sApps.GetReplicaSet(ownerref.Name, pods[0].Namespace)
					if err != nil {
						logrus.Errorf("failed to get replicaset %s. cause: %v", ownerref.Name, err)
						return nil, true, err
					}
					lenExpectedPods = int(*rs.Spec.Replicas)
				case "StatefulSet":
					st, err := k8sApps.GetStatefulSet(ownerref.Name, pods[0].Namespace)
					if err != nil {
						logrus.Errorf("failed to get statefulset %s. cause: %v", ownerref.Name, err)
						return nil, true, err
					}
					lenExpectedPods = int(*st.Spec.Replicas)
				}
			}
			// in case we have more pods for a non shared volume we expect only one of them to be ready
			if !vol.Shared {
				lenExpectedPods = 1
			}
		}

		if lenValidatedPods == lenExpectedPods {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("pods pending validation current: %d. Expected: %d", lenValidatedPods, lenExpectedPods)
	}

	if _, err := task.DoRetryWithTimeout(t, defaultTimeout, defaultRetryInterval); err != nil {
		return err
	}

	return nil
}

func (k *k8sSchedOps) validateMountsInPods(
	vol *volume.Volume,
	pvName string,
	pods []corev1.Pod,
	d node.Driver) ([]string, error) {

	validatedMountPods := make([]string, 0)
	nodes := node.GetNodesByName()
PodLoop:
	for _, p := range pods {
		pod, err := k8sCore.GetPodByName(p.Name, p.Namespace)
		if err != nil && err == k8serrors.ErrPodsNotFound {
			logrus.Warnf("pod %s not found. probably it got rescheduled", p.Name)
			continue
		} else if !k8sCore.IsPodReady(*pod) {
			// if pod is not ready, delay the check
			printStatus(*pod)
			continue
		} else if err != nil {
			return validatedMountPods, err
		}

		containerPaths := getContainerPVCMountMap(*pod)
		skipHostMountCheck := false
		for containerName, path := range containerPaths {
			pxMountCheckRegex := regexp.MustCompile(fmt.Sprintf("^(/dev/pxd.+|pxfs.+|/dev/mapper/pxd-enc.+|/dev/loop.+|\\d+\\.\\d+\\.\\d+\\.\\d+:/var/lib/osd/pxns.+) %s.+", path))
			output, err := k8sCore.RunCommandInPod([]string{"cat", "/proc/mounts"}, pod.Name, containerName, pod.Namespace)
			if err != nil && (err == k8serrors.ErrPodsNotFound || strings.Contains(err.Error(), "container not found")) {
				// if pod is not found or in completed state so delay the check and move to next pod
				logrus.Warnf("Failed to execute command in pod. Cause %v", err)
				continue PodLoop
			} else if err != nil {
				return validatedMountPods, err
			}
			mounts := strings.Split(output, "\n")
			pxMountFound := false
			for _, line := range mounts {
				pxMounts := pxMountCheckRegex.FindStringSubmatch(line)
				if len(pxMounts) > 0 {
					logrus.Debugf("pod: [%s] %s has PX mount: %v", pod.Namespace, pod.Name, pxMounts)
					pxMountFound = true
					// in case there are two pods running with non shared volume, one of them will be in read-only
					skipHostMountCheck = isMountReadOnly(line)
					break
				}
			}

			if !pxMountFound {
				return validatedMountPods, fmt.Errorf("pod: [%s] %s does not have PX mount. Mounts are: %v", pod.Namespace, pod.Name, mounts)
			}
		}

		if skipHostMountCheck {
			continue
		}

		currentNode, nodeExists := nodes[p.Spec.NodeName]
		if !nodeExists {
			return validatedMountPods, fmt.Errorf("node %s for pod [%s] %s not found", p.Spec.NodeName, p.Namespace, p.Name)
		}

		// ignore error when a command not exactly fail, like grep when empty return exit 1
		connOpts := node.ConnectionOpts{
			TimeBeforeRetry: defaultRetryInterval,
			Timeout:         defaultTimeout,
			IgnoreError:     true,
		}

		volMount, _ := d.RunCommand(currentNode,
			fmt.Sprintf("cat /proc/mounts | grep -E '(pxd|pxfs|pxns|pxd-enc|loop)' | grep %s", pvName), connOpts)
		if len(volMount) == 0 {
			return validatedMountPods, fmt.Errorf("volume %s not mounted on node %s", vol.Name, currentNode.Name)
		}

		validatedMountPods = append(validatedMountPods, pod.Name)
	}
	return validatedMountPods, nil
}

func isMountReadOnly(mount string) bool {
	var re = regexp.MustCompile(`ro,|,ro`)
	return re.MatchString(mount)
}

func (k *k8sSchedOps) ValidateSnapshot(params map[string]string, parent *api.Volume) error {
	if parentPVCAnnotation, ok := params[snapshotAnnotation]; ok {
		logrus.Debugf("Validating annotation based snapshot/clone")
		return k.validateVolumeClone(parent, parentPVCAnnotation)
	} else if snapshotName, ok := params[storkSnapshotAnnotation]; ok {
		logrus.Debugf("Validating Stork clone")
		return k.validateStorkClone(parent, snapshotName)
	}
	logrus.Debugf("Validating Stork snapshot")
	return k.validateStorkSnapshot(parent, params)
}

func (k *k8sSchedOps) validateVolumeClone(parent *api.Volume, parentAnnotation string) error {
	parentPVCName, exists := parent.Locator.VolumeLabels[pvcLabel]
	if !exists {
		return fmt.Errorf("Parent volume does not have a PVC label")
	}

	if parentPVCName != parentAnnotation {
		return fmt.Errorf("Parent name [%s] does not match the source PVC annotation "+
			"[%s] on the clone/snapshot", parentPVCName, parentAnnotation)
	}
	return nil
}

func (k *k8sSchedOps) validateStorkClone(parent *api.Volume, snapshotName string) error {
	volumeLabels := parent.Locator.VolumeLabels
	if volumeLabels != nil {
		snapName, ok := volumeLabels[storkSnapshotNameKey]
		if ok && snapName == snapshotName {
			return nil
		}
	}

	parentName := parent.Locator.Name
	if parentName == snapshotName {
		return nil
	}

	return fmt.Errorf("snapshot annotation: %s on the clone PVC matches neither parent volume "+
		"name: %s nor parent volume labels: %v", snapshotName, parentName, volumeLabels)
}

func (k *k8sSchedOps) validateStorkSnapshot(parent *api.Volume, params map[string]string) error {
	parentName, exists := parent.Locator.VolumeLabels[pvcLabel]
	if !exists {
		return fmt.Errorf("Parent volume does not have a PVC label")
	}

	if parentName != params[k8sdriver.SnapshotParent] {
		return fmt.Errorf("Parent PVC name [%s] does not match the snapshot's source "+
			"PVC [%s]", parentName, params[k8sdriver.SnapshotParent])
	}
	return nil
}

func (k *k8sSchedOps) GetVolumeName(vol *volume.Volume) string {
	if vol != nil && vol.ID != "" {
		return vol.ID
	}
	return ""
}

func (k *k8sSchedOps) ValidateVolumeCleanup(d node.Driver) error {
	nodeToPodsMap := make(map[string][]string)
	nodeMap := make(map[string]node.Node)

	connOpts := node.ConnectionOpts{
		Timeout:         1 * time.Minute,
		TimeBeforeRetry: 10 * time.Second,
	}
	listVolOpts := node.FindOpts{
		ConnectionOpts: connOpts,
		Name:           "*portworx-volume",
	}

	for _, n := range node.GetStorageDriverNodes() {
		volDirList, _ := d.FindFiles(k8sPodsRootDir, n, listVolOpts)
		nodeToPodsMap[n.Name] = separateFilePaths(volDirList)
		nodeMap[n.Name] = n
	}

	existingPods, _ := k8sCore.GetPods("", nil)

	orphanPodsMap := make(map[string][]string)
	dirtyVolPodsMap := make(map[string][]string)
	dirFindOpts := node.FindOpts{
		ConnectionOpts: connOpts,
		MaxDepth:       1,
		MinDepth:       1,
		Type:           node.Directory,
	}

	for nodeName, volDirPaths := range nodeToPodsMap {
		var orphanPods []string
		var dirtyVolPods []string

		for _, path := range volDirPaths {
			podUID := extractPodUID(path)
			found := false
			for _, existingPod := range existingPods.Items {
				if podUID == string(existingPod.UID) {
					found = true
					break
				}
			}
			if found {
				continue
			}

			n := nodeMap[nodeName]
			// Check if /var/lib/kubelet/pods/{podUID}/volumes/kubernetes.io~portworx-volume is empty
			if !isDirEmpty(path, n, d) {
				pvcDirsFind, _ := d.FindFiles(path, n, dirFindOpts)
				pvcDirs := separateFilePaths(pvcDirsFind)
				for _, pvcDir := range pvcDirs {
					// Check if /var/lib/kubelet/pods/{podUID}/volumes/kubernetes.io~portworx-volume/{pvc} is empty
					if isDirEmpty(pvcDir, n, d) {
						orphanPods = append(orphanPods, podUID)
					} else {
						dirtyVolPods = append(dirtyVolPods, podUID)
					}
				}
			}
		}

		if len(orphanPods) > 0 {
			orphanPodsMap[nodeName] = orphanPods
		}
		if len(dirtyVolPods) > 0 {
			dirtyVolPodsMap[nodeName] = dirtyVolPods
		}
	}

	if len(dirtyVolPodsMap) == 0 {
		return nil
	}
	return &ErrFailedToCleanupVolume{
		OrphanPods:   orphanPodsMap,
		DirtyVolPods: dirtyVolPodsMap,
	}
}

func isDirEmpty(path string, n node.Node, d node.Driver) bool {
	emptyDirsFindOpts := node.FindOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         1 * time.Minute,
			TimeBeforeRetry: 10 * time.Second,
		},
		MaxDepth: 0,
		MinDepth: 0,
		Type:     node.Directory,
		Empty:    true,
	}
	if emptyDir, _ := d.FindFiles(path, n, emptyDirsFindOpts); len(emptyDir) == 0 {
		return false
	}
	return true
}

// GetServiceEndpoint get IP addr of portworx-service, preferable external IP
func (k *k8sSchedOps) GetServiceEndpoint() (string, error) {
	return k8sCore.GetServiceEndpoint(PXServiceName, PXNamespace)
}

func (k *k8sSchedOps) UpgradePortworx(ociImage, ociTag, pxImage, pxTag string) error {

	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "talisman",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      talismanServiceAccount,
			Namespace: PXNamespace,
		}},
		RoleRef: rbacv1.RoleRef{
			Kind: "ClusterRole",
			Name: "cluster-admin",
		},
	}
	binding, err := k8sRbac.CreateClusterRoleBinding(binding)
	if err != nil {
		return err
	}

	account := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      talismanServiceAccount,
			Namespace: PXNamespace,
		},
	}
	account, err = k8sCore.CreateServiceAccount(account)
	if err != nil {
		return err
	}

	args := []string{
		"-operation", "upgrade",
		"-ocimonimage", ociImage,
		"-ocimontag", ociTag,
	}

	if pxImage != "" && pxTag != "" {
		args = append(args, "-pximage", pxImage, "-pxtag", pxTag)
	}

	logrus.Infof("args: %v", args)

	// create a talisman job
	var valOne int32 = 1
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "talisman",
			Namespace: PXNamespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &valOne,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					ServiceAccountName: talismanServiceAccount,
					Containers: []corev1.Container{
						{
							Name:  "talisman",
							Image: talismanImage,
							Args:  args,
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}

	job, err = k8sBatch.CreateJob(job)
	if err != nil {
		return err
	}

	numNodes, err := k8sCore.GetNodes()
	if err != nil {
		return err
	}

	jobTimeout := time.Duration(len(numNodes.Items)) * 10 * time.Minute

	err = k8sBatch.ValidateJob(job.Name, job.Namespace, jobTimeout)
	if err != nil {
		return err
	}

	// cleanup
	err = k8sBatch.DeleteJob(job.Name, job.Namespace)
	if err != nil {
		return err
	}

	err = k8sRbac.DeleteClusterRoleBinding(binding.Name)
	if err != nil {
		return err
	}

	err = k8sCore.DeleteServiceAccount(account.Name, account.Namespace)
	if err != nil {
		return err
	}

	return nil
}

// IsPXReadyOnNode validates if Portworx pod is up and running
func (k *k8sSchedOps) IsPXReadyOnNode(n node.Node) bool {
	pxPods, err := k8sCore.GetPodsByNode(n.Name, PXNamespace)
	if err != nil {
		logrus.Errorf("Failed to get apps on node %s", n.Name)
		return false
	}
	for _, pod := range pxPods.Items {
		if pod.Labels["name"] == PXDaemonSet && !k8sCore.IsPodReady(pod) {
			printStatus(pod)
			return false
		}
	}
	return true
}

// IsPXEnabled returns true  if px is enabled on given node
func (k *k8sSchedOps) IsPXEnabled(n node.Node) (bool, error) {
	t := func() (interface{}, bool, error) {
		node, err := k8sCore.GetNodeByName(n.Name)
		if err != nil {
			logrus.Errorf("Failed to get node %v", err)
			return nil, true, err
		}
		return node, false, nil
	}

	node, err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second)
	if err != nil {
		logrus.Errorf("Failed to get node %v", err)
		return false, err
	}

	kubeNode := node.(*corev1.Node)
	// if node has px/enabled label set to false or node-type public or
	// has any taints then px is disabled on node
	if kubeNode.Labels[PXEnabledLabelKey] == "false" || kubeNode.Labels[dcosNodeType] == "public" || len(kubeNode.Spec.Taints) > 0 {
		logrus.Infof("PX is not enabled on node %v. Will be skipped for tests.", n.Name)
		return false, nil
	}

	// for OpenShift Enterprise if node has node-role.kubernetes.io/infra=true and
	// it doesn't have node-role.kubernetes.io/compute=true then PX is disabled on node
	if nodeLabelValue, hasKey := kubeNode.Labels[k8sRoleNodeInfraLabelKey]; hasKey {
		if nodeLabelValue == "true" {
			if _, hasKey := kubeNode.Labels[k8sRoleNodeComputeLabelKey]; !hasKey {
				logrus.Infof("PX is not enabled on node %v. Will be skipped for tests.", n.Name)
				return false, nil
			}
		}

	}

	logrus.Infof("PX is enabled on node %v.", n.Name)
	return true, nil
}

// GetRemotePXNodes returns list of PX node found on destination k8s cluster
// refereced by kubeconfig
func (k *k8sSchedOps) GetRemotePXNodes(destKubeConfig string) ([]node.Node, error) {
	var addrs []string
	var remoteNodeList []node.Node

	pxNodes, err := getPXNodes(destKubeConfig)
	if err != nil {
		logrus.Errorf("Error getting PX Nodes %v : %v", pxNodes, err)
		return nil, err
	}

	for _, pxNode := range pxNodes {
		logrus.Info("px node on remote :", pxNode.Name)
		for _, addr := range pxNode.Status.Addresses {
			if addr.Type == corev1.NodeExternalIP || addr.Type == corev1.NodeInternalIP {
				addrs = append(addrs, addr.Address)
			}
		}
		newNode := node.Node{
			Name:      pxNode.Name,
			Addresses: addrs,
			Type:      node.TypeWorker,
		}

		remoteNodeList = append(remoteNodeList, newNode)
	}

	return remoteNodeList, nil
}

// getContainerPVCMountMap is a helper routine to return map of containers in the pod that
// have a PVC. The values in the map are the mount paths of the PVC
func getContainerPVCMountMap(pod corev1.Pod) map[string]string {
	containerPaths := make(map[string]string)

	// Each pvc in a pod spec has a associated name (which is different from the actual PVC name).
	// These names get referenced by containers in a pod. So first let's get a map of these names.
	// e.g below PVC "px-nginx-pvc" has a name "nginx-persistent-storage" below
	//  volumes:
	//  - name: nginx-persistent-storage
	//    persistentVolumeClaim:
	//      claimName: px-nginx-pvc
	pvcNamesInSpec := make(map[string]string)
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			pvcNamesInSpec[v.Name] = v.PersistentVolumeClaim.ClaimName
		}
	}

	// Now find containers in the pod that use above PVCs and also get their destination mount paths
	for _, c := range pod.Spec.Containers {
		for _, cMount := range c.VolumeMounts {
			if _, ok := pvcNamesInSpec[cMount.Name]; ok {
				containerPaths[c.Name] = cMount.MountPath
			}
		}
	}

	return containerPaths
}

func separateFilePaths(volDirList string) []string {
	trimmedList := strings.TrimSpace(volDirList)
	if trimmedList == "" {
		return []string{}
	}
	return strings.Split(trimmedList, "\n")
}

func extractPodUID(volDirPath string) string {
	re := regexp.MustCompile(k8sPodsRootDir +
		"/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/.*")
	match := re.FindStringSubmatch(volDirPath)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}

// return PX nodes on k8s cluster provided by kubeconfig file
func getPXNodes(destKubeConfig string) ([]corev1.Node, error) {
	var pxNodes []corev1.Node
	// get schd-ops/k8s instance of destination cluster
	destClient, err := core.NewInstanceFromConfigFile(destKubeConfig)
	if err != nil {
		logrus.Errorf("Unable to get k8s instance: %v", err)
		return nil, err
	}

	nodes, err := destClient.GetNodes()
	if err != nil {
		return nil, err
	}

	// get label on node where PX is Enabled
	for _, node := range nodes.Items {
		pxEnabled := true
		for key, value := range pxDisabledConditions {
			if node.Labels[key] == value {
				pxEnabled = false
				break
			}
		}
		// worker node and px is not disabled
		if !destClient.IsNodeMaster(node) && pxEnabled {
			pxNodes = append(pxNodes, node)
		}
	}

	return pxNodes, nil
}

// CreateAutopilotRule creates the AutopilotRule object
func (k *k8sSchedOps) CreateAutopilotRule(apRule apapi.AutopilotRule) (*apapi.AutopilotRule, error) {
	k8sOps := k8sAutopilot
	apRule.Labels = map[string]string{
		"creator": "torpedo",
	}
	autopilotRule, err := k8sOps.CreateAutopilotRule(&apRule)
	if k8s_errors.IsAlreadyExists(err) {
		if autopilotRule, err := k8sOps.GetAutopilotRule(apRule.Name); err == nil {
			logrus.Infof("Using existing autopilot rule: %v", apRule.ObjectMeta.Name)
			return autopilotRule, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to create autopilot rule: %v. Err: %v", apRule.Name, err)
	}
	logrus.Infof("Created Autopilot Rule: %v", autopilotRule.ObjectMeta.Name)
	return autopilotRule, nil
}

func (k *k8sSchedOps) ListAutopilotRules() (*apapi.AutopilotRuleList, error) {
	k8sOps := k8sAutopilot
	listAutopilotRules, err := k8sOps.ListAutopilotRules()
	if err != nil {
		return nil, fmt.Errorf("Failed to get list of autopilotrules. Err: %v", err)
	}
	return listAutopilotRules, nil
}

func printStatus(pods ...corev1.Pod) {
	for _, pod := range pods {
		status := ""
		ready := false
		for _, st := range pod.Status.Conditions {
			switch st.Type {
			case corev1.PodScheduled:
				status += fmt.Sprintf("Scheduled: %v ", st.Status)
			case corev1.PodReady:
				status += fmt.Sprintf("Ready: %v ", st.Status)
				ready, _ = strconv.ParseBool(fmt.Sprintf("%v", st.Status))
			case corev1.PodInitialized:
				status += fmt.Sprintf("Initialized: %v ", st.Status)
			}
		}
		if len(pod.Status.Reason) > 0 {
			status += fmt.Sprintf("Phase: %v Reason: %s", pod.Status.Phase, pod.Status.Reason)
		}
		if ready {
			logrus.Infof("Pod [%s] %s ready on node %s - %s", pod.Namespace, pod.Name, pod.Spec.NodeName,
				status)
		} else {
			logrus.Infof("Pod [%s] %s not ready on node %s - %s", pod.Namespace, pod.Name,
				pod.Spec.NodeName, status)
		}
	}
}

func init() {
	k := &k8sSchedOps{}
	Register("k8s", k)
}
