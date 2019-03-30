package schedops

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/sched-ops/k8s"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	k8s_driver "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/sirupsen/logrus"
	batch_v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// PXServiceName is the name of the portworx service in kubernetes
	PXServiceName = "portworx-service"
	// PXNamespace is the kubernetes namespace in which portworx daemon set runs
	PXNamespace = "kube-system"
	// PXDaemonSet is the name of portworx daemon set in k8s deployment
	PXDaemonSet = "portworx"
	// k8sPxServiceLabelKey is the label key used for px systemd service control
	k8sPxServiceLabelKey = "px/service"
	// k8sServiceOperationStart is label value for starting Portworx service
	k8sServiceOperationStart = "start"
	// k8sServiceOperationStop is label value for stopping Portworx service
	k8sServiceOperationStop = "stop"
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
	// pxenable is label used to check whethere px installation is enabled/disabled on node
	pxEnabled = "px/enabled"
	// nodeType is label used to check kubernetes node-type
	dcosNodeType           = "kubernetes.dcos.io/node-type"
	talismanServiceAccount = "talisman-account"
	talismanImage          = "portworx/talisman:latest"
)

const (
	defaultRetryInterval = 5 * time.Second
	defaultTimeout       = 2 * time.Minute
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

func (k *k8sSchedOps) StopPxOnNode(n node.Node) error {
	return k8s.Instance().AddLabelOnNode(n.Name, k8sPxServiceLabelKey, k8sServiceOperationStop)
}

func (k *k8sSchedOps) StartPxOnNode(n node.Node) error {
	return k8s.Instance().AddLabelOnNode(n.Name, k8sPxServiceLabelKey, k8sServiceOperationStart)
}

func (k *k8sSchedOps) ValidateOnNode(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateOnNode",
	}
}

func (k *k8sSchedOps) ValidateAddLabels(replicaNodes []api.Node, vol *api.Volume) error {
	pvc, ok := vol.Locator.VolumeLabels[pvcLabel]
	if !ok {
		return nil
	}

	var missingLabelNodes []string
	for _, rs := range replicaNodes {
		t := func() (interface{}, bool, error) {
			n, err := k8s.Instance().GetNodeByName(rs.Id)
			if err != nil || n == nil {
				addrs := []string{rs.DataIp, rs.MgmtIp}
				n, err = k8s.Instance().SearchNodeByAddresses(addrs)
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
			nodeLabels, err := k8s.Instance().GetLabelsOnNode(n.Name)
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

	validatedPods := make([]string, 0)
	t := func() (interface{}, bool, error) {
		pods, err := k8s.Instance().GetPodsUsingPV(pvName)
		if err != nil {
			return nil, true, err
		}
		resp, err := k.validateMountsInPods(vol, pvName, pods, d, validatedPods)
		if err != nil {
			logrus.Errorf("failed to validate mount in pods: %v err: %v", pods, err)
			return nil, true, err
		}
		validatedPods = append(validatedPods, resp...)
		lenValidatedPods := len(validatedPods)
		lenExpectedPods := len(pods)
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

func excludePods(pods []corev1.Pod, excludePods []string) []corev1.Pod {
	if len(excludePods) == 0 {
		return pods
	}
	newPods := make([]corev1.Pod, 0)
	for _, pod := range pods {
		count := 0
		for _, podName := range excludePods {
			if podName == pod.Name {
				count++
			}
		}
		if count == 0 {
			newPods = append(newPods, pod)
		}
	}
	return newPods
}

func (k *k8sSchedOps) validateMountsInPods(
	vol *volume.Volume,
	pvName string,
	pods []corev1.Pod,
	d node.Driver,
	podsToExclude []string) ([]string, error) {

	validatedMountPods := make([]string, 0)
	nodes := node.GetNodesByName()
	newPods := excludePods(pods, podsToExclude)
	for _, p := range newPods {
		currentNode, nodeExists := nodes[p.Spec.NodeName]
		if !nodeExists {
			return validatedMountPods, fmt.Errorf("node %s for pod [%s] %s not found", p.Spec.NodeName, p.Namespace, p.Name)
		}

		pod, err := k8s.Instance().GetPodByName(p.Name, p.Namespace)
		if err != nil && err == k8s.ErrPodsNotFound {
			logrus.Warnf("pod %s not found. probably it got rescheduled", p.Name)
			continue
		} else if !k8s.Instance().IsPodReady(*pod) && ((len(validatedMountPods) > 0 || len(podsToExclude) > 0) && !vol.Shared) {
			//when volume is not shared and there is one pod already validated, skip the other pods
			remainingPods := excludePods(newPods, validatedMountPods)
			t := func() []string {
				pods := make([]string, 0)
				for _, pod := range remainingPods {
					pods = append(pods, pod.Name)
				}
				return pods
			}
			validatedMountPods = append(validatedMountPods, t()...)
			break
		} else if !k8s.Instance().IsPodReady(*pod) {
			// if pod is not ready, delay the check
			logrus.Warnf("pod %s still not running. Status: %v", pod.Name, pod.Status.Phase)
			continue
		} else if err != nil {
			return validatedMountPods, err
		}
		//logrus.Infof("Pod [%s] %s ready for volume setup check.\n Pod phase: %v\n Pod Init Container statuses: %v\n Pod Container Statuses: %v", pod.Namespace, pod.Name, pod.Status.Phase, pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses)

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
		containerPaths := getContainerPVCMountMap(*pod)
		for containerName, path := range containerPaths {
			pxMountCheckRegex := regexp.MustCompile(fmt.Sprintf("^(/dev/pxd.+|pxfs.+|/dev/mapper/pxd-enc.+|/dev/loop.+|\\d+\\.\\d+\\.\\d+\\.\\d+:/var/lib/osd/pxns.+) %s.+", path))
			output, err := k8s.Instance().RunCommandInPod([]string{"cat", "/proc/mounts"}, pod.Name, containerName, pod.Namespace)
			if err != nil && err != k8s.ErrPodsNotFound {
				return validatedMountPods, err
			} else if err == k8s.ErrPodsNotFound {
				// if pod is not found it is probably rescheduled so delay the check
				logrus.Warnf("Failed to execute command in pod, %s not found. probably it got rescheduled", pod.Name)
				continue
			}
			mounts := strings.Split(output, "\n")
			pxMountFound := false
			for _, line := range mounts {
				pxMounts := pxMountCheckRegex.FindStringSubmatch(line)
				if len(pxMounts) > 0 {
					logrus.Debugf("pod: [%s] %s has PX mount: %v", pod.Namespace, pod.Name, pxMounts)
					pxMountFound = true
					break
				}
			}

			if !pxMountFound {
				return validatedMountPods, fmt.Errorf("pod: [%s] %s does not have PX mount. Mounts are: %v", pod.Namespace, pod.Name, mounts)
			}
		}
		validatedMountPods = append(validatedMountPods, pod.Name)
	}
	return validatedMountPods, nil
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

	if parentName != params[k8s_driver.SnapshotParent] {
		return fmt.Errorf("Parent PVC name [%s] does not match the snapshot's source "+
			"PVC [%s]", parentName, params[k8s_driver.SnapshotParent])
	}
	return nil
}

func (k *k8sSchedOps) GetVolumeName(vol *volume.Volume) string {
	if vol != nil && vol.ID != "" {
		return fmt.Sprintf("pvc-%s", vol.ID)
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

	for _, n := range node.GetWorkerNodes() {
		volDirList, _ := d.FindFiles(k8sPodsRootDir, n, listVolOpts)
		nodeToPodsMap[n.Name] = separateFilePaths(volDirList)
		nodeMap[n.Name] = n
	}

	existingPods, _ := k8s.Instance().GetPods("", nil)

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

func (k *k8sSchedOps) GetServiceEndpoint() (string, error) {
	svc, err := k8s.Instance().GetService(PXServiceName, PXNamespace)
	if err == nil {
		return svc.Spec.ClusterIP, nil
	}
	return "", err
}

func (k *k8sSchedOps) UpgradePortworx(ociImage, ociTag string) error {
	inst := k8s.Instance()

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
	binding, err := inst.CreateClusterRoleBinding(binding)
	if err != nil {
		return err
	}

	account := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      talismanServiceAccount,
			Namespace: PXNamespace,
		},
	}
	account, err = inst.CreateServiceAccount(account)
	if err != nil {
		return err
	}

	// create a talisman job
	var valOne int32 = 1
	job := &batch_v1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "talisman",
			Namespace: PXNamespace,
		},
		Spec: batch_v1.JobSpec{
			BackoffLimit: &valOne,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					ServiceAccountName: talismanServiceAccount,
					Containers: []corev1.Container{
						{
							Name:  "talisman",
							Image: talismanImage,
							Args: []string{
								"-operation", "upgrade", "-ocimonimage", ociImage, "-ocimontag", ociTag,
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}

	job, err = inst.CreateJob(job)
	if err != nil {
		return err
	}

	numNodes, err := inst.GetNodes()
	if err != nil {
		return err
	}

	jobTimeout := time.Duration(len(numNodes.Items)) * 10 * time.Minute

	err = inst.ValidateJob(job.Name, job.Namespace, jobTimeout)
	if err != nil {
		return err
	}

	// cleanup
	err = inst.DeleteJob(job.Name, job.Namespace)
	if err != nil {
		return err
	}

	err = inst.DeleteClusterRoleBinding(binding.Name)
	if err != nil {
		return err
	}

	err = inst.DeleteServiceAccount(account.Name, account.Namespace)
	if err != nil {
		return err
	}

	return nil
}

// Method to validate if Portworx pod is up and running
func (k *k8sSchedOps) IsPXReadyOnNode(n node.Node) bool {
	pxPods, err := k8s.Instance().GetPodsByNode(n.Name, PXNamespace)
	if err != nil {
		logrus.Errorf("Failed to get apps on node %s", n.Name)
		return false
	}
	for _, pod := range pxPods.Items {
		if pod.Labels["name"] == PXDaemonSet && !k8s.Instance().IsPodReady(pod) {
			logrus.Errorf("Error on %s Pod: %v is not up yet. Pod Status: %v", pod.Status.PodIP, pod.Name, pod.Status.Phase)
			return false
		}
	}
	return true
}

// IsPXEnabled returns true  if px is enabled on given node
func (k *k8sSchedOps) IsPXEnabled(n node.Node) (bool, error) {
	t := func() (interface{}, bool, error) {
		node, err := k8s.Instance().GetNodeByName(n.Name)
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
	if kubeNode.Labels[pxEnabled] == "false" || kubeNode.Labels[dcosNodeType] == "public" || len(kubeNode.Spec.Taints) > 0 {
		logrus.Infof("PX is not enabled on node %v. Will be skipped for tests.", n.Name)
		return false, nil
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
	destClient, err := k8s.NewInstance(destKubeConfig)
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
		// worker node and px is not disabled
		if !destClient.IsNodeMaster(node) && node.Labels[pxEnabled] != "false" {
			pxNodes = append(pxNodes, node)
		}
	}

	return pxNodes, nil
}

func init() {
	k := &k8sSchedOps{}
	Register("k8s", k)
}
