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
	pvcLabel               = "pvc"
	talismanServiceAccount = "talisman-account"
	talismanImage          = "portworx/talisman:latest"
)

const (
	podExecTimeout       = 2 * time.Minute
	defaultRetryInterval = 10 * time.Second
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

func (k *k8sSchedOps) ValidateVolumeSetup(vol *volume.Volume) error {
	pvName := k.GetVolumeName(vol)
	if len(pvName) == 0 {
		return fmt.Errorf("failed to get PV name for : %v", vol)
	}

	pods, err := k8s.Instance().GetPodsUsingPV(pvName)
	if err != nil {
		return err
	}

	for _, p := range pods {
		if ready := k8s.Instance().IsPodReady(p); !ready {
			continue
		}

		containerPaths := getContainerPVCMountMap(p)
		for containerName, path := range containerPaths {
			pxMountCheckRegex := regexp.MustCompile(fmt.Sprintf("^(/dev/pxd.+|pxfs.+) %s.+", path))

			t := func() (interface{}, bool, error) {
				output, err := k8s.Instance().RunCommandInPod([]string{"cat", "/proc/mounts"}, p.Name, containerName, p.Namespace)
				if err != nil {
					logrus.Errorf("failed to run command in pod: %v err: %v", p, err)
					return nil, true, err
				}

				return output, false, nil
			}

			output, err := task.DoRetryWithTimeout(t, podExecTimeout, defaultRetryInterval)
			if err != nil {
				return err
			}

			mounts := strings.Split(output.(string), "\n")
			pxMountFound := false
			for _, line := range mounts {
				pxMounts := pxMountCheckRegex.FindStringSubmatch(line)
				if len(pxMounts) > 0 {
					logrus.Debugf("pod: [%s] %s have PX mount: %v", p.Namespace, p.Name, pxMounts)
					pxMountFound = true
					break
				}
			}

			if !pxMountFound {
				return fmt.Errorf("pod: [%s] %s does not have PX mount. Mounts are: %v", p.Namespace, p.Name, mounts)
			}
		}
	}

	return nil
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

	existingPods, _ := k8s.Instance().GetPods("")

	orphanPodsMap := make(map[string][]string)
	dirtyVolPodsMap := make(map[string][]string)

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
			orphanPods = append(orphanPods, podUID)

			// Check if there are files under portworx volume
			// We use a depth of 2 because the files stored in the volume are in the pvc
			// directory under the portworx-volume folder for that pod. For instance,
			// ../kubernetes-io~portworx-volume/pvc-<id>/<all_user_files>
			n := nodeMap[nodeName]
			findFileOpts := node.FindOpts{
				ConnectionOpts: connOpts,
				MinDepth:       2,
				MaxDepth:       2,
			}
			files, _ := d.FindFiles(path, n, findFileOpts)
			if len(strings.TrimSpace(files)) > 0 {
				dirtyVolPods = append(dirtyVolPods, podUID)
			}
		}

		if len(orphanPods) > 0 {
			orphanPodsMap[nodeName] = orphanPods
			if len(dirtyVolPods) > 0 {
				dirtyVolPodsMap[nodeName] = dirtyVolPods
			}
		}
	}

	if len(orphanPodsMap) == 0 {
		return nil
	}
	return &ErrFailedToCleanupVolume{
		OrphanPods:   orphanPodsMap,
		DirtyVolPods: dirtyVolPodsMap,
	}
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

func init() {
	k := &k8sSchedOps{}
	Register("k8s", k)
}
