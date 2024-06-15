package utils

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/libopenstorage/stork/pkg/k8sutils"

	"github.com/aquilax/truncate"
	patch "github.com/evanphx/json-patch"
	"github.com/libopenstorage/stork/drivers"
	kdmpDriver "github.com/portworx/kdmp/pkg/drivers"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// CattlePrefix is the prefix to all Rancher related annotations and labels
	CattlePrefix = "cattle.io"
	// CattleProjectPrefix is the prefix used in all Rancher project related annotations and labels
	CattleProjectPrefix = "cattle.io/projectId"
	// PXIncrementalCountAnnotation is the annotation used to set cloud backup incremental count
	// for volume
	PXIncrementalCountAnnotation = "portworx.io/cloudsnap-incremental-count"
	// trimCRDGroupNameKey - groups name containing the string from this configmap field will be trimmed
	trimCRDGroupNameKey = "TRIM_CRD_GROUP_NAME"
	// QuitRestoreCrTimestampUpdate is sent in the channel to informs the go routine to stop any further update
	QuitRestoreCrTimestampUpdate = 13
	// UpdateRestoreCrTimestampInDeleteResourcePath is sent in channel to signify go routine to update the timestamp
	UpdateRestoreCrTimestampInDeleteResourcePath = 11
	// UpdateRestoreCrTimestampInPrepareResourcePath is sent in channel to signify go routine to update the timestamp
	UpdateRestoreCrTimestampInPrepareResourcePath = 17
	// UpdateRestoreCrTimestampInApplyResourcePath is sent in channel to signify go routine to update the timestamp
	UpdateRestoreCrTimestampInApplyResourcePath = 19
	// duration in which the restore CR to be updated with timestamp
	TimeoutUpdateRestoreCrTimestamp = 15 * time.Minute
	// duration in which the backup CR to be updated with timestamp
	TimeoutUpdateBackupCrTimestamp = 15 * time.Minute
	// duration in which the restore CR to be updated for resource Count progress
	TimeoutUpdateRestoreCrProgress = 5 * time.Minute
	// sleep interval for restore time stamp update go-routine to check channel for any data
	SleepIntervalForCheckingChannel = 10 * time.Second
	// RestoreCrChannelBufferSize is the count of maximum signals it can hold in restore CR update related channel
	RestoreCrChannelBufferSize = 11
	// PrefixBackup - prefix string that will be used for the kdmp backup job
	PrefixBackup = "backup"
	// PrefixNFSBackup prefix string that will be used for the nfs backup job
	PrefixNFSBackup = "nfs-backup"
	// PrefixRestore prefix string that will be used for the kdmp restore job
	PrefixRestore = "nfs-restore-resource"
	// PrefixNFSRestorePVC prefix string that will be used for pvc creation during nfs vol restore
	PrefixNFSRestorePVC = "nfs-restore-pvc"
	// PrefixVMRestoreIncludeResourceMap prefix string that will be used for the kdmp restore job for VM includeresource map
	PrefixVMRestoreIncludeResourceMap = "nfs-restore-vm-includeresource"

	// KdmpAnnotationPrefix - KDMP annotation prefix
	KdmpAnnotationPrefix = "kdmp.portworx.com/"
	// ApplicationBackupCRNameKey - key name to store the applicationbackup CR name with KDMP annotation prefix
	ApplicationBackupCRNameKey = KdmpAnnotationPrefix + "applicationbackup-cr-name"
	// ApplicationBackupCRUIDKey - key name to store the applicationbackup CR UID with KDMP annotation prefix
	ApplicationBackupCRUIDKey = KdmpAnnotationPrefix + "applicationbackup-cr-uid"
	// BackupObjectNameKey - annotation key value for backup object name with KDMP annotation prefix
	BackupObjectNameKey = KdmpAnnotationPrefix + "backupobject-name"
	// BackupObjectUIDKey - annotation key value for backup object UID with KDMP annotation prefix
	BackupObjectUIDKey = KdmpAnnotationPrefix + "backupobject-uid"
	// ApplicationRestoreCRNameKey - key name to store the applicationrestore CR name with KDMP annotation prefix
	ApplicationRestoreCRNameKey = KdmpAnnotationPrefix + "applicationrestore-cr-name"
	// ApplicationRestoreCRUIDKey - key name to store the applicationrestore CR UID with KDMP annotation prefix
	ApplicationRestoreCRUIDKey = KdmpAnnotationPrefix + "applicationrestore-cr-uid"
	// RestoreObjectNameKey - key name to store the restore object name with KDMP annotation prefix
	RestoreObjectNameKey = KdmpAnnotationPrefix + "restoreobject-name"
	// RestoreObjectUIDKey - key name to store the restore object UID with KDMP annotation prefix
	RestoreObjectUIDKey = KdmpAnnotationPrefix + "restoreobject-uid"

	// PxbackupAnnotationPrefix - px-backup annotation prefix
	PxbackupAnnotationPrefix = "portworx.io/"
	// PxbackupAnnotationCreateByKey - annotation key name to indicate whether the CR was created by px-backup or stork
	PxbackupAnnotationCreateByKey = PxbackupAnnotationPrefix + "created-by"
	// PxbackupAnnotationCreateByValue - annotation key value for create-by key for px-backup
	PxbackupAnnotationCreateByValue = "px-backup"

	// PxbackupObjectUIDKey -annotation key name for backup object UID with px-backup prefix
	PxbackupObjectUIDKey = PxbackupAnnotationPrefix + "backup-uid"
	// PxbackupObjectNameKey - annotation key name for backup object name with px-backup prefix
	PxbackupObjectNameKey = PxbackupAnnotationPrefix + "backup-name"
	// PxbackupAnnotationSkipdriverKey - annotation key name to skip backup for this specific driver
	PxbackupAnnotationSkipdriverKey = PxbackupAnnotationPrefix + "skip-driver"

	// SkipResourceAnnotation - annotation value to skip resource during resource collector
	SkipResourceAnnotation = "stork.libopenstorage.org/skip-resource"
	// StorkAPIVersion API version
	StorkAPIVersion = "stork.libopenstorage.org/v1alpha1"
	// BackupLocationKind CR kind
	BackupLocationKind = "BackupLocation"
	// PXServiceName is the name of the portworx service in kubernetes
	PXServiceName                         = "portworx-service"
	VMRestoreIncludeResourceMapAnnotation = "stork.libopenstorage.org/vm-includeresource"
	// podSecurityStandardEnforceMode - which PS standard to enforce while deploying the k8s workload Object
	podSecurityStandardEnforceMode = "pod-security.kubernetes.io/enforce"
	//undefined UID or GID
	UndefinedId = int64(-1)
	PsaUIDKey   = "portworx.io/psa-uid"
	PsaGIDKey   = "portworx.io/psa-gid"
)

// Map of ignored namespace to be backed up for faster lookout
var IgnoreNamespaces = map[string]bool{
	"kube-system":     true,
	"kube-public":     true,
	"kube-node-lease": true,
}

// ParseKeyValueList parses a list of key=values string into a map
func ParseKeyValueList(expressions []string) (map[string]string, error) {
	matchLabels := make(map[string]string)
	for _, e := range expressions {
		entry := strings.SplitN(e, "=", 2)
		if len(entry) != 2 {
			return nil, fmt.Errorf("invalid key value: %s provided. "+
				"Example format: app=mysql", e)
		}

		matchLabels[entry[0]] = entry[1]
	}

	return matchLabels, nil
}

// GetTrimmedGroupName - get the trimmed group name
// Usually the groups of names of CRDs that belongs to the common operator have same group name.
// For example:
// keycloakbackups.keycloak.org, keycloakclients.keycloak.org, keycloakrealms.keycloak.org
// keycloaks.keycloak.org, keycloakusers.keycloak.org
// Here the group name is "keycloak.org"
// In some case, the CRDs names are as follow:
// agents.agent.k8s.elastic.co - groupname: agent.k8s.elastic.co
// apmservers.apm.k8s.elastic.co - groupname: apm.k8s.elastic.co
// beats.beat.k8s.elastic.co - group name: beat.k8s.elastic.co
// Here the group name are different even though they belong to a same opeator.
// But they have common three parts, like "k8s.elastic.co"
// So added a logic to combine the CRDs, if they have common last three part, if the group have more than three parts.
func GetTrimmedGroupName(group string) string {
	kdmpData, err := core.Instance().GetConfigMap(drivers.KdmpConfigmapName, drivers.KdmpConfigmapNamespace)
	if err != nil {
		logrus.Warnf("error in reading configMap [%v/%v]",
			drivers.KdmpConfigmapName, drivers.KdmpConfigmapNamespace)
		return group
	}
	if len(kdmpData.Data[trimCRDGroupNameKey]) != 0 {
		groupNameList := strings.Split(kdmpData.Data[trimCRDGroupNameKey], ",")
		for _, groupName := range groupNameList {
			if strings.Contains(group, groupName) {
				return groupName
			}
		}
	}
	return group
}

// GetStorageClassNameForPVC - Get the storageClass name from the PVC spec
func GetStorageClassNameForPVC(pvc *v1.PersistentVolumeClaim) (string, error) {
	var scName string
	if pvc.Spec.StorageClassName != nil && len(*pvc.Spec.StorageClassName) > 0 {
		scName = *pvc.Spec.StorageClassName
	} else {
		scName = pvc.Annotations[v1.BetaStorageClassAnnotation]
	}

	if len(scName) == 0 {
		return "", fmt.Errorf("PVC: %s does not have a storage class", pvc.Name)
	}
	return scName, nil
}

// ParseRancherProjectMapping - maps the target projectId to the source projectId
func ParseRancherProjectMapping(
	data map[string]string,
	projectMapping map[string]string,
) {
	for key, value := range data {
		if strings.Contains(key, CattleProjectPrefix) {
			if targetProjectID, ok := projectMapping[value]; ok &&
				targetProjectID != "" {
				data[key] = targetProjectID
			}
		}
	}
}

// GetSizeOfObject - Gets the in-memory size of a object
// It may include the golang runtime headers related to GC
// If the structure object contains unexported field, then encoder will fail.
func GetSizeOfObject(object interface{}) (int, error) {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(object); err != nil {
		return 0, err
	}
	return buf.Len(), nil
}

// Get ObjectDetails returns name, namespace, kind of the given object
func GetObjectDetails(o interface{}) (name, namespace, kind string, err error) {
	metadata, err := meta.Accessor(o)
	if err != nil {
		return "", "", "", err
	}
	objType, err := meta.TypeAccessor(o)
	if err != nil {
		return "", "", "", err
	}
	return metadata.GetName(), metadata.GetNamespace(), objType.GetKind(), nil
}

// GetValidLabel - will validate the label to make sure the length is less 63 and contains valid label format.
// If the length is greater then 63, it will truncate to 63 character.
func GetValidLabel(labelVal string) string {
	if len(labelVal) > validation.LabelValueMaxLength {
		labelVal = truncate.Truncate(labelVal, validation.LabelValueMaxLength, "", truncate.PositionEnd)
		// make sure the truncated value does not end with the hyphen.
		labelVal = strings.Trim(labelVal, "-")
		// make sure the truncated value does not end with the dot.
		labelVal = strings.Trim(labelVal, ".")
	}
	return labelVal
}

// GetShortUID returns the first part of the UID
func GetShortUID(uid string) string {
	if len(uid) < 8 {
		return ""
	}
	return uid[0:7]
}

func IsNFSBackuplocationType(
	namespace, name string,
) (bool, error) {
	backupLocation, err := storkops.Instance().GetBackupLocation(name, namespace)
	if err != nil {
		return false, fmt.Errorf("error getting backup location path for backup [%v/%v]: %v", namespace, name, err)
	}
	if backupLocation.Location.Type == stork_api.BackupLocationNFS {
		return true, nil
	}
	return false, nil
}

func GetUIDLastSection(uid types.UID) string {
	parts := strings.Split(string(uid), "-")
	uidLastSection := parts[len(parts)-1]

	if uidLastSection == "" {
		uidLastSection = string(uid)
	}
	return uidLastSection
}

func CompareFiles(filePath1 string, filePath2 string) (bool, error) {
	content1, err := os.ReadFile(filePath1)
	if err != nil {
		return false, err
	}

	content2, err := os.ReadFile(filePath2)
	if err != nil {
		return false, err
	}

	// Compare the byte content of the files
	return string(content1) == string(content2), nil
}

func GetStashedConfigMapName(objKind string, group string, objName string) string {
	cmName := fmt.Sprintf("%s-%s-%s", objKind, group, objName)
	if len(cmName) > 253 {
		cmName = cmName[:253]
	}
	return cmName
}

// Returns namespaces in which Portworx is deployed.
func GetPortworxNamespace() ([]string, error) {
	// https://docs.portworx.com/portworx-enterprise/reference/crd/storage-cluster#storagecluster-annotations
	// If you run Portworx in a namespace other than kube-system and not using the default 9001 start port
	// You can end up having multiple Portworx services in the cluster
	var namespaces []string
	allServices, err := core.Instance().ListServices("", metav1.ListOptions{})
	if err != nil {
		logrus.Errorf("error in getting list of all services")
		return nil, fmt.Errorf("failed to get list of services. Err: %v", err)
	}
	for _, svc := range allServices.Items {
		if svc.Name == PXServiceName {
			namespaces = append(namespaces, svc.Namespace)
		}
	}
	if len(namespaces) == 0 {
		logrus.Warnf("Unable to find [%s] service in the cluster", PXServiceName)
		return nil, fmt.Errorf("cannot find [%s] Portworx service from the list of services", PXServiceName)
	} else {
		logrus.Infof("Found [%s] service in the following namespaces: %v", PXServiceName, namespaces)
		return namespaces, nil
	}
}

// CreateVolumeSnapshotSchedulePatch will return the patch between two volumesnapshot schedule objects
func CreateVolumeSnapshotSchedulePatch(snapshot *stork_api.VolumeSnapshotSchedule, updatedSnapshot *stork_api.VolumeSnapshotSchedule) ([]byte, error) {
	oldData, err := runtime.Encode(unstructured.UnstructuredJSONScheme, snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal old data: %v", err)
	}
	newData, err := runtime.Encode(unstructured.UnstructuredJSONScheme, updatedSnapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal new data: %v", err)
	}

	patchBytes, err := patch.CreateMergePatch(oldData, newData)
	if err != nil {
		return nil, fmt.Errorf("failed to create merge patch: %v", err)
	}

	patchBytes, err = addResourceVersion(patchBytes, snapshot.ResourceVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to add resource version: %v", err)
	}

	return patchBytes, nil
}

func addResourceVersion(patchBytes []byte, resourceVersion string) ([]byte, error) {
	var patchMap map[string]interface{}
	err := json.Unmarshal(patchBytes, &patchMap)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling patch: %v", err)
	}
	u := unstructured.Unstructured{Object: patchMap}
	a, err := meta.Accessor(&u)
	if err != nil {
		return nil, fmt.Errorf("error creating accessor: %v", err)
	}
	a.SetResourceVersion(resourceVersion)
	versionBytes, err := json.Marshal(patchMap)
	if err != nil {
		return nil, fmt.Errorf("error marshalling json patch: %v", err)
	}
	return versionBytes, nil
}

func GetMergedNamespacesWithLabelSelector(namespaceList []string, namespaceSelectors map[string]string) ([]string, error) {
	if len(namespaceSelectors) == 0 {
		return namespaceList, nil
	}
	uniqueNamespaces := make(map[string]bool)
	for _, ns := range namespaceList {
		uniqueNamespaces[ns] = true
	}

	for key, val := range namespaceSelectors {
		namespaces, err := core.Instance().ListNamespaces(map[string]string{key: val})
		if err != nil {
			return nil, err
		}
		for _, namespace := range namespaces.Items {
			uniqueNamespaces[namespace.GetName()] = true
		}
	}

	migrationNamespaces := make([]string, 0, len(uniqueNamespaces))
	for namespace := range uniqueNamespaces {
		migrationNamespaces = append(migrationNamespaces, namespace)
	}
	return migrationNamespaces, nil
}

// IsSubList returns true if the first slice is sublist of the second slice.
// Returns ->
// bool isSubList : list A is a subset of list B
// []string subsetStrings : strings common in both list A and list B
// []string nonSubsetStrings : strings present only in list A and not in list B
func IsSubList(listA []string, listB []string) (bool, []string, []string) {
	// subsetStrings -> strings found in both A and B
	// nonSubsetStrings -> strings found in A, but not in B
	nonSubsetStrings := make([]string, 0)
	subsetStrings := make([]string, 0)
	superset := make(map[string]bool)
	for _, str := range listB {
		superset[str] = true
	}
	for _, str := range listA {
		if !superset[str] {
			nonSubsetStrings = append(nonSubsetStrings, str)
		} else {
			subsetStrings = append(subsetStrings, str)
		}
	}
	return len(nonSubsetStrings) == 0, subsetStrings, nonSubsetStrings
}

// ExcludeListAFromListB takes 2 slices of strings as input and returns subset of B which is disjoint from A
func ExcludeListAFromListB(listA []string, listB []string) []string {
	nonCommonStrings := make([]string, 0)
	setA := make(map[string]bool)
	for _, str := range listA {
		setA[str] = true
	}
	for _, str := range listB {
		if !setA[str] {
			nonCommonStrings = append(nonCommonStrings, str)
		}
	}
	return nonCommonStrings
}

// GetAdminNamespace we fetch the value of adminNamespace from the stork-controller-cm created in kube-system namespace
func GetAdminNamespace() string {
	adminNs, err := k8sutils.GetConfigValue(k8sutils.StorkControllerConfigMapName, metav1.NamespaceSystem, k8sutils.AdminNsKey)
	if err != nil {
		logrus.Warnf("Error in reading %v cm for the key %v, switching to default value : %v",
			k8sutils.StorkControllerConfigMapName, k8sutils.AdminNsKey, err)
		adminNs = k8sutils.DefaultAdminNamespace
	}
	return adminNs
}

func DoesMigrationScheduleMigrateNamespaces(migrationSchedule stork_api.MigrationSchedule, activatedNSList []string) (bool, error) {
	namespaceList := migrationSchedule.Spec.Template.Spec.Namespaces
	namespaceSelectors := migrationSchedule.Spec.Template.Spec.NamespaceSelectors
	migrationNamespaces, err := GetMergedNamespacesWithLabelSelector(namespaceList, namespaceSelectors)
	if err != nil {
		return false, fmt.Errorf("unable to get the namespaces based on the provided --namespace-selectors : %v", err)
	}
	activationNamespacesSet := make(map[string]bool)
	for _, ns := range activatedNSList {
		activationNamespacesSet[ns] = true
	}
	found := false
	for _, ns := range migrationNamespaces {
		if activationNamespacesSet[ns] {
			found = true
			break
		}
	}
	return found, nil
}

// get Pod from PVC details
func GetPodFromPVC(pvcName, namespace string) (*v1.Pod, error) {
	fn := "GetPodFromPVCName"
	// create pod list to store the relevant pods
	var podList []*v1.Pod
	pods, err := core.Instance().GetPodsUsingPVC(pvcName, namespace)
	if err != nil {
		errMsg := fmt.Sprintf("error fetching pods using PVC %s/%s: %v", namespace, pvcName, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	// filter out the pods that are created by us
	for _, pod := range pods {
		labels := pod.ObjectMeta.Labels
		if _, ok := labels[kdmpDriver.DriverNameLabel]; ok {
			continue
		}
		podList = append(podList, &pod)
	}

	if len(podList) == 0 {
		errMsg := fmt.Sprintf("no application pod found for PVC %s/%s", namespace, pvcName)
		logrus.Debugf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	} else if len(podList) > 1 {
		logrus.Warnf("%s: multiple application pods found using common PVC %s/%s", fn, namespace, pvcName)
		// TODO we need to handle this case of PVC being used by multiple pods in the same namespace
		// For now we will return the first pod in the list as a placeholder
	}
	return podList[0], nil
}

func getIdFromSecurityContext(securityContext interface{}) (int64, int64) {
	uid := UndefinedId
	gid := UndefinedId

	// Baseline or privilege PSA can have nil security Context.
	if securityContext == nil {
		return uid, gid
	}
	switch s := securityContext.(type) {
	case *v1.SecurityContext:
		if s != nil {
			if s.RunAsUser != nil {
				uid = int64(*s.RunAsUser)
			}
			if s.RunAsGroup != nil {
				gid = int64(*s.RunAsGroup)
			}
		}
	case *v1.PodSecurityContext:
		if s != nil {
			if s.RunAsUser != nil {
				uid = int64(*s.RunAsUser)
			}
			if s.RunAsGroup != nil {
				gid = int64(*s.RunAsGroup)
			}
		}
	default:
		// Handle the default case here
		logrus.Debugf("Unknown type of security context obtained: %v", s)
	}
	return uid, gid
}

// GetPodUserId: Get the UID/GID of the application pod which is using the PVC for which backup is triggered.
func GetPodUserId(pod *v1.Pod) (int64, int64) {
	uid := UndefinedId
	gid := UndefinedId

	//Get User ID or get Group-ID in the absence of User ID from container's security context
	if len(pod.Spec.Containers) > 1 {
		// Case-01: There can be more than one container present per application pod and each container could have
		//          configured SecurityContext separately. On that case we need user intervention to pick the container,
		//          in the absence of that for this release, we will pick first found UID logic as an best-effort approach.
		// Case 02: If more than one container exist per pod but only one container has defined securityContext then
		//          we can pick that though.
		for _, container := range pod.Spec.Containers {
			// No need to handle the case for the initContainers here
			// since it is a different list in pod spec i.e. pod.Spec.InitContainers
			securityContext := container.SecurityContext
			uid, gid = getIdFromSecurityContext(securityContext)
			if uid != UndefinedId || gid != UndefinedId {
				return uid, gid
			}
		}
	} else {
		// Only one container for the pod is found, let's pick the UID or GID of it
		securityContext := pod.Spec.Containers[0].SecurityContext
		uid, gid = getIdFromSecurityContext(securityContext)
		if uid != UndefinedId || gid != UndefinedId {
			return uid, gid
		}
	}
	// UID or GID is not found in any of the container spec, try Getting the UID/GID from the pod's security context
	if uid == UndefinedId && gid == UndefinedId {
		securityContext := pod.Spec.SecurityContext
		uid, gid = getIdFromSecurityContext(securityContext)
		if uid != UndefinedId || gid != UndefinedId {
			return uid, gid
		}
	}
	// Neither pod nor container has the securityContext defined with User Id or group Id, return error with undefined uids
	return uid, gid
}

// Get uid gid from application backup cr
func GetUIDGIDFromBackupCR(backup *stork_api.ApplicationBackup, pvcName string, pvcNamespace string) (int64, int64, error) {
	fn := "GetUIDGIDFromBackupCR"
	if backup == nil {
		err := fmt.Errorf("backup cr is nil")
		return UndefinedId, UndefinedId, err
	}
	// read the uid and gid from the application backup cr's volume array
	for _, volume := range backup.Status.Volumes {
		if volume.PersistentVolumeClaim == pvcName && volume.Namespace == pvcNamespace {
			logrus.Infof("%v: Found uid:[%v] and gid:[%v] for the restore of the pvc[%v]", fn, volume.JobSecurityContext.RunAsUser, volume.JobSecurityContext.RunAsGroup, volume.PersistentVolumeClaim)
			return volume.JobSecurityContext.RunAsUser, volume.JobSecurityContext.RunAsGroup, nil
		}
	}
	return UndefinedId, UndefinedId, nil
}

// GetAppUidGid - Get the UID/GID of the application pod which has psa enforcement in its namespace using the PVC
func GetAppUidGid(pvcName string, namespace string, backup *stork_api.ApplicationBackup, getUIDFromApp bool) (int64, int64, error) {
	fn := "GetAppUidGid"
	uid := UndefinedId
	gid := UndefinedId
	var err error

	if pvcName == "" || namespace == "" {
		err = fmt.Errorf("%v: pvc name or namespace is empty", fn)
		return uid, gid, err
	}
	var pod *v1.Pod
	if getUIDFromApp {
		// Get the pod which uses this PVC for backup scenario
		pod, err = GetPodFromPVC(pvcName, namespace)
		// Check if the error message returned is "no application pod found for PVC"
		// that means this is a stand alone PVC without any App so we don't need to look for UID/GID
		// But other errors we must return to caller with failure
		if err != nil && strings.Contains(err.Error(), "no application pod found for PVC") {
			return UndefinedId, UndefinedId, nil
		} else if err != nil {
			logrus.Errorf("%s: %v", fn, err)
			return uid, gid, err
		}
		// Get the UID/GID of the pod
		uid, gid = GetPodUserId(pod)
	} else {
		//get uid gid from applicationBackup CR for restore
		uid, gid, err = GetUIDGIDFromBackupCR(backup, pvcName, namespace)
		if err != nil {
			logrus.Errorf("%s: %v", fn, err)
			return uid, gid, err
		}
	}
	return uid, gid, nil
}
