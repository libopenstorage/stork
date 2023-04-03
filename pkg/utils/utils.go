package utils

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	"github.com/libopenstorage/stork/drivers"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
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
	// UpdateRestoreCrTimestamp is sent in channel to signify go routine to update the timestamp
	UpdateRestoreCrTimestamp = 11
	// duration in which the restore CR to be updated
	FifteenMinuteWait = 15 * time.Minute
	// sleep interval for restore time stamp update go-routine to check channel for any data
	SleepIntervalForCheckingChannel = 10 * time.Second
	// RestoreCrChannelBufferSize is the count of maximum signals it can hold in restore CR update related channel
	RestoreCrChannelBufferSize = 11
)

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
