package utils

import (
	"fmt"
	"github.com/libopenstorage/stork/drivers"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	"strings"
)

const (
	// CattlePrefix is the prefix to all Rancher related annotations and labels
	CattlePrefix = "cattle.io"
	// CattleProjectPrefix is the prefix used in all Rancher project related annotations and labels
	CattleProjectPrefix = "cattle.io/projectId"
	// PXIncrementalCountAnnotation is the annotation used to set cloud backup incremental count
	// for volume
	PXIncrementalCountAnnotation = "portworx.io/cloudsnap-incremental-count"
	// StorageClassAnnotationKey - Annotation key for storageClass
	StorageClassAnnotationKey = "volume.beta.kubernetes.io/storage-class"
	// StorageProvisionerAnnotationKey - Annotation key for storage provisioner
	StorageProvisionerAnnotationKey = "volume.beta.kubernetes.io/storage-provisioner"
	// trimCRDGroupNameKey - groups name containing the string from this configmap field will be trimmed
	trimCRDGroupNameKey = "TRIM_CRD_GROUP_NAME"
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
