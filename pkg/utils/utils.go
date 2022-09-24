package utils

import (
	"fmt"
	"github.com/aquilax/truncate"
	"k8s.io/apimachinery/pkg/util/validation"
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

	// PrefixBackup - prefix string that will be used for the kdmp backup job
	PrefixBackup = "backup"
	// PrefixRestore prefix string that will be used for the kdmp restore job
	PrefixRestore = "restore"

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
	// SkipResourceAnnotation - annotation value to skip resource during resource collector
	SkipResourceAnnotation = "stork.libopenstorage.org/skip-resource"
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
