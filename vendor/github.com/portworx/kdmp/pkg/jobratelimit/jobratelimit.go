package jobratelimit

import (
	"fmt"
	"strconv"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	log "github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// BackupJobLimitKey - backup job limit configmap key
	BackupJobLimitKey = "KDMP_BACKUP_JOB_LIMIT"
	// RestoreJobLimitKey - restore job limit configmap key
	RestoreJobLimitKey = "KDMP_RESTORE_JOB_LIMIT"
	// DeleteJobLimitKey = delete job limit configmap key
	DeleteJobLimitKey = "KDMP_DELETE_JOB_LIMIT"
	// MaintenanceJobLimitKey - maintenance job limit configmap key
	MaintenanceJobLimitKey = "KDMP_MAINTENANCE_JOB_LIMIT"
	// PvcNameKey - PVC name label key
	PvcNameKey = "kdmp.portworx.com/pvc-name"
	// PvcUIDKey - PVC UID label key
	PvcUIDKey = "kdmp.portworx.com/pvc-uid"
	// DefaultBackupJobLimit - default backup job limit value
	DefaultBackupJobLimit = 5
	// DefaultRestoreJobLimit - default restore job limit value
	DefaultRestoreJobLimit = 5
	// DefaultDeleteJobLimit - default delete job limit value
	DefaultDeleteJobLimit = 5
	// DefaultMaintenanceJobLimit - default maintenance job limit value
	DefaultMaintenanceJobLimit = 5
	// DefaultJobLimit - default job limit value
	DefaultJobLimit = 5
)

// getJobLimitConfigmapKey - Takes drivertype name and gives the job limit configmap
func getJobLimitConfigmapKey(driverName string) (string, error) {
	switch driverName {
	case drivers.KopiaBackup:
		return BackupJobLimitKey, nil
	case drivers.KopiaRestore:
		return RestoreJobLimitKey, nil
	case drivers.KopiaDelete:
		return DeleteJobLimitKey, nil
	case drivers.KopiaMaintenance:
		return MaintenanceJobLimitKey, nil
	default:
		return "", fmt.Errorf("invalid driver name %v", driverName)
	}
}

// getJobCountByType takes the jobType as a param and returns the count of jobs matching the label in all namespaces
func getJobCountByType(jobType string) (int, error) {

	labelSelector := drivers.DriverNameLabel + "=" + jobType
	options := metav1.ListOptions{
		LabelSelector: labelSelector,
	}
	getAllNamespaces := getAllNamespaces()
	var count int
	for _, item := range getAllNamespaces.Items {
		jobCount := 0
		allJobs, err := batch.Instance().ListAllJobs(item.Name, options)
		if err != nil {
			errMsg := fmt.Sprintf("failed to get job list: %v", err)
			log.Errorf("%v", errMsg)
			return 0, fmt.Errorf("%v", errMsg)
		}
		jobCount = len(allJobs.Items)
		// Check if any of the job is already in completed state.
		// If complemented, exclude them from counting.
		for _, job := range allJobs.Items {
			for _, condition := range job.Status.Conditions {
				if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
					jobCount = jobCount - 1
					break
				}
			}
		}
		count = count + jobCount
	}
	return count, nil
}

func getAllNamespaces() *corev1.NamespaceList {
	labelSelector := map[string]string{}
	allNameSpaces, err := core.Instance().ListNamespaces(labelSelector)
	if err != nil {
		log.Errorf("failed to list all namespace: %s", err)
	}
	return allNameSpaces
}

func getDefaultJobLimit(jobType string) int {
	switch jobType {
	case drivers.KopiaBackup:
		return DefaultBackupJobLimit
	case drivers.KopiaRestore:
		return DefaultRestoreJobLimit
	case drivers.KopiaDelete:
		return DefaultDeleteJobLimit
	case drivers.KopiaMaintenance:
		return DefaultMaintenanceJobLimit
	default:
		log.Warnf("unsupported job type [%v]", jobType)
		return DefaultJobLimit
	}
}

// jobLimitByType takes the job type and fetches the value from the config map
func jobLimitByType(jobType string) int {
	configmapKey, err := getJobLimitConfigmapKey(jobType)
	if err != nil {
		log.Warnf("unsupported job type [%v]", jobType)
		return DefaultJobLimit
	}
	value := utils.GetConfigValue(configmapKey)
	if value == "" {
		return getDefaultJobLimit(jobType)
	}
	jobLimit, err := strconv.Atoi(value)
	// if its not found in configmap the strconv will through error
	if err != nil {
		log.Errorf("error in converting job limit for job type [%v] from configmap: %v", jobType, err)
		return getDefaultJobLimit(jobType)
	}
	return jobLimit
}

// CanJobBeScheduled takes the jobType and returns whether the given job can run or not based on limit set
func CanJobBeScheduled(jobType string) (bool, error) {
	jobCount, err := getJobCountByType(jobType)
	if err != nil {
		return false, err
	}
	jobLimitCount := jobLimitByType(jobType)
	if jobCount >= jobLimitCount {
		return false, nil
	}
	return true, nil
}

// IsJobForPvcAlreadyRunning - Check whether there is job already running for the given PVC
func IsJobForPvcAlreadyRunning(pvcName, pvcNamespace, pvcUID, jobType string) (bool, error) {
	driverNameLabel := drivers.DriverNameLabel + "=" + jobType
	pvcNameLabel := PvcNameKey + "=" + pvcName
	pvcUIDLabel := PvcUIDKey + "=" + pvcUID
	labelSelector := driverNameLabel + "," + pvcNameLabel + "," + pvcUIDLabel
	options := metav1.ListOptions{
		LabelSelector: labelSelector,
	}
	allJobs, err := batch.Instance().ListAllJobs(pvcNamespace, options)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get job list: %v", err)
		log.Errorf("%v", errMsg)
		return true, fmt.Errorf("%v", errMsg)
	}
	if len(allJobs.Items) == 0 {
		return false, nil
	}
	return true, nil
}

// IsJobAlreadyPresent - will check whether the given job is already present.
func IsJobAlreadyPresent(name, namespace string) bool {
	_, err := batch.Instance().GetJob(name, namespace)
	return err == nil
}
