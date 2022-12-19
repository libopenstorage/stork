package applicationbackup

import (
	"fmt"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/pkg/log"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	configMapName                          = "secret-configmap"
	backupLocationType                     = storkv1.BackupLocationS3
	backupLocationPath                     = "testpath"
	s3SecretName                           = "s3secret"
	applicationBackupScheduleRetryInterval = 10 * time.Second
	applicationBackupScheduleRetryTimeout  = 5 * time.Minute
)

func CreateBackupLocation(
	name string,
	namespace string,
	secretName string,
) (*storkv1.BackupLocation, error) {
	log.Infof("Using backup location type as %v", backupLocationType)
	secretObj, err := core.Instance().GetSecret(secretName, "default")
	if err != nil {
		return nil, fmt.Errorf("secret %v is not present in default namespace", secretName)
	}
	// copy secret to the app namespace
	newSecretObj := secretObj.DeepCopy()
	newSecretObj.Namespace = namespace
	newSecretObj.ResourceVersion = ""
	_, err = core.Instance().CreateSecret(newSecretObj)
	if err != nil {
		return nil, fmt.Errorf("failed to copy secret %v in required namespace %v", secretName, namespace)
	}
	if err != nil {
		return nil, fmt.Errorf("secret %v is not getting created  in default namespace", secretName)
	}
	backupLocation := &storkv1.BackupLocation{
		ObjectMeta: meta.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{"stork.libopenstorage.ord/skipresource": "true"},
		},
		Location: storkv1.BackupLocationItem{
			Type:         backupLocationType,
			Path:         backupLocationPath,
			SecretConfig: secretObj.Name,
		},
	}

	backupLocation, err = storkops.Instance().CreateBackupLocation(backupLocation)
	if err != nil {
		return nil, err
	}

	// Doing a "Get" on the backuplocation created to add any missing info from the secrets,
	// that might be required to later get buckets from the cloud objectstore
	backupLocation, err = storkops.Instance().GetBackupLocation(backupLocation.Name, backupLocation.Namespace)
	if err != nil {
		return nil, err
	}
	return backupLocation, nil
}

func CreateApplicationBackup(
	name string,
	namespace string,
	backupLocation *storkv1.BackupLocation,
) (*storkv1.ApplicationBackup, error) {

	appBackup := &storkv1.ApplicationBackup{
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: storkv1.ApplicationBackupSpec{
			Namespaces:     []string{namespace},
			BackupLocation: backupLocation.Name,
		},
	}

	return storkops.Instance().CreateApplicationBackup(appBackup)
}

func WaitForAppBackupCompletion(name, namespace string, timeout time.Duration) error {
	getAppBackup := func() (interface{}, bool, error) {
		appBackup, err := storkops.Instance().GetApplicationBackup(name, namespace)
		if err != nil {
			return "", false, err
		}

		if appBackup.Status.Status != storkv1.ApplicationBackupStatusSuccessful {
			return "", true, fmt.Errorf("app backups %s in %s not complete yet.Retrying", name, namespace)
		}
		return "", false, nil
	}
	_, err := task.DoRetryWithTimeout(getAppBackup, timeout, applicationBackupScheduleRetryInterval)
	return err

}

func WaitForAppBackupToStart(name, namespace string, timeout time.Duration) error {
	getAppBackup := func() (interface{}, bool, error) {
		appBackup, err := storkops.Instance().GetApplicationBackup(name, namespace)
		if err != nil {
			return "", false, err
		}

		if appBackup.Status.Status != storkv1.ApplicationBackupStatusInProgress {
			return "", true, fmt.Errorf("App backups %s in %s has not started yet.Retrying Status: %s", name, namespace, appBackup.Status.Status)
		}
		return "", false, nil
	}
	_, err := task.DoRetryWithTimeout(getAppBackup, timeout, applicationBackupScheduleRetryInterval)
	return err
}
