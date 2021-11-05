package utils

import (
	"fmt"
	"time"

	coreops "github.com/portworx/sched-ops/k8s/core"
	rbacops "github.com/portworx/sched-ops/k8s/rbac"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// BackupJobPrefix job prefix for backup jobs
	BackupJobPrefix = "b"
	// RestoreJobPrefix job prefix for restore jobs
	RestoreJobPrefix = "r"
	// SkipResourceAnnotation skipping kopia secret to be backed up
	SkipResourceAnnotation = "stork.libopenstorage.org/skip-resource"
	// BackupObjectNameKey - label key to store backup object name
	BackupObjectNameKey = "backup-object-name"
	// BackupObjectUIDKey - label key to store backup object uid
	BackupObjectUIDKey = "backup-object-uid"
	// TLSCertMountVol mount vol name for tls certificate secret
	TLSCertMountVol       = "tls-secret"
	defaultTimeout        = 1 * time.Minute
	progressCheckInterval = 5 * time.Second
	// KdmpConfigmapName kdmp config map name
	KdmpConfigmapName = "kdmp-config"
	// KdmpConfigmapNamespace kdmp config map ns
	KdmpConfigmapNamespace = "kube-system"
	// DefaultCompresion default compression type
	DefaultCompresion = "s2-parallel-8"
)

// SetupServiceAccount create a service account and bind it to a provided role.
func SetupServiceAccount(name, namespace string, role *rbacv1.Role) error {
	if role != nil {
		role.Name, role.Namespace = name, namespace
		role.Annotations = map[string]string{
			SkipResourceAnnotation: "true",
		}
		if _, err := rbacops.Instance().CreateRole(role); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("create %s/%s role: %s", namespace, name, err)
		}
		if _, err := rbacops.Instance().CreateRoleBinding(roleBindingFor(name, namespace)); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("create %s/%s rolebinding: %s", namespace, name, err)
		}
	}
	var sa *corev1.ServiceAccount
	var err error
	if sa, err = coreops.Instance().CreateServiceAccount(serviceAccountFor(name, namespace)); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s/%s serviceaccount: %s", namespace, name, err)
	}
	t := func() (interface{}, bool, error) {
		sa, err = coreops.Instance().GetServiceAccount(name, namespace)
		if err != nil {
			errMsg := fmt.Sprintf("failed fetching sa [%v/%v]: %v", name, namespace, err)
			logrus.Tracef("%v", errMsg)
			return "", true, fmt.Errorf("%v", errMsg)
		}
		if sa.Secrets == nil {
			errMsg := fmt.Sprintf("secret token is missing in sa [%v/%v]", name, namespace)
			return "", true, fmt.Errorf("%v", errMsg)
		}
		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, defaultTimeout, progressCheckInterval); err != nil {
		errMsg := fmt.Sprintf("max retries done, failed in fetching secret token of sa [%v/%v]: %v ", name, namespace, err)
		logrus.Errorf("%v", errMsg)
		// Exhausted all retries
		return err
	}

	tokenName := sa.Secrets[0].Name
	secretToken, err := coreops.Instance().GetSecret(tokenName, namespace)
	if err != nil {
		return fmt.Errorf("failed in getting secretToken [%v] of service account [%v/%v]: %v", tokenName, name, namespace, err)
	}
	secretToken.Annotations[SkipResourceAnnotation] = "true"
	_, err = coreops.Instance().UpdateSecret(secretToken)
	if err != nil {
		return fmt.Errorf("failed in updating the secretToken [%v] of service account [%v/%v]: %v", tokenName, name, namespace, err)
	}
	return nil
}

// CleanServiceAccount removes a service account with a corresponding role and rolebinding.
func CleanServiceAccount(name, namespace string) error {
	if err := rbacops.Instance().DeleteRole(name, namespace); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s role: %s", namespace, name, err)
	}
	if err := rbacops.Instance().DeleteRoleBinding(name, namespace); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s rolebinding: %s", namespace, name, err)
	}
	if err := coreops.Instance().DeleteServiceAccount(name, namespace); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s serviceaccount: %s", namespace, name, err)
	}
	return nil
}

func roleBindingFor(name, namespace string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				SkipResourceAnnotation: "true",
			},
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      name,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Name:     name,
			Kind:     "Role",
			APIGroup: rbacv1.GroupName,
		},
	}
}

func serviceAccountFor(name, namespace string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				SkipResourceAnnotation: "true",
			},
		},
	}
}
