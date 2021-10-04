package utils

import (
	"fmt"

	coreops "github.com/portworx/sched-ops/k8s/core"
	rbacops "github.com/portworx/sched-ops/k8s/rbac"
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
)

// SetupServiceAccount create a service account and bind it to a provided role.
func SetupServiceAccount(name, namespace string, role *rbacv1.Role) error {
	if role != nil {
		role.Name, role.Namespace = name, namespace
		if _, err := rbacops.Instance().CreateRole(role); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("create %s/%s role: %s", namespace, name, err)
		}
		if _, err := rbacops.Instance().CreateRoleBinding(roleBindingFor(name, namespace)); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("create %s/%s rolebinding: %s", namespace, name, err)
		}
	}
	if _, err := coreops.Instance().CreateServiceAccount(serviceAccountFor(name, namespace)); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s/%s serviceaccount: %s", namespace, name, err)
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
		},
	}
}

// FrameCredSecretName frames credential secret name
func FrameCredSecretName(prefix, deName string) string {
	return prefix + "-" + deName
}
