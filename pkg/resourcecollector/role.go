package resourcecollector

import (
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) roleBindingToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}

	name := metadata.GetName()
	return !strings.HasPrefix(name, "system:"), nil
}

func (r *ResourceCollector) roleToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}

	name := metadata.GetName()
	return !strings.HasPrefix(name, "system:"), nil
}

func (r *ResourceCollector) prepareRoleBindingForApply(
	object runtime.Unstructured,
	namespaceMappings map[string]string,
) error {
	var rb rbacv1.RoleBinding
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &rb)
	if err != nil {
		return err
	}
	rb.Subjects, err = r.updateSubjects(rb.Subjects, namespaceMappings)
	if err != nil {
		return err
	}
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&rb)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)

	return nil

}
