package resourcecollector

import (
	"strings"

	"github.com/sirupsen/logrus"
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
	subjectWithNs := make([]rbacv1.Subject, 0)
	subjectWithoutNs := make([]rbacv1.Subject, 0)
	// Create list of rolebinding subjects that has namspace and pass it to updateSubject to
	// update destination namespace based on the namespace mapping.
	for _, subject := range rb.Subjects {
		if len(subject.Namespace) != 0 {
			subjectWithNs = append(subjectWithNs, subject)
		} else {
			subjectWithoutNs = append(subjectWithoutNs, subject)
		}
	}
	rb.Subjects = subjectWithNs
	rb.Subjects, err = r.updateSubjects(rb.Subjects, namespaceMappings)
	if err != nil {
		return err
	}
	rb.Subjects = append(rb.Subjects, subjectWithoutNs...)
	logrus.Infof("sivakumar --- update rb subjects %+v", rb.Subjects)
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&rb)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)

	return nil

}
