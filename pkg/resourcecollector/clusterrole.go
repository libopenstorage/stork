package resourcecollector

import (
	"strings"

	"github.com/portworx/sched-ops/k8s"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
)

// Checks if the subject is in the specified namespace
func (r *ResourceCollector) subjectInNamespace(subject *rbacv1.Subject, namespace string) (bool, error) {
	switch subject.Kind {
	case rbacv1.ServiceAccountKind:
		// For ServiceAccount the namespace is in the spec
		if subject.Namespace == namespace {
			return true, nil
		}
	case rbacv1.UserKind:
		// For User we need to parse the username to get the namespace
		userNamespace, _, err := serviceaccount.SplitUsername(subject.Name)
		if err != nil {
			return false, nil
		}
		if userNamespace == namespace {
			return true, nil
		}
	case rbacv1.GroupKind:
		// For Group  we need to parse the username to get the namespace
		groupNamespace := strings.TrimPrefix(subject.Name, serviceaccount.ServiceAccountUsernamePrefix)
		if groupNamespace == namespace {
			return true, nil
		}
	}
	return false, nil
}

func (r *ResourceCollector) clusterRoleBindingToBeCollected(
	labelSelectors map[string]string,
	object runtime.Unstructured,
	namespace string,
) (bool, error) {
	var crb rbacv1.ClusterRoleBinding
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &crb); err != nil {
		return false, err
	}
	if val, ok := crb.Labels["kubernetes.io/bootstrapping"]; ok && val == "rbac-defaults" {
		return false, nil
	}
	// Check if there is a subject for the namespace which is requested
	for _, subject := range crb.Subjects {
		collect, err := r.subjectInNamespace(&subject, namespace)
		if err != nil || collect {
			return collect, err
		}
	}
	return false, nil
}

func (r *ResourceCollector) clusterRoleToBeCollected(
	labelSelectors map[string]string,
	object runtime.Unstructured,
	crbs *rbacv1.ClusterRoleBindingList,
	namespace string,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}
	if val, ok := metadata.GetLabels()["kubernetes.io/bootstrapping"]; ok && val == "rbac-defaults" {
		return false, nil
	}

	name := metadata.GetName()
	// Find the corresponding ClusterRoleBinding and see if it belongs to the requested namespace
	for _, crb := range crbs.Items {
		if crb.RoleRef.Name == name {
			for _, subject := range crb.Subjects {
				collect, err := r.subjectInNamespace(&subject, namespace)
				if err != nil || collect {
					return collect, err
				}
			}
		}
	}
	return false, nil
}

func (r *ResourceCollector) prepareClusterRoleBindingForCollection(
	object runtime.Unstructured,
	namespaces []string,
) error {
	var crb rbacv1.ClusterRoleBinding
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &crb); err != nil {
		return err
	}
	subjectsToCollect := make([]rbacv1.Subject, 0)
	// Check if there is a subject for the namespace which is requested
	for _, subject := range crb.Subjects {
		for _, ns := range namespaces {
			collect, err := r.subjectInNamespace(&subject, ns)
			if err != nil {
				return err
			}

			if collect {
				subjectsToCollect = append(subjectsToCollect, subject)
			}
		}
	}
	crb.Subjects = subjectsToCollect
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&crb)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)

	return nil
}

func (r *ResourceCollector) prepareClusterRoleBindingForApply(
	object runtime.Unstructured,
	namespaceMappings map[string]string,
) error {
	var crb rbacv1.ClusterRoleBinding
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &crb); err != nil {
		return err
	}
	subjectsToApply := make([]rbacv1.Subject, 0)
	for sourceNamespace, destNamespace := range namespaceMappings {
		// Check if there is a subject for the namespace which is requested
		for _, subject := range crb.Subjects {
			collect, err := r.subjectInNamespace(&subject, sourceNamespace)
			if err != nil {
				return err
			}
			if !collect {
				continue
			}

			switch subject.Kind {
			case rbacv1.UserKind:
				_, username, err := serviceaccount.SplitUsername(subject.Name)
				if err != nil {
					return err
				}
				// Regnerate the Username for the destination namespace
				subject.Name = serviceaccount.MakeUsername(destNamespace, username)
			case rbacv1.GroupKind:
				// Regnerate the Group name for the destination namespace
				subject.Name = serviceaccount.MakeNamespaceGroupName(destNamespace)
			case rbacv1.ServiceAccountKind:
				// Update the Namespace in the spec for ServiceAccounts
				subject.Namespace = destNamespace
			}
			subjectsToApply = append(subjectsToApply, subject)
		}
	}
	crb.Subjects = subjectsToApply
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&crb)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)

	return nil
}

func (r *ResourceCollector) mergeAndUpdateClusterRoleBinding(
	object runtime.Unstructured,
) error {
	var newCRB rbacv1.ClusterRoleBinding
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &newCRB); err != nil {
		return err
	}

	currentCRB, err := k8s.Instance().GetClusterRoleBinding(newCRB.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, err = k8s.Instance().CreateClusterRoleBinding(&newCRB)
		}
		return err
	}

	// Map which will help eliminate duplicate subjects since the subject string
	// will be unique for different subjects
	updatedSubjects := make(map[string]rbacv1.Subject)

	// First add the current subjects to the map
	for _, subject := range currentCRB.Subjects {
		updatedSubjects[subject.String()] = subject
	}
	// Then add the new subjects to be merged
	for _, subject := range newCRB.Subjects {
		updatedSubjects[subject.String()] = subject
	}

	// Then get the updated list of subjects from the map to apply
	currentCRB.Subjects = make([]rbacv1.Subject, 0)
	for _, subject := range updatedSubjects {
		currentCRB.Subjects = append(currentCRB.Subjects, subject)
	}

	_, err = k8s.Instance().UpdateClusterRoleBinding(currentCRB)
	return err
}
