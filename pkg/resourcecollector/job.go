package resourcecollector

import (
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	controllerUIDLabel = "controller-uid"
)

func (r *ResourceCollector) prepareJobForCollection(
	object runtime.Unstructured,
	namespaces []string,
) error {
	var job batchv1.Job
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &job); err != nil {
		return err
	}

	if job.Labels != nil {
		delete(job.Labels, controllerUIDLabel)
	}
	if job.Spec.Selector != nil && job.Spec.Selector.MatchLabels != nil {
		delete(job.Spec.Selector.MatchLabels, controllerUIDLabel)
	}
	if job.Spec.Template.Labels != nil {
		delete(job.Spec.Template.Labels, controllerUIDLabel)
	}
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&job)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)

	return nil
}
