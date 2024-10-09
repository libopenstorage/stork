package resourcecollector

import (
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	controllerUIDLabels = []string{"controller-uid", "batch.kubernetes.io/controller-uid"}
)

func (r *ResourceCollector) prepareJobForCollection(object runtime.Unstructured) error {
	var job batchv1.Job
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &job); err != nil {
		return err
	}

	if job.Labels != nil {
		for _, label := range controllerUIDLabels {
			delete(job.Labels, label)
		}
	}
	if job.Spec.Selector != nil && job.Spec.Selector.MatchLabels != nil {
		for _, label := range controllerUIDLabels {
			delete(job.Spec.Selector.MatchLabels, label)
		}
	}
	if job.Spec.Template.Labels != nil {
		for _, label := range controllerUIDLabels {
			delete(job.Spec.Template.Labels, label)
		}
	}
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&job)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)

	return nil
}
