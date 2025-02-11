package resourcecollector

import (
	operatorsv1 "github.com/operator-framework/api/pkg/operators/v1"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) prepareOgForApply(
	object runtime.Unstructured,
	namespaceMappings map[string]string,
) error {
	var og operatorsv1.OperatorGroup
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &og)
	if err != nil {
		return err
	}
	//csv.ObjectMeta.Annotations["olm.targetNamespaces"] = namespaceMappings[csv.ObjectMeta.Namespace]
	// csv.ObjectMeta.Annotations["olm.operatorNamespace"] = namespaceMappings[csv.ObjectMeta.Namespace]
	og.Spec.TargetNamespaces = []string{og.ObjectMeta.Namespace}
	logrus.Infof("sivakumar --------- og.Spec.TargetNamespaces %v", og.Spec.TargetNamespaces)
	logrus.Infof("sivakumar -------> meta %+v", og.ObjectMeta)
	og.ObjectMeta.CreationTimestamp = metav1.Time{}
	og.Status.LastUpdated = &metav1.Time{}

	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&og)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)

	return nil

}
