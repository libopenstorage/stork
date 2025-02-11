package resourcecollector

import (
	elasticsearchv1 "github.com/elastic/cloud-on-k8s/v2/pkg/apis/elasticsearch/v1"

	"github.com/sirupsen/logrus"
	//	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) prepareElasticsearchForApply(
	object runtime.Unstructured,
	namespaceMappings map[string]string,
) error {
	var es elasticsearchv1.Elasticsearch
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &es)
	if err != nil {
		return err
	}
	if es.ObjectMeta.Annotations != nil {
		// delete(es.ObjectMeta.Annotations, "eck.k8s.elastic.co/orchestration-hints")
		es.ObjectMeta.Annotations = nil
	}
	//csv.ObjectMeta.Annotations["olm.targetNamespaces"] = namespaceMappings[csv.ObjectMeta.Namespace]
	// csv.ObjectMeta.Annotations["olm.operatorNamespace"] = namespaceMappings[csv.ObjectMeta.Namespace]
	/*og.Spec.TargetNamespaces = []string{og.ObjectMeta.Namespace}
	logrus.Infof("sivakumar --------- og.Spec.TargetNamespaces %v", og.Spec.TargetNamespaces)
	logrus.Infof("sivakumar -------> meta %+v", og.ObjectMeta)
	og.ObjectMeta.CreationTimestamp = metav1.Time{}
	og.Status.LastUpdated = &metav1.Time{}
	*/
	logrus.Infof("sivakumar --- es.ObjectMeta.Annotations %v", es.ObjectMeta.Annotations)
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&es)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)

	return nil

}
