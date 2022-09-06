package resourcecollector

import (
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/sirupsen/logrus"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	admissionv1beta1 "k8s.io/api/admissionregistration/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) prepareMutatingWebHookForApply(
	object runtime.Unstructured,
	namespaceMappings map[string]string,
) error {
	ok, err := version.RequiresV1Webhooks()
	if err != nil {
		return err
	}
	if ok {
		// v1 version
		var webhookCfg admissionv1.MutatingWebhookConfiguration
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &webhookCfg); err != nil {
			logrus.Errorf("mutatingWebHookToBeCollected: failed in getting mutating webhook: err %v", err)
			return err
		}
		for _, webhook := range webhookCfg.Webhooks {
			if webhook.ClientConfig.Service != nil {
				if destNamespace, ok := namespaceMappings[webhook.ClientConfig.Service.Namespace]; ok {
					// update the namespace with destination namespace
					webhook.ClientConfig.Service.Namespace = destNamespace
				}
			}
		}
		o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&webhookCfg)
		if err != nil {
			return err
		}
		object.SetUnstructuredContent(o)
		return nil
	}
	// v1beta1 version
	var webhookCfg admissionv1beta1.MutatingWebhookConfiguration
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &webhookCfg); err != nil {
		logrus.Errorf("mutatingWebHookToBeCollected: failed in getting mutating webhook: err %v", err)
		return err
	}
	for _, webhook := range webhookCfg.Webhooks {
		if webhook.ClientConfig.Service != nil {
			if destNamespace, ok := namespaceMappings[webhook.ClientConfig.Service.Namespace]; ok {
				// update the namespace with destination namespace
				webhook.ClientConfig.Service.Namespace = destNamespace
			}
		}
	}
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&webhookCfg)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)
	return nil
}

func (r *ResourceCollector) prepareValidatingWebHookForApply(
	object runtime.Unstructured,
	namespaceMappings map[string]string,
) error {
	ok, err := version.RequiresV1Webhooks()
	if err != nil {
		return err
	}
	if ok {
		// v1 version
		var webhookCfg admissionv1.ValidatingWebhookConfiguration
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &webhookCfg); err != nil {
			logrus.Errorf("validatingWebHookToBeCollected: failed in getting validating webhook: err %v", err)
			return err
		}
		for _, webhook := range webhookCfg.Webhooks {
			if webhook.ClientConfig.Service != nil {
				if destNamespace, ok := namespaceMappings[webhook.ClientConfig.Service.Namespace]; ok {

					// update the namespace with destination namespace
					webhook.ClientConfig.Service.Namespace = destNamespace
				}
			}
		}
		o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&webhookCfg)
		if err != nil {
			return err
		}
		object.SetUnstructuredContent(o)
		return nil
	}
	// v1beta1 version
	var webhookCfg admissionv1beta1.ValidatingWebhookConfiguration
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &webhookCfg); err != nil {
		logrus.Errorf("validatingWebHookToBeCollected: failed in getting validating webhook: err %v", err)
		return err
	}
	for _, webhook := range webhookCfg.Webhooks {
		if webhook.ClientConfig.Service != nil {
			if destNamespace, ok := namespaceMappings[webhook.ClientConfig.Service.Namespace]; ok {
				// update the namespace with destination namespace
				webhook.ClientConfig.Service.Namespace = destNamespace
			}
		}
	}
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&webhookCfg)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)
	return nil
}

func (r *ResourceCollector) validatingWebHookToBeCollected(
	object runtime.Unstructured,
	namespace string,
) (bool, error) {
	ok, err := version.RequiresV1Webhooks()
	if err != nil {
		return false, err
	}
	if ok {
		// v1 version
		var webhookCfg admissionv1.ValidatingWebhookConfiguration
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &webhookCfg); err != nil {
			logrus.Errorf("validatingWebHookToBeCollected: failed in getting validating webhook: err %v", err)
			return false, err
		}
		for _, webhook := range webhookCfg.Webhooks {
			if webhook.ClientConfig.Service != nil {
				if namespace == webhook.ClientConfig.Service.Namespace {
					return true, nil
				}
			}
		}
		return false, nil
	}
	// v1beta1 version
	var webhookCfg admissionv1beta1.ValidatingWebhookConfiguration
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &webhookCfg); err != nil {
		logrus.Errorf("validatingWebHookToBeCollected: failed in getting validating webhook: err %v", err)
		return false, err
	}
	for _, webhook := range webhookCfg.Webhooks {
		if webhook.ClientConfig.Service != nil {
			if namespace == webhook.ClientConfig.Service.Namespace {
				return true, nil
			}
		}
	}
	return false, nil
}

func (r *ResourceCollector) mutatingWebHookToBeCollected(
	object runtime.Unstructured,
	namespace string,
) (bool, error) {
	ok, err := version.RequiresV1Webhooks()
	if err != nil {
		return false, err
	}
	if ok {
		// v1 version
		var webhookCfg admissionv1.MutatingWebhookConfiguration
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &webhookCfg); err != nil {
			logrus.Errorf("mutatingWebHookToBeCollected: failed in getting mutating webhook: err %v", err)
			return false, err
		}
		for _, webhook := range webhookCfg.Webhooks {
			if webhook.ClientConfig.Service != nil {
				if namespace == webhook.ClientConfig.Service.Namespace {
					return true, nil
				}
			}
		}
		return false, nil
	}
	// v1beta1 version
	var webhookCfg admissionv1beta1.MutatingWebhookConfiguration
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &webhookCfg); err != nil {
		logrus.Errorf("mutatingWebHookToBeCollected: failed in getting mutating webhook: err %v", err)
		return false, err
	}
	for _, webhook := range webhookCfg.Webhooks {
		if webhook.ClientConfig.Service != nil {
			if namespace == webhook.ClientConfig.Service.Namespace {
				return true, nil
			}
		}
	}
	return false, nil
}
