package k8s

import (
	"fmt"
	"os"

	prometheusclient "github.com/coreos/prometheus-operator/pkg/client/versioned"
	snap_client "github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	autopilotclientset "github.com/libopenstorage/autopilot-api/pkg/client/clientset/versioned"
	ostclientset "github.com/libopenstorage/operator/pkg/client/clientset/versioned"
	storkclientset "github.com/libopenstorage/stork/pkg/client/clientset/versioned"
	ocp_clientset "github.com/openshift/client-go/apps/clientset/versioned"
	ocp_security_clientset "github.com/openshift/client-go/security/clientset/versioned"
	talismanclientset "github.com/portworx/talisman/pkg/client/clientset/versioned"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// ClientSetter is an interface to allow setting different clients on the Ops object
type ClientSetter interface {
	// SetConfig sets the config and resets the client
	SetConfig(config *rest.Config)
	// SetConfigFromPath sets the config from a kubeconfig file
	SetConfigFromPath(configPath string) error
	// SetClient set the k8s clients
	SetClient(
		kubernetes.Interface,
		rest.Interface,
		storkclientset.Interface,
		apiextensionsclient.Interface,
		dynamic.Interface,
		ocp_clientset.Interface,
		ocp_security_clientset.Interface,
		autopilotclientset.Interface,
	)
	// SetBaseClient sets the kubernetes clientset
	SetBaseClient(kubernetes.Interface)
	// SetSnapshotClient sets the snapshot clientset
	SetSnapshotClient(rest.Interface)
	// SetStorkClient sets the stork clientset
	SetStorkClient(storkclientset.Interface)
	// SetOpenstorageOperatorClient sets the openstorage operator clientset
	SetOpenstorageOperatorClient(ostclientset.Interface)
	// SetAPIExtensionsClient sets the api extensions clientset
	SetAPIExtensionsClient(apiextensionsclient.Interface)
	// SetDynamicClient sets the dynamic clientset
	SetDynamicClient(dynamic.Interface)
	// SetOpenshiftAppsClient sets the openshift apps clientset
	SetOpenshiftAppsClient(ocp_clientset.Interface)
	// SetOpenshiftSecurityClient sets the openshift security clientset
	SetOpenshiftSecurityClient(ocp_security_clientset.Interface)
	// SetTalismanClient sets the talisman clientset
	SetTalismanClient(talismanclientset.Interface)
	// SetAutopilotClient sets the autopilot clientset
	SetAutopilotClient(autopilotclientset.Interface)
	// SetPrometheusClient sets the prometheus clientset
	SetPrometheusClient(prometheusclient.Interface)
}

// SetConfig sets the config and resets the client
func (k *k8sOps) SetConfig(config *rest.Config) {
	k.config = config
	k.client = nil
}

// SetConfigFromPath takes the path to a kubeconfig file
// and then internally calls SetConfig to set it
func (k *k8sOps) SetConfigFromPath(configPath string) error {
	if configPath == "" {
		k.SetConfig(nil)
		return nil
	}
	config, err := clientcmd.BuildConfigFromFlags("", configPath)
	if err != nil {
		return err
	}

	k.SetConfig(config)
	return nil
}

// SetClient set the k8s clients
func (k *k8sOps) SetClient(
	client kubernetes.Interface,
	snapClient rest.Interface,
	storkClient storkclientset.Interface,
	apiExtensionClient apiextensionsclient.Interface,
	dynamicInterface dynamic.Interface,
	ocpClient ocp_clientset.Interface,
	ocpSecurityClient ocp_security_clientset.Interface,
	autopilotClient autopilotclientset.Interface,
) {
	k.client = client
	k.snapClient = snapClient
	k.storkClient = storkClient
	k.apiExtensionClient = apiExtensionClient
	k.dynamicInterface = dynamicInterface
	k.ocpClient = ocpClient
	k.ocpSecurityClient = ocpSecurityClient
	k.autopilotClient = autopilotClient
}

// SetBaseClient sets the kubernetes clientset
func (k *k8sOps) SetBaseClient(client kubernetes.Interface) {
	k.client = client
}

// SetSnapshotClient sets the snapshot clientset
func (k *k8sOps) SetSnapshotClient(snapClient rest.Interface) {
	k.snapClient = snapClient
}

// SetStorkClient sets the stork clientset
func (k *k8sOps) SetStorkClient(storkClient storkclientset.Interface) {
	k.storkClient = storkClient
}

// SetOpenstorageOperatorClient sets the openstorage operator clientset
func (k *k8sOps) SetOpenstorageOperatorClient(ostClient ostclientset.Interface) {
	k.ostClient = ostClient
}

// SetAPIExtensionsClient sets the api extensions clientset
func (k *k8sOps) SetAPIExtensionsClient(apiExtensionsClient apiextensionsclient.Interface) {
	k.apiExtensionClient = apiExtensionsClient
}

// SetDynamicClient sets the dynamic clientset
func (k *k8sOps) SetDynamicClient(dynamicClient dynamic.Interface) {
	k.dynamicInterface = dynamicClient
}

// SetOpenshiftAppsClient sets the openshift apps clientset
func (k *k8sOps) SetOpenshiftAppsClient(ocpAppsClient ocp_clientset.Interface) {
	k.ocpClient = ocpAppsClient
}

// SetOpenshiftSecurityClient sets the openshift security clientset
func (k *k8sOps) SetOpenshiftSecurityClient(ocpSecurityClient ocp_security_clientset.Interface) {
	k.ocpSecurityClient = ocpSecurityClient
}

// SetAutopilotClient sets the autopilot clientset
func (k *k8sOps) SetAutopilotClient(autopilotClient autopilotclientset.Interface) {
	k.autopilotClient = autopilotClient
}

// SetTalismanClient sets the talisman clientset
func (k *k8sOps) SetTalismanClient(talismanClient talismanclientset.Interface) {
	k.talismanClient = talismanClient
}

// SetPrometheusClient sets the prometheus clientset
func (k *k8sOps) SetPrometheusClient(prometheusClient prometheusclient.Interface) {
	k.prometheusClient = prometheusClient
}

// initK8sClient the k8s client if uninitialized
func (k *k8sOps) initK8sClient() error {
	if k.client == nil {
		err := k.setK8sClient()
		if err != nil {
			return err
		}

		// Quick validation if client connection works
		_, err = k.client.Discovery().ServerVersion()
		if err != nil {
			return fmt.Errorf("failed to connect to k8s server: %s", err)
		}

	}
	return nil
}

// setK8sClient instantiates a k8s client
func (k *k8sOps) setK8sClient() error {
	var err error

	if k.config != nil {
		err = k.loadClientFor(k.config)
	} else {
		kubeconfig := os.Getenv("KUBECONFIG")
		if len(kubeconfig) > 0 {
			err = k.loadClientFromKubeconfig(kubeconfig)
		} else {
			err = k.loadClientFromServiceAccount()
		}

	}
	if err != nil {
		return err
	}

	if k.client == nil {
		return ErrK8SApiAccountNotSet
	}

	return nil
}

// loadClientFromServiceAccount loads a k8s client from a ServiceAccount specified in the pod running px
func (k *k8sOps) loadClientFromServiceAccount() error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	k.config = config
	return k.loadClientFor(config)
}

func (k *k8sOps) loadClientFromKubeconfig(kubeconfig string) error {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return err
	}

	k.config = config
	return k.loadClientFor(config)
}

func (k *k8sOps) loadClientFromConfigBytes(kubeconfig []byte) error {
	config, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return err
	}

	k.config = config
	return k.loadClientFor(config)
}

func (k *k8sOps) loadClientFor(config *rest.Config) error {
	var err error
	k.client, err = kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	k.snapClient, _, err = snap_client.NewClient(config)
	if err != nil {
		return err
	}

	k.storkClient, err = storkclientset.NewForConfig(config)
	if err != nil {
		return err
	}

	k.ostClient, err = ostclientset.NewForConfig(config)
	if err != nil {
		return err
	}

	k.talismanClient, err = talismanclientset.NewForConfig(config)
	if err != nil {
		return err
	}

	k.apiExtensionClient, err = apiextensionsclient.NewForConfig(config)
	if err != nil {
		return err
	}

	k.dynamicInterface, err = dynamic.NewForConfig(config)
	if err != nil {
		return err
	}

	k.ocpClient, err = ocp_clientset.NewForConfig(config)
	if err != nil {
		return err
	}

	k.ocpSecurityClient, err = ocp_security_clientset.NewForConfig(config)
	if err != nil {
		return err
	}

	k.autopilotClient, err = autopilotclientset.NewForConfig(config)
	if err != nil {
		return err
	}

	k.prometheusClient, err = prometheusclient.NewForConfig(config)
	if err != nil {
		return err
	}

	return nil
}
