package storkctl

import (
	"fmt"
	"io"
	"strings"
	"time"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/utils/strings/slices"
)

func toTimeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC822)
}

func handleEmptyList(out io.Writer) {
	msg := "No resources found."
	printMsg(msg, out)
}

func printMsg(msg string, out io.Writer) {
	if _, printErr := fmt.Fprintln(out, msg); printErr != nil {
		fmt.Println(msg)
	}
}

func getDiscoveryClientForApiResources(cmdFactory Factory) (discovery.DiscoveryInterface, error) {
	config, err := cmdFactory.GetConfig()
	if err != nil {
		return nil, err
	}
	aeclient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("error getting apiextension client, %v", err)
	}

	return aeclient.Discovery(), nil
}

func isValidResourceType(resourceType string, apiResource metav1.APIResource) bool {
	resourceType = strings.ToLower(resourceType)
	// comparing all small cases
	if resourceType == strings.ToLower(apiResource.Name) ||
		resourceType == strings.ToLower(apiResource.Kind) ||
		resourceType == strings.ToLower(apiResource.SingularName) ||
		slices.Contains(apiResource.ShortNames, resourceType) {
		return true
	}
	return false
}
