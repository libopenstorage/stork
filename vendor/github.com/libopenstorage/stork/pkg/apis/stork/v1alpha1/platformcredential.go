package v1alpha1

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// PlatformCredentialResourceName is name for "platformcredential" resource
	PlatformCredentialResourceName = "platformcredential"
	// PlatformCredentialResourcePlural is plural for "platformcredentials" resource
	PlatformCredentialResourcePlural = "platformcredentials"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PlatformCredential represents a platformcredential object
type PlatformCredential struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PlatformCredentialSpec `json:"spec"`
	Cluster           ClusterItem            `json:"cluster"`
}

type PlatformCredentialSpec struct {
	Type          PlatformCredentialType `json:"type"`
	RancherConfig *RancherConfig         `json:"rancherConfig,omitempty"`
	SecretConfig  string                 `json:"secretConfig"`
}

// PlatformCredentialType is the type of the backup location
type PlatformCredentialType string

const (
	// PlatformCredentialRancher
	PlatformCredentialRancher PlatformCredentialType = "rancher"
)

// RancherConfig specifies the config required to connect to a Rancher API Server
type RancherConfig struct {
	Endpoint string `json:"endpoint"`
	Token    string `json:"token"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PlatformCredentialList is a list of ApplicationBackups
type PlatformCredentialList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []PlatformCredential `json:"items"`
}

// UpdateFromSecret updated the config information from the secret if not provided inline
func (pc *PlatformCredential) UpdateFromSecret(client kubernetes.Interface) error {

	switch pc.Spec.Type {
	case PlatformCredentialRancher:
		return pc.getMergedRancherConfig(client)
	default:
		return fmt.Errorf("invalid PlatformCredential type %v", pc.Spec.Type)
	}

}

func (pc *PlatformCredential) getMergedRancherConfig(client kubernetes.Interface) error {
	if pc.Spec.SecretConfig != "" {
		secretConfig, err := client.CoreV1().Secrets(pc.Namespace).Get(context.TODO(), pc.Spec.SecretConfig, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error getting secretConfig for platformcredential: %v", err)
		}
		if val, ok := secretConfig.Data["endpoint"]; ok && val != nil {
			pc.Spec.RancherConfig.Endpoint = strings.TrimSuffix(string(val), "\n")
		}
		if val, ok := secretConfig.Data["token"]; ok && val != nil {
			pc.Spec.RancherConfig.Token = strings.TrimSuffix(string(val), "\n")
		}
	}
	return nil
}
