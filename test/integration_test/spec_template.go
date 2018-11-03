package integrationtest

const (
	remotePairName = "remoteclusterpair"
	remoteConfig   = "remoteconfigmap"
	pairFileName   = "cluster-pair.yaml"
	remoteFilePath = "/tmp/kubeconfig"
	clusterIP      = "clusterip"
	clusterToken   = "token"
	clusterPort    = "clusterport"
)

// ClusterPairRequest to create new clusterpair spec file
type ClusterPairRequest struct {
	PairName           string
	ConfigMapName      string
	SpecDirPath        string
	RemoteIP           string
	RemoteClusterToken string
	RemotePort         string
}

// KubeConfigSpec struct for yaml string
type KubeConfigSpec struct {
	ClusterInfo []ClusterInfo `yaml:"clusters,omitempty"`
	ContextInfo []ContextInfo `yaml:"contexts,omitempty"`
	UserInfo    []UserInfo    `yaml:"users,omitempty"`
}

// ClusterInfo in KubeConfigFile
type ClusterInfo struct {
	Name    string            `yaml:"name,omitempty"`
	Cluster map[string]string `yaml:"cluster,omitempty"`
}

// ContextInfo of kubeConfig file
type ContextInfo struct {
	Name    string
	Context map[string]string `yaml:"context,omitempty"`
}

// UserInfo in kubeconfig file
type UserInfo struct {
	Name string
	User map[string]string `yaml:"user,omitempty"`
}

// ClusterPair to poulate cluster pair spec with required parameters
type ClusterPair struct {
	PairName             string
	RemoteIP             string
	RemoteToken          string
	RemotePort           int
	RemoteKubeServer     string
	RemoteConfigAuthData string
	RemoteConfigKeyData  string
	RemoteConfigCertData string
}

var clusterPairSpec = `apiVersion: stork.libopenstorage.org/v1alpha1
kind: ClusterPair
metadata:
  name: {{ .PairName }}
spec:
  options:
    ip: {{ .RemoteIP }}
    token: {{ .RemoteToken }} 
    port: "{{ .RemotePort }}"
  config:
    clusters:
      cluster.local:
        certificate-authority-data: {{ .RemoteConfigAuthData }}
        server: {{.RemoteKubeServer}}
    contexts:
      admin-cluster.local:
        cluster:  cluster.local
        user: admin-cluster.local
    current-context: admin-cluster.local
    users:
      admin-cluster.local:
        client-certificate-data: {{ .RemoteConfigCertData }}
        client-key-data: {{ .RemoteConfigKeyData }}`
