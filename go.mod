module github.com/portworx/torpedo

go 1.12

require (
	github.com/Azure/azure-sdk-for-go v38.2.0+incompatible // indirect
	github.com/Azure/azure-storage-blob-go v0.9.0
	github.com/Azure/go-autorest/autorest v0.9.4 // indirect
	github.com/LINBIT/golinstor v0.27.0
	github.com/Microsoft/go-winio v0.4.14 // indirect
	github.com/aws/aws-sdk-go v1.31.13
	github.com/coreos/go-oidc v2.2.1+incompatible // indirect
	github.com/docker/docker v1.4.2-0.20181221150755-2cb26cfe9cbf
	github.com/donovanhide/eventsource v0.0.0-20171031113327-3ed64d21fb0b // indirect
	github.com/fatih/color v1.7.0
	github.com/gambol99/go-marathon v0.7.1
	github.com/gogo/googleapis v1.4.0 // indirect
	github.com/golang/protobuf v1.4.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.14.6 // indirect
	github.com/hashicorp/go-version v1.1.0
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/vault/api v1.0.4
	github.com/kubernetes-incubator/external-storage v0.0.0-00010101000000-000000000000
	github.com/libopenstorage/autopilot-api v1.3.0
	github.com/libopenstorage/cloudops v0.0.0-20200604165016-9cc0977d745e
	github.com/libopenstorage/gossip v0.0.0-20190507031959-c26073a01952 // indirect
	github.com/libopenstorage/openstorage v8.0.1-0.20200129213931-ba5ed2ee0f18+incompatible
	github.com/libopenstorage/stork v1.3.0-beta1.0.20200630005842-9255e7a98775
	github.com/mattn/go-isatty v0.0.10 // indirect
	github.com/onsi/ginkgo v1.10.2
	github.com/onsi/gomega v1.7.0
	github.com/openshift/client-go v0.0.0-20200116152001-92a2713fa240 // indirect
	github.com/pborman/uuid v1.2.0
	github.com/portworx/kvdb v0.0.0-20191223203141-f42097b1fcd8 // indirect
	github.com/portworx/px-backup-api v1.0.1-0.20200821003202-b927067c1043
	github.com/portworx/sched-ops v0.0.0-20200723032629-257aba0944dc
	github.com/sirupsen/logrus v1.5.0
	github.com/stretchr/testify v1.5.1
	gocloud.dev v0.20.0
	golang.org/x/crypto v0.0.0-20200604202706-70a84ac30bf9
	golang.org/x/net v0.0.0-20200602114024-627f9648deb9
	google.golang.org/grpc v1.29.1
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
	gopkg.in/yaml.v2 v2.2.8
	helm.sh/helm/v3 v3.0.0-00010101000000-000000000000
	k8s.io/api v0.17.2
	k8s.io/apimachinery v0.17.2
	k8s.io/client-go v11.0.1-0.20190409021438-1a26190bd76a+incompatible
)

replace (
	github.com/Azure/go-autorest => github.com/Azure/go-autorest v13.0.0+incompatible
	github.com/docker/docker => github.com/docker/docker v17.12.0-ce-rc1.0.20190717161051-705d9623b7c1+incompatible
	github.com/kubernetes-incubator/external-storage v0.0.0-00010101000000-000000000000 => github.com/libopenstorage/external-storage v5.3.0-alpha.1.0.20200130041458-d2b33d4448ea+incompatible
	github.com/prometheus/prometheus => github.com/prometheus/prometheus v1.8.2-0.20190424153033-d3245f150225
	helm.sh/helm/v3 => helm.sh/helm/v3 v3.0.0
	k8s.io/api => k8s.io/api v0.16.4
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.16.4
	k8s.io/apimachinery => k8s.io/apimachinery v0.16.4
	k8s.io/apiserver => k8s.io/apiserver v0.16.4
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.16.4
	k8s.io/client-go => k8s.io/client-go v0.16.4
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.16.4
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.16.4
	k8s.io/code-generator => k8s.io/code-generator v0.16.4
	k8s.io/component-base => k8s.io/component-base v0.16.4
	k8s.io/cri-api => k8s.io/cri-api v0.16.4
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.16.4
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.16.4
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.16.4
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.16.4
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.16.4
	k8s.io/kubectl => k8s.io/kubectl v0.16.4
	k8s.io/kubelet => k8s.io/kubelet v0.16.4
	k8s.io/kubernetes => k8s.io/kubernetes v1.16.0
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.16.4
	k8s.io/metrics => k8s.io/metrics v0.16.4
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.16.4
)
