module github.com/libopenstorage/stork

go 1.15

require (
	cloud.google.com/go v0.65.0
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-sdk-for-go v43.0.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.9.0
	github.com/Azure/go-autorest/autorest v0.11.13
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.5
	github.com/Azure/go-autorest/autorest/to v0.4.0
	github.com/LINBIT/golinstor v0.27.0
	github.com/Shopify/logrus-bugsnag v0.0.0-20171204204709-577dee27f20d // indirect
	github.com/aquilax/truncate v1.0.0
	github.com/aws/aws-sdk-go v1.35.37
	github.com/bitly/go-simplejson v0.5.0 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/bshuster-repo/logrus-logstash-hook v1.0.2 // indirect
	github.com/bugsnag/bugsnag-go v2.1.2+incompatible // indirect
	github.com/bugsnag/panicwrap v1.3.4 // indirect
	github.com/docker/libtrust v0.0.0-20160708172513-aabc10ec26b7 // indirect
	github.com/garyburd/redigo v1.6.3 // indirect
	github.com/go-openapi/inflect v0.19.0
	github.com/gofrs/uuid v4.2.0+incompatible // indirect
	github.com/gorilla/handlers v1.5.1 // indirect
	github.com/hashicorp/go-multierror v1.1.0
	github.com/hashicorp/go-version v1.2.1
	github.com/heptio/ark v1.0.0
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/kubernetes-csi/external-snapshotter/client/v4 v4.0.0
	github.com/kubernetes-incubator/external-storage v0.20.4-openstorage-rc7
	github.com/kubernetes-sigs/aws-ebs-csi-driver v0.9.0
	github.com/libopenstorage/openstorage v8.0.1-0.20211105030910-665c2f474186+incompatible
	github.com/libopenstorage/secrets v0.0.0-20220413195519-57d1c446c5e9
	github.com/mitchellh/hashstructure v1.0.0
	github.com/openshift/api v0.0.0-20210105115604-44119421ec6b
	github.com/openshift/client-go v0.0.0-20210112165513-ebc401615f47
	github.com/pborman/uuid v1.2.0
	github.com/portworx/kdmp v0.4.1-0.20220612094815-855f9e2fd62e
	github.com/portworx/sched-ops v1.20.4-rc1.0.20220401024625-dbc61a336f65
	github.com/portworx/torpedo v0.20.4-rc1.0.20210325154352-eb81b0cdd145
	github.com/prometheus/client_golang v1.11.0
	github.com/sirupsen/logrus v1.8.1
	github.com/skyrings/skyring-common v0.0.0-20160929130248-d1c0bb1cbd5e
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli v1.22.2
	github.com/yvasiyarov/go-metrics v0.0.0-20150112132944-c25f46c4b940 // indirect
	github.com/yvasiyarov/gorelic v0.0.7 // indirect
	github.com/yvasiyarov/newrelic_platform_go v0.0.0-20160601141957-9c099fbc30e9 // indirect
	gocloud.dev v0.20.0
	golang.org/x/oauth2 v0.0.0-20201208152858-08078c50e5b5
	google.golang.org/api v0.30.0
	google.golang.org/grpc v1.40.0
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.21.4
	k8s.io/apiextensions-apiserver v0.21.4
	k8s.io/apimachinery v0.21.4
	k8s.io/apiserver v0.21.4
	k8s.io/cli-runtime v0.21.4
	k8s.io/client-go v12.0.0+incompatible
	k8s.io/code-generator v0.21.4
	k8s.io/component-helpers v0.21.4
	k8s.io/kube-scheduler v0.0.0
	k8s.io/kubectl v0.21.4
	k8s.io/kubernetes v1.21.4
	rsc.io/letsencrypt v0.0.3 // indirect
	sigs.k8s.io/controller-runtime v0.9.0
	sigs.k8s.io/gcp-compute-persistent-disk-csi-driver v0.7.0
	sigs.k8s.io/sig-storage-lib-external-provisioner/v6 v6.3.0
)

replace (
	github.com/LINBIT/golinstor => github.com/LINBIT/golinstor v0.39.0
	github.com/banzaicloud/k8s-objectmatcher => github.com/banzaicloud/k8s-objectmatcher v1.5.1
	github.com/census-instrumentation/opencensus-proto => github.com/census-instrumentation/opencensus-proto v0.3.0
	github.com/docker/distribution => github.com/docker/distribution v2.7.0+incompatible
	github.com/docker/docker => github.com/moby/moby v20.10.4+incompatible
	github.com/go-logr/logr => github.com/go-logr/logr v0.3.0
	github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.4.1
	//github.com/heptio/ark => github.com/heptio/ark v1.0.0
	github.com/heptio/velero => github.com/heptio/velero v1.0.0
	github.com/kubernetes-csi/external-snapshotter/client/v4 => github.com/kubernetes-csi/external-snapshotter/client/v4 v4.0.0
	github.com/kubernetes-incubator/external-storage => github.com/libopenstorage/external-storage v0.20.4-openstorage-rc7
	github.com/kubernetes-incubator/external-storage v0.20.4-openstorage-rc7 => github.com/libopenstorage/external-storage v0.20.4-openstorage-rc7
	github.com/libopenstorage/autopilot-api => github.com/libopenstorage/autopilot-api v0.6.1-0.20210301232050-ca2633c6e114
	github.com/libopenstorage/openstorage => github.com/libopenstorage/openstorage v1.0.1-0.20220426191846-06ff1e09348f
	github.com/portworx/sched-ops => github.com/portworx/sched-ops v1.20.4-rc1.0.20220401024625-dbc61a336f65
	github.com/portworx/torpedo => github.com/portworx/torpedo v0.0.0-20220413175518-a912f1a6faa2
	gopkg.in/fsnotify.v1 v1.4.7 => github.com/fsnotify/fsnotify v1.4.7
	helm.sh/helm/v3 => helm.sh/helm/v3 v3.6.0

	k8s.io/api => k8s.io/api v0.21.4
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.21.4
	k8s.io/apimachinery => k8s.io/apimachinery v0.21.4
	k8s.io/apiserver => k8s.io/apiserver v0.21.4
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.21.4
	k8s.io/client-go => k8s.io/client-go v0.21.4
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.21.4
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.21.4
	k8s.io/code-generator => k8s.io/code-generator v0.21.4
	k8s.io/component-base => k8s.io/component-base v0.21.4
	k8s.io/component-helpers => k8s.io/component-helpers v0.21.4
	k8s.io/controller-manager => k8s.io/controller-manager v0.21.4
	k8s.io/cri-api => k8s.io/cri-api v0.21.4
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.21.4
	k8s.io/klog/v2 => k8s.io/klog/v2 v2.4.0
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.21.4
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.21.4
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.21.4
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.21.4
	k8s.io/kubectl => k8s.io/kubectl v0.21.4
	k8s.io/kubelet => k8s.io/kubelet v0.21.4
	k8s.io/kubernetes => k8s.io/kubernetes v1.21.4
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.21.4
	k8s.io/metrics => k8s.io/metrics v0.21.4
	k8s.io/mount-utils => k8s.io/mount-utils v0.21.4
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.21.4
	sigs.k8s.io/controller-runtime => sigs.k8s.io/controller-runtime v0.9.0
	sigs.k8s.io/sig-storage-lib-external-provisioner/v6 => sigs.k8s.io/sig-storage-lib-external-provisioner/v6 v6.3.0
)
