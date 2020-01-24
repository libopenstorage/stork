module github.com/portworx/torpedo

go 1.12

require (
	cloud.google.com/go v0.37.4 // indirect
	github.com/Azure/azure-sdk-for-go v38.2.0+incompatible // indirect
	github.com/Azure/go-autorest v13.3.1+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.9.4 // indirect
	github.com/Azure/go-autorest/autorest/azure/auth v0.4.2 // indirect
	github.com/Azure/go-autorest/autorest/to v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.2.0 // indirect
	github.com/Microsoft/go-winio v0.4.14 // indirect
	github.com/aws/aws-sdk-go v1.25.11
	github.com/coreos/go-oidc v2.1.0+incompatible // indirect
	github.com/coreos/prometheus-operator v0.31.1 // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/docker/docker v1.4.2-0.20170731201938-4f3616fb1c11
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/docker/libtrust v0.0.0-20160708172513-aabc10ec26b7 // indirect
	github.com/docker/spdystream v0.0.0-20181023171402-6480d4af844c // indirect
	github.com/donovanhide/eventsource v0.0.0-20171031113327-3ed64d21fb0b // indirect
	github.com/emicklei/go-restful v2.10.0+incompatible // indirect
	github.com/fatih/color v1.7.0
	github.com/gambol99/go-marathon v0.7.1
	github.com/go-openapi/jsonreference v0.19.3 // indirect
	github.com/go-openapi/spec v0.19.3 // indirect
	github.com/gogo/protobuf v1.3.0 // indirect
	github.com/golang/groupcache v0.0.0-20191002201903-404acd9df4cc // indirect
	github.com/golang/protobuf v1.3.2
	github.com/google/uuid v1.1.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.11.3 // indirect
	github.com/imdario/mergo v0.3.8 // indirect
	github.com/json-iterator/go v1.1.7 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kubernetes-incubator/external-storage v0.0.0-00010101000000-000000000000
	github.com/libopenstorage/autopilot-api v0.6.0
	github.com/libopenstorage/cloudops v0.0.0-20200114171448-10fa10d97720
	github.com/libopenstorage/gossip v0.0.0-20190507031959-c26073a01952 // indirect
	github.com/libopenstorage/openstorage v7.0.1-0.20191218211301-d03367435351+incompatible
	github.com/libopenstorage/operator v0.0.0-20191009190641-8642de5d0812 // indirect
	github.com/libopenstorage/stork v1.3.0-beta1.0.20191009210244-6a3497c42b2a
	github.com/mailru/easyjson v0.7.0 // indirect
	github.com/mattn/go-isatty v0.0.10 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/onsi/ginkgo v1.10.2
	github.com/onsi/gomega v1.7.0
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/opencontainers/image-spec v1.0.1 // indirect
	github.com/openshift/api v0.0.0-20190322043348-8741ff068a47 // indirect
	github.com/openshift/client-go v0.0.0-20180830153425-431ec9a26e50 // indirect
	github.com/operator-framework/operator-sdk v0.0.7 // indirect
	github.com/pborman/uuid v0.0.0-20180906182336-adf5a7427709
	github.com/portworx/kvdb v0.0.0-20190911174000-a0108bddd091 // indirect
	github.com/portworx/sched-ops v0.0.0-20191218205357-e76403b281e8
	github.com/portworx/talisman v0.0.0-20191007232806-837747f38224 // indirect
	github.com/pquerna/cachecontrol v0.0.0-20180517163645-1555304b9b35 // indirect
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/pflag v1.0.5 // indirect
	go.opencensus.io v0.22.1 // indirect
	golang.org/x/crypto v0.0.0-20191206172530-e9b2fee46413
	golang.org/x/net v0.0.0-20191009170851-d66e71096ffb
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45 // indirect
	golang.org/x/sys v0.0.0-20191010194322-b09406accb47 // indirect
	golang.org/x/time v0.0.0-20190921001708-c4c64cad1fd0 // indirect
	google.golang.org/api v0.4.0 // indirect
	google.golang.org/appengine v1.6.5 // indirect
	google.golang.org/genproto v0.0.0-20191009194640-548a555dbc03 // indirect
	google.golang.org/grpc v1.24.0
	gopkg.in/square/go-jose.v2 v2.3.1 // indirect
	gopkg.in/yaml.v2 v2.2.4
	k8s.io/api v0.0.0-20190816222004-e3a6b8045b0b
	k8s.io/apiextensions-apiserver v0.0.0-20190918224502-6154570c2037 // indirect
	k8s.io/apimachinery v0.0.0-20190816221834-a9f1d8a9c101
	k8s.io/client-go v11.0.1-0.20190409021438-1a26190bd76a+incompatible
	k8s.io/klog v1.0.0 // indirect
	k8s.io/kube-openapi v0.0.0-20190918143330-0270cf2f1c1d // indirect
	k8s.io/utils v0.0.0-20191010214722-8d271d903fe4 // indirect
	sigs.k8s.io/controller-runtime v0.2.2 // indirect
)

replace (
	github.com/kubernetes-incubator/external-storage v0.0.0-00010101000000-000000000000 => github.com/libopenstorage/external-storage v5.1.1-0.20190919185747-9394ee8dd536+incompatible
	k8s.io/client-go v2.0.0-alpha.0.0.20181121191925-a47917edff34+incompatible => k8s.io/client-go v2.0.0+incompatible
)
