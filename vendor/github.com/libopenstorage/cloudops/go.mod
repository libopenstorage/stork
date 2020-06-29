module github.com/libopenstorage/cloudops

go 1.13

require (
	cloud.google.com/go v0.37.4
	github.com/Azure/azure-sdk-for-go v26.7.0+incompatible
	github.com/Azure/go-autorest v11.9.0+incompatible
	github.com/aws/aws-sdk-go v1.19.20
	github.com/beorn7/perks v1.0.0 // indirect
	github.com/codegangsta/inject v0.0.0-20140425184007-37d7f8432a3e // indirect
	github.com/codeskyblue/go-sh v0.0.0-20170112005953-b097669b1569
	github.com/dimchansky/utfbom v1.1.0 // indirect
	github.com/golang/mock v1.3.1-0.20190508161146-9fa652df1129
	github.com/google/go-cmp v0.3.0 // indirect
	github.com/google/uuid v1.1.1 // indirect
	github.com/hashicorp/go-version v1.1.0
	github.com/libopenstorage/openstorage v8.0.1-0.20190926212733-daaed777713e+incompatible
	github.com/libopenstorage/secrets v0.0.0-20190403224602-c282e8dc17bf
	github.com/pborman/uuid v0.0.0-20180906182336-adf5a7427709
	github.com/portworx/sched-ops v0.0.0-20200123020607-b0799c4686f5
	github.com/prometheus/client_golang v0.9.3-0.20190127221311-3c4408c8b829
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90 // indirect
	github.com/prometheus/procfs v0.0.0-20190425082905-87a4384529e0 // indirect
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	github.com/vmware/govmomi v0.22.2
	google.golang.org/api v0.4.0
	gopkg.in/yaml.v2 v2.2.4
	k8s.io/apimachinery v0.0.0-20190816221834-a9f1d8a9c101
	k8s.io/client-go v11.0.1-0.20190409021438-1a26190bd76a+incompatible
	k8s.io/klog v1.0.0
)

replace (
	github.com/kubernetes-incubator/external-storage v0.0.0-00010101000000-000000000000 => github.com/libopenstorage/external-storage v5.1.1-0.20190919185747-9394ee8dd536+incompatible
	github.com/prometheus/prometheus v2.9.2+incompatible => github.com/prometheus/prometheus v1.8.2-0.20190424153033-d3245f150225
	k8s.io/client-go v2.0.0-alpha.0.0.20181121191925-a47917edff34+incompatible => k8s.io/client-go v2.0.0+incompatible
)
