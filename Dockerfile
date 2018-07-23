FROM registry.access.redhat.com/rhel7-atomic
MAINTAINER Portworx Inc. <support@portworx.com>

LABEL name="openstorage/stork" \
      maintainer="support@portworx.com" \
      vendor="Portworx Inc." \
      version="1.1.3" \
      release="1" \
      summary="STORK" \
      description="Storage Orchestrator Runtime for Kubernetes" \
      url="https://github.com/libopenstorage/stork" \
      io.openshift.tags="portworx,storage,pv,pvc,storageclass,stork,persistent,openstorage" \
      io.k8s.display-name="STORK" \
      io.k8s.description="Storage Orchestrator Runtime for Kubernetes"

COPY LICENSE /licenses/
COPY help.1 /

WORKDIR /

COPY ./bin/stork /
