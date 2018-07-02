FROM registry.access.redhat.com/rhel7-atomic
MAINTAINER Portworx Inc. <support@portworx.com>

LABEL name="openstorage/cmdexecutor" \
      maintainer="support@portworx.com" \
      vendor="Portworx Inc." \
      version="1.1" \
      release="1" \
      summary="STORK Command executor" \
      description="CLI to execute async commands for Kubernetes Pods" \
      url="https://github.com/libopenstorage/stork" \
      io.openshift.tags="portworx,storage,pv,pvc,storageclass,stork,persistent,openstorage" \
      io.k8s.display-name="STORK Command executor" \
      io.k8s.description="CLI to execute async commands for Kubernetes Pods"

COPY LICENSE /licenses/
COPY help-cmdexecutor.1 /help.1

WORKDIR /

COPY ./bin/cmdexecutor /
