FROM registry.access.redhat.com/ubi7-dev-preview/ubi:latest
MAINTAINER Portworx Inc. <support@portworx.com>

LABEL name="openstorage/stork" \
      maintainer="support@portworx.com" \
      vendor="Portworx Inc." \
      version="2.2.0" \
      release="1" \
      summary="STORK" \
      description="Storage Operator Runtime for Kubernetes" \
      url="https://github.com/libopenstorage/stork" \
      io.openshift.tags="portworx,storage,pv,pvc,storageclass,stork,persistent,openstorage,cloud" \
      io.k8s.display-name="STORK" \
      io.k8s.description="Storage Operator Runtime for Kubernetes"

COPY LICENSE /licenses/
COPY help.1 /

RUN yum repolist
RUN yum updateinfo -y
RUN yum update -y
RUN yum install wget tar python ca-certificates -y

RUN wget -O /usr/local/bin/aws-iam-authenticator https://amazon-eks.s3-us-west-2.amazonaws.com/1.10.3/2018-07-26/bin/linux/amd64/aws-iam-authenticator
RUN chmod +x /usr/local/bin/aws-iam-authenticator

RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-224.0.0-linux-x86_64.tar.gz
RUN tar xf google-cloud-sdk-224.0.0-linux-x86_64.tar.gz
RUN rm -rf google-cloud-sdk-224.0.0-linux-x86_64.tar.gz

WORKDIR /

COPY ./bin/stork /
COPY ./bin/linux/storkctl /storkctl/linux/
COPY ./bin/darwin/storkctl /storkctl/darwin/
COPY ./bin/windows/storkctl.exe /storkctl/windows/
