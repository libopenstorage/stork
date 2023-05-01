FROM registry.access.redhat.com/ubi8-minimal:latest
MAINTAINER Portworx Inc. <support@portworx.com>

ARG VERSION=master
ARG RELEASE=latest
LABEL name="Stork" \
       vendor="Openstorage.org" \
       version=${VERSION} \
       release=${RELEASE} \
       summary="Storage Operator Runtime for Kubernetes" \
       description="Stork is a Cloud Native storage operator runtime scheduler plugin"

RUN microdnf clean all && microdnf install -y python3.9 ca-certificates tar gzip openssl

RUN python3 -m pip install awscli && python3 -m pip install oci-cli && python3 -m pip install rsa --upgrade


RUN curl -q -o /usr/local/bin/aws-iam-authenticator https://amazon-eks.s3-us-west-2.amazonaws.com/1.10.3/2018-07-26/bin/linux/amd64/aws-iam-authenticator && \
    chmod +x /usr/local/bin/aws-iam-authenticator

#Install Google Cloud SDK
ARG GCLOUD_SDK=google-cloud-sdk-418.0.0-linux-x86_64.tar.gz
ARG GCLOUD_INSTALL_DIR="/usr/lib"
RUN curl -q -o $GCLOUD_SDK https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/$GCLOUD_SDK && \
    tar xf $GCLOUD_SDK -C $GCLOUD_INSTALL_DIR && rm -rf $GCLOUD_SDK && \
    rm -rf $GCLOUD_INSTALL_DIR/google-cloud-sdk/platform/gsutil \
           $GCLOUD_INSTALL_DIR/google-cloud-sdk/RELEASE_NOTES
ENV PATH "${PATH}:$GCLOUD_INSTALL_DIR/google-cloud-sdk/bin"
#Install gke-gcloud-auth-plugin
RUN gcloud components install gke-gcloud-auth-plugin
#Create symlink /google-cloud-sdk/bin -> /usr/lib/google-cloud-sdk/bin for legacy cluster pair with gcp auth plugin
RUN mkdir google-cloud-sdk
RUN ln -s /usr/lib/google-cloud-sdk/bin /google-cloud-sdk/bin

WORKDIR /

COPY ./bin/linux/storkctl /storkctl/linux/
COPY ./bin/darwin/storkctl /storkctl/darwin/
COPY ./bin/windows/storkctl.exe /storkctl/windows/
COPY ./LICENSE /licenses
COPY ./bin/stork /
COPY ./bin/px_statfs.so /
