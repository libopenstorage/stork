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

RUN microdnf clean all && microdnf install -y python3.9 ca-certificates tar gzip

RUN python3 -m pip install awscli  && python3 -m pip install rsa --upgrade


RUN curl -q -o /usr/local/bin/aws-iam-authenticator https://amazon-eks.s3-us-west-2.amazonaws.com/1.10.3/2018-07-26/bin/linux/amd64/aws-iam-authenticator && \
    chmod +x /usr/local/bin/aws-iam-authenticator

ARG GCLOUD_SDK=google-cloud-sdk-269.0.0-linux-x86_64.tar.gz
# Remove the test directories
# Also don't need gsutil
RUN curl -q -o $GCLOUD_SDK https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/$GCLOUD_SDK && \
    tar xf $GCLOUD_SDK && rm -rf $GCLOUD_SDK && \
    rm -rf /google-cloud-sdk/platform/gsutil/third_party/oauth2client/tests \
        /google-cloud-sdk/platform/gsutil/third_party/rsa/tests \
        /google-cloud-sdk/platform/gsutil/third_party/httplib2/python2/httplib2/test \
        /google-cloud-sdk/platform/gsutil && \
    python3 -m pip install pyyaml>=5.1 rsa>=4.0 urllib3>=1.24.2 --upgrade -t /google-cloud-sdk/lib/third_party

WORKDIR /

COPY ./bin/linux/storkctl /storkctl/linux/
COPY ./bin/darwin/storkctl /storkctl/darwin/
COPY ./bin/windows/storkctl.exe /storkctl/windows/
COPY ./LICENSE /licenses
COPY ./bin/stork /
