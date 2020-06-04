FROM alpine:latest
MAINTAINER Portworx Inc. <support@portworx.com>

RUN apk add tar python3 ca-certificates && apk upgrade

RUN wget -q -O /usr/local/bin/aws-iam-authenticator https://amazon-eks.s3-us-west-2.amazonaws.com/1.10.3/2018-07-26/bin/linux/amd64/aws-iam-authenticator && \
    chmod +x /usr/local/bin/aws-iam-authenticator
RUN pip3 install --upgrade pip && pip3 install awscli && pip3 install rsa --upgrade

ARG GCLOUD_SDK=google-cloud-sdk-269.0.0-linux-x86_64.tar.gz
# Remove the test directories
# Also don't need gsutil
RUN wget -q https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/$GCLOUD_SDK && \
    tar xf $GCLOUD_SDK && rm -rf $GCLOUD_SDK && \
    rm -rf /google-cloud-sdk/platform/gsutil/third_party/oauth2client/tests \
        /google-cloud-sdk/platform/gsutil/third_party/rsa/tests \
        /google-cloud-sdk/platform/gsutil/third_party/httplib2/python2/httplib2/test \
        /google-cloud-sdk/platform/gsutil && \
    pip3 install pyyaml>=5.1 rsa>=4.0 urllib3>=1.24.2 --upgrade -t /google-cloud-sdk/lib/third_party

WORKDIR /

COPY ./bin/linux/storkctl /storkctl/linux/
COPY ./bin/darwin/storkctl /storkctl/darwin/
COPY ./bin/windows/storkctl.exe /storkctl/windows/
COPY ./LICENSE /licenses
COPY ./bin/stork /
